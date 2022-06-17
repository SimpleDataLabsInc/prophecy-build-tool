"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""

import json
import os
import re
import subprocess
from typing import Dict, Optional, Any

import yaml
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import DbfsService, JobsService


def get_or_none(obj: Dict, key: str) -> Optional[Any]:
    return obj[key] if key in obj else None


def main():
    config = EnvironmentVariableConfigProvider().get_config()
    api_client = _get_api_client(config)

    dbfs_service = DbfsService(api_client)
    jobs_service = JobsService(api_client)

    path_root = '/Users/maciej/Downloads/qoi7acmu_project_1452/'
    path_project = path_root + 'pbt_project.yml'

    pipelines: Dict = {}
    jobs: Dict = {}
    with open(path_project, 'r') as _in:
        project = yaml.safe_load(_in)
        jobs = project['jobs']
        pipelines = project['pipelines']

    print('Found jobs: %s' % ', '.join(map(lambda job: job['name'], jobs.values())))
    print('Found pipelines: %s' % ', '.join(
        map(lambda pipeline: '%s (%s)' % (pipeline['name'], pipeline['language']), pipelines.values())))

    print('Building pipelines')
    pipelines_build_path = {}
    for path_pipeline, pipeline in pipelines.items():
        print('Building pipeline %s' % path_pipeline)

        path_pipeline_absolute = path_root + path_pipeline + '/code'
        process = subprocess.Popen(['python3', 'setup.py', 'build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=path_pipeline_absolute)
        stdout, stderr = process.communicate()
        if len(stderr) > 0:
            print(stdout)
            print(stderr)
        else:
            print(stdout)

        pipelines_build_path[path_pipeline] = None
        path_pipeline_dist = path_pipeline_absolute + '/dist'
        for path_pipeline_build in os.listdir(path_pipeline_dist):
            path_pipeline_build_absolute = path_pipeline_dist + '/' + path_pipeline_build
            if os.path.isfile(path_pipeline_build_absolute) and path_pipeline_build.endswith('.whl'):
                pipelines_build_path[path_pipeline] = {
                    'source_absolute': path_pipeline_build_absolute,
                    'source': path_pipeline_build,
                    'uploaded': False
                }

    for path_job, job in jobs.items():
        path_job_absolute = path_root + path_job + '/code'
        path_job_definition = path_job_absolute + '/databricks-job.json'

        job_definition = {}
        with open(path_job_definition, 'r') as _in:
            job_definition = json.load(_in)

        components = job_definition['components']
        for component in components:
            if 'PipelineComponent' in component:
                pipeline_uri = component['PipelineComponent']['id']

                uri_pattern = '([0-9]+)/([-_.A-Za-z0-9 /]+)'
                pipeline_id = re.search(uri_pattern, pipeline_uri).group(2)

                source_path = pipelines_build_path[pipeline_id]['source_absolute']
                target_path = component['PipelineComponent']['path']

                print('Uploading %s to %s' % (pipelines_build_path[pipeline_id]['source'], target_path))
                print(dbfs_service.put(target_path, overwrite=True, src_path=source_path))

                pipelines_build_path[pipeline_id]['uploaded'] = True

        job_request = job_definition['request']['CreateNewJobRequest']
        job_request['version'] = '2.1'

        limit = 25
        current_offset = 0
        found_job = None
        while found_job is None:
            response = jobs_service.list_jobs(limit=limit, offset=current_offset, version='2.1')
            current_offset += limit

            found_jobs = response['jobs'] if 'jobs' in response else []
            for potential_found_job in found_jobs:
                if potential_found_job['settings']['name'] == job_request['name']:
                    found_job = potential_found_job
                    break

            if found_job is not None or len(found_jobs) <= 0:
                break

        job_request = job_definition['request']['CreateNewJobRequest']
        if found_job is None:
            jobs_service.create_job(**job_request)
        else:
            jobs_service.reset_job(found_job['job_id'], new_settings=job_request, version='2.1')


if __name__ == '__main__':
    main()

