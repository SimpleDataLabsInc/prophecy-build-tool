"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""

import json
import os
import re
import subprocess
from typing import Dict, Optional, Any

import click
import yaml
from rich import print
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import DbfsService, JobsService


def get_or_none(obj: Dict, key: str) -> Optional[Any]:
    return obj[key] if key in obj else None


class ProphecyBuildTool:

    def __init__(self, path_root: str):
        print('[bold purple]Prophecy-build-tool[/bold purple] [bold black]v1.0.0[/bold black]\n')

        self.path_root = path_root
        self.path_project = os.path.join(self.path_root, 'pbt_project.yml')

        self._verify_project()
        self._verify_databricks_configs()

        config = EnvironmentVariableConfigProvider().get_config()
        self.api_client = _get_api_client(config)

        self.dbfs_service = DbfsService(self.api_client)
        self.jobs_service = JobsService(self.api_client)

        self._parse_project()

        self.pipelines_build_path = {}

    def build(self):
        print('\n[bold blue]Building %s pipelines 🚰[/bold blue]' % self.pipelines_count)
        for pipeline_i, (path_pipeline, pipeline) in enumerate(self.pipelines.items()):
            print('\n  Building pipeline %s [%s/%s]' % (path_pipeline, pipeline_i + 1, self.pipelines_count))

            path_pipeline_absolute = os.path.join(os.path.join(self.path_root, path_pipeline), 'code')
            process = subprocess.Popen(['python3', 'setup.py', 'build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       cwd=path_pipeline_absolute)
            stdout, stderr = process.communicate()
            if len(stderr) > 0:
                print('   ', '\n    '.join([line.decode('utf-8') for line in stdout.splitlines()]))
                print('   ', '\n    '.join([line.decode('utf-8') for line in stdout.splitlines()]))
                print('   ', '\n    '.join([line.decode('utf-8') for line in stderr.splitlines()]))
            else:
                print('   ', '\n    '.join([line.decode('utf-8') for line in stdout.splitlines()]))

            self.pipelines_build_path[path_pipeline] = None
            path_pipeline_dist = path_pipeline_absolute + '/dist'
            for path_pipeline_build in os.listdir(path_pipeline_dist):
                path_pipeline_build_absolute = path_pipeline_dist + '/' + path_pipeline_build
                if os.path.isfile(path_pipeline_build_absolute) and path_pipeline_build.endswith('.whl'):
                    self.pipelines_build_path[path_pipeline] = {
                        'source_absolute': path_pipeline_build_absolute,
                        'source': path_pipeline_build,
                        'uploaded': False
                    }

        print("\n[bold blue]✅ Build complete![/bold blue]")

    def deploy(self):
        self.build()

        print('\n[bold blue]Deploying %s jobs ⏱[/bold blue]' % self.jobs_count)
        for job_idx, (path_job, job) in enumerate(self.jobs.items()):
            print('\n  Deploying job %s [%s/%s]' % (path_job, job_idx + 1, self.jobs_count))

            path_job_absolute = os.path.join(os.path.join(self.path_root, path_job), 'code')
            path_job_definition = os.path.join(path_job_absolute, 'databricks-job.json')

            job_definition = {}
            with open(path_job_definition, 'r') as _in:
                job_definition = json.load(_in)

            components = job_definition['components']
            for component in components:
                if 'PipelineComponent' in component:
                    pipeline_uri = component['PipelineComponent']['id']

                    uri_pattern = '([0-9]+)/([-_.A-Za-z0-9 /]+)'
                    pipeline_id = re.search(uri_pattern, pipeline_uri).group(2)

                    source_path = self.pipelines_build_path[pipeline_id]['source_absolute']
                    target_path = component['PipelineComponent']['path']

                    print('    Uploading %s to %s' % (self.pipelines_build_path[pipeline_id]['source'], target_path))
                    self.dbfs_service.put(target_path, overwrite=True, src_path=source_path)

                    self.pipelines_build_path[pipeline_id]['uploaded'] = True

            job_request = job_definition['request']['CreateNewJobRequest']
            job_request['version'] = '2.1'

            limit = 25
            current_offset = 0
            found_job = None
            while found_job is None:
                response = self.jobs_service.list_jobs(limit=limit, offset=current_offset, version='2.1')
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
                print('    Creating a new job: %s' % (job_request['name']))
                self.jobs_service.create_job(**job_request)
            else:
                print('    Updating an existing job: %s' % (job_request['name']))
                self.jobs_service.reset_job(found_job['job_id'], new_settings=job_request, version='2.1')

        print("\n[bold blue]✅ Deployment complete![/bold blue]")

    def _parse_project(self):
        self.pipelines: Dict = {}
        self.jobs: Dict = {}
        with open(self.path_project, 'r') as _in:
            self.project = yaml.safe_load(_in)
            self.jobs = self.project['jobs']
            self.pipelines = self.project['pipelines']

        self.pipelines_count = len(self.pipelines)
        self.jobs_count = len(self.jobs)

        jobs_str = ', '.join(map(lambda job: job['name'], self.jobs.values()))
        print('Found %s jobs: %s' % (self.jobs_count, jobs_str))

        pipelines_str = ', '.join(
            map(lambda pipeline: '%s (%s)' % (pipeline['name'], pipeline['language']), self.pipelines.values()))
        print('Found %s pipelines: %s' % (self.pipelines_count, pipelines_str))

    @classmethod
    def _verify_databricks_configs(cls):
        host = os.environ.get('DATABRICKS_HOST')
        token = os.environ.get('DATABRICKS_TOKEN')

        if host is None or token is None:
            cls._error('[i]DATABRICKS_HOST[/i] & [i]DATABRICKS_TOKEN[/i] environment variables are required to '
                       'deploy your Databricks Workflows')

    def _verify_project(self):
        if not os.path.isfile(self.path_project):
            self._error('Missing [i]pbt_project.yml[/i] file. Are you sure you pointed pbt into a Prophecy project? '
                        'Current path [u]%s[/u]' % self.path_root)

    @classmethod
    def _error(cls, message: str):
        print('[bold red]ERROR[/bold red]:', message)
        exit()


@click.group()
def cli():
    pass


@click.command()
@click.option('--path', help='Path to the directory containing the pbt_project.yml file')
def build(path):
    pbt = ProphecyBuildTool(path)
    pbt.build()


@cli.command()
@click.option('--path', help='Path to the directory containing the pbt_project.yml file')
def deploy(path):
    pbt = ProphecyBuildTool(path)
    pbt.deploy()


if __name__ == '__main__':
    cli()
