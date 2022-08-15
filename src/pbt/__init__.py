"""
DATABRICKS_HOST, DATABRICKS_TOKEN
"""
import collections
import json
import os
import re
import subprocess
import sys
import time
from typing import Dict, Optional, Any

import click
import yaml
from rich import print
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import DbfsService, JobsService


def get_or_none(obj: Dict, key: str) -> Optional[Any]:
    return obj[key] if key in obj else None


class Process:
    def __init__(self, process_args, current_working_directory, std_output=subprocess.PIPE, std_err=subprocess.PIPE,
                 is_shell=False):
        self.process_args = process_args
        self.current_working_directory = current_working_directory
        self.std_output = std_output
        self.std_err = std_err
        self.is_shell = is_shell


class ProphecyBuildTool:

    def __init__(self, path_root: str):
        print('[bold purple]Prophecy-build-tool[/bold purple] [bold black]v1.0.0[/bold black]\n')

        self.path_root = path_root
        self.path_project = os.path.join(self.path_root, 'pbt_project.yml')

        self._verify_project()
        self._verify_databricks_configs()
        self._parse_project()

        config = EnvironmentVariableConfigProvider().get_config()

        self.api_client = _get_api_client(config)

        self.dbfs_service = DbfsService(self.api_client)
        self.jobs_service = JobsService(self.api_client)

        self.pipelines_build_path = {}

    def build(self):
        print('\n[bold blue]Building %s pipelines 🚰[/bold blue]' % self.pipelines_count)
        overall_build_status = True
        for pipeline_i, (path_pipeline, pipeline) in enumerate(self.pipelines.items()):
            print('\n  Building pipeline %s [%s/%s]' % (path_pipeline, pipeline_i + 1, self.pipelines_count))

            path_pipeline_absolute = os.path.join(os.path.join(self.path_root, path_pipeline), 'code')
            rc = self.build_python(
                path_pipeline_absolute) if self.project_language == "python" else self.build_scala(
                path_pipeline_absolute)

            self.pipelines_build_path[path_pipeline] = None
            build_file_found = False

            if self.project_language == 'python':
                path_pipeline_dist = path_pipeline_absolute + '/dist'
                for path_pipeline_build in os.listdir(path_pipeline_dist):
                    path_pipeline_build_absolute = path_pipeline_dist + '/' + path_pipeline_build
                    if os.path.isfile(path_pipeline_build_absolute) and path_pipeline_build.endswith('.whl'):
                        build_file_found = True
                        self.pipelines_build_path[path_pipeline] = {
                            'source_absolute': path_pipeline_build_absolute,
                            'source': path_pipeline_build,
                            'uploaded': False
                        }
            elif self.project_language == 'scala':
                path_pipeline_dist = path_pipeline_absolute + '/target'
                for path_pipeline_build in os.listdir(path_pipeline_dist):
                    path_pipeline_build_absolute = path_pipeline_dist + '/' + path_pipeline_build
                    if os.path.isfile(path_pipeline_build_absolute) and path_pipeline_build.endswith('.jar'):
                        build_file_found = True
                        self.pipelines_build_path[path_pipeline] = {
                            'source_absolute': path_pipeline_build_absolute,
                            'source': path_pipeline_build,
                            'uploaded': False
                        }

            if rc == 0:
                if build_file_found:
                    print("\n[bold blue]✅ Build complete![/bold blue]")
                else:
                    print(
                        f"\n[bold red]❌ Build completed but built target not found for pipeline: {pipeline['name']}![/bold red]")
                    overall_build_status = False
            else:
                print(f"\n[bold red]❌ Build failed for pipeline: {pipeline['name']}![/bold red]")
                overall_build_status = False

        if not overall_build_status:
            sys.exit(1)

    def deploy(self):
        self.build()

        print('\n[bold blue]Deploying %s jobs ⏱[/bold blue]' % self.jobs_count)

        pipelines_upload_failures = collections.defaultdict(list)
        job_update_failures = dict()

        for job_idx, (path_job, job) in enumerate(self.jobs.items()):
            print('\n  Deploying job %s [%s/%s]' % (path_job, job_idx + 1, self.jobs_count))

            path_job_absolute = os.path.join(os.path.join(self.path_root, path_job), 'code')
            path_job_definition = os.path.join(path_job_absolute, 'databricks-job.json')

            with open(path_job_definition, 'r') as _in:
                job_definition = json.load(_in)

            components = job_definition['components']

            for component in components:
                if 'PipelineComponent' in component:
                    pipeline_uri = component['PipelineComponent']['id']

                    # Matches project_id/pipelines/pipeline_name or pipelines/pipeline_name or /pipelines/pipeline_name
                    # group(2) should return pipelines/pipeline_name
                    uri_pattern = '([0-9]*)(pipelines/[-_.A-Za-z0-9 /]+)'
                    pipeline_id = re.search(uri_pattern, pipeline_uri).group(2)

                    source_path = self.pipelines_build_path[pipeline_id]['source_absolute']
                    target_path = component['PipelineComponent']['path']

                    print('    Uploading %s to %s' % (self.pipelines_build_path[pipeline_id]['source'], target_path))

                    try:
                        self.dbfs_service.put(target_path, overwrite=True, src_path=source_path)
                        self.pipelines_build_path[pipeline_id]['uploaded'] = True
                    except Exception as e:
                        self.pipelines_build_path[pipeline_id]['uploaded'] = False
                        pipelines_upload_failures[pipeline_id].append(str(e))

            if len(pipelines_upload_failures) > 0:
                print("\n[bold red]❌ Upload failed for one or more pipelines [/bold red]")
                for pipeline_id, errors in pipelines_upload_failures.items():
                    print('\n[bold red]❌ %s: Exceptions: %s [/bold red]' % (pipeline_id, "\n".join(errors)))

                # Process next job
                continue

            job_request = job_definition['request']['CreateNewJobRequest']
            job_request['version'] = '2.1'

            limit = 25
            current_offset = 0
            found_job = None
            try:
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
            except Exception as e:
                print(
                    f"\n[bold red]❌ Create/Update for job: {job_request['name']} failed with exception: {e} [/bold red]")
                job_update_failures[job_request['name']] = str(e)

        if len(pipelines_upload_failures) == 0 and len(job_update_failures) == 0:
            print("\n[bold blue]✅ Deployment completed successfully![/bold blue]")
        else:
            print("\n[bold red]❌ Deployment failed![/bold red]")
            if len(pipelines_upload_failures) > 0:
                print("   Upload failed for pipelines: %s" % (" ,".join(pipelines_upload_failures.keys())))
            if len(job_update_failures) > 0:
                print("   Create/Update failed for jobs: %s" % (" ,".join(job_update_failures.keys())))
            sys.exit(1)

    def run_unit_tests(self):
        if self._verify_unit_test_env():
            unit_test_results = {}

            for pipeline_i, (path_pipeline, pipeline) in enumerate(self.pipelines.items()):
                print('\n  Unit Testing pipeline %s [%s/%s]' % (path_pipeline, pipeline_i + 1, self.pipelines_count))

                path_pipeline_absolute = os.path.join(os.path.join(self.path_root, path_pipeline), 'code')
                if self.project_language == 'python':
                    if os.path.isfile(os.path.join(path_pipeline_absolute, 'test/TestSuite.py')):
                        unit_test_results[path_pipeline] = self.test_python(path_pipeline_absolute)
                elif self.project_language == 'scala':
                    unit_test_results[path_pipeline] = self.test_scala(path_pipeline_absolute)
            is_any_ut_failed = False
            for path_pipeline, return_code in unit_test_results.items():
                if return_code not in (0, 5):
                    is_any_ut_failed = True
                    print(
                        f"\n[bold red]❌ Unit test for pipeline: {path_pipeline} failed with return code {return_code}![/bold red]")
                else:
                    print(f"\n[bold blue]✅ Unit test for pipeline: {path_pipeline} succeeded.[/bold blue]")

            if is_any_ut_failed:
                sys.exit(1)
        else:
            sys.exit(1)

    def build_python(self, path_pipeline_absolute):
        return self._process_sequential(
            [
                # Install dependencies of particular pipeline
                # 1. Export dependencies to egg_info/requires.txt
                Process(['python', 'setup.py', '-q', 'egg_info'], path_pipeline_absolute),
                # 2. Extract the install_requires, tests_requires and extra_requires and install them
                Process([f"pip install -q `grep -v '^\[' *.egg-info/requires.txt`"],
                        path_pipeline_absolute, is_shell=True),
                # Check for compilation errors
                Process(['python', '-m', 'compileall', '.', '-q'], path_pipeline_absolute),
                # Generate wheel
                Process(['python', 'setup.py', 'bdist_wheel'], path_pipeline_absolute)
            ]
        )

    def build_scala(self, path_pipeline_absolute):
        return self._process_sequential(
            [Process(['mvn', 'clean', 'package', '-q', '-DskipTests'], path_pipeline_absolute)])

    def test_scala(self, path_pipeline_absolute):
        return self._process_sequential(
            [
                Process(['mvn', 'test', '-q', '-Dfabric=' + self.fabric],
                        path_pipeline_absolute)
            ]
        )

    def test_python(self, path_pipeline_absolute):
        return self._process_sequential(
            [
                # Install dependencies of particular pipeline
                # 1. Export dependencies to egg_info/requires.txt
                Process(['python', 'setup.py', '-q', 'egg_info'], path_pipeline_absolute),
                # 2. Extract the install_requires, tests_requires and extra_requires and install them
                Process([f"pip install -q `grep -v '^\[' *.egg-info/requires.txt`"],
                        path_pipeline_absolute, is_shell=True),
                # Run the unit test
                Process(['python', '-m', 'pytest', '-v', 'test/TestSuite.py', '--html=report.html',
                         '--self-contained-html', '--junitxml=report.xml'],
                        path_pipeline_absolute)
            ]
        )

    def _verify_unit_test_env(self):
        self.fabric = os.environ.get('FABRIC_NAME')

        if self.fabric is None:
            self._error('[i]FABRIC_NAME[/i] environment variable is required to '
                        'run your Unit tests')
            return False
        else:
            return True

    def _parse_project(self):
        self.pipelines: Dict = {}
        self.jobs: Dict = {}
        with open(self.path_project, 'r') as _in:
            self.project = yaml.safe_load(_in)
            self.jobs = self.project['jobs']
            self.pipelines = self.project['pipelines']
            self.project_language = self.project['language']

        self.pipelines_count = len(self.pipelines)
        self.jobs_count = len(self.jobs)

        jobs_str = ', '.join(map(lambda job: job['name'], self.jobs.values()))
        print('Found %s jobs: %s' % (self.jobs_count, jobs_str))

        pipelines_str = ', '.join(
            map(lambda pipeline: '%s (%s)' % (pipeline['name'], pipeline['language']), self.pipelines.values()))
        print('Found %s pipelines: %s' % (self.pipelines_count, pipelines_str))

    def _process_sequential(self, processes, time_between_each_cmd=1):
        return_code = 0
        for process in processes:
            result = subprocess.run(process.process_args, stdout=process.std_output, stderr=process.std_err,
                                    shell=process.is_shell, cwd=process.current_working_directory)
            return_code, stdout, stderr = result.returncode, result.stdout, result.stderr

            if len(stderr) > 0:
                print('   ', '\n    '.join([line.decode('utf-8') for line in stdout.splitlines()]))
                print('   ', '\n    '.join([line.decode('utf-8') for line in stderr.splitlines()]))
            else:
                print('   ', '\n    '.join([line.decode('utf-8') for line in stdout.splitlines()]))

            if return_code != 0:
                break
            time.sleep(time_between_each_cmd)
        return return_code

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


@cli.command()
@click.option('--path', help='Path to the directory containing the pbt_project.yml file')
def build(path):
    pbt = ProphecyBuildTool(path)
    pbt.build()


@cli.command()
@click.option('--path', help='Path to the directory containing the pbt_project.yml file')
def deploy(path):
    pbt = ProphecyBuildTool(path)
    pbt.deploy()


@cli.command()
@click.option('--path', help='Path to the directory containing the pbt_project.yml file')
def test(path):
    pbt = ProphecyBuildTool(path)
    pbt.run_unit_tests()


if __name__ == 'pbt':
    cli()
