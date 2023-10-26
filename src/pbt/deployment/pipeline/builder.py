import os
import re
import subprocess
import sys
import threading
from abc import ABC
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, List

from rich import print

from ..jobs import JobData
from ..jobs.airflow import AirflowJobDeployment
from ..jobs.databricks import DatabricksJobsDeployment
from ..project import Project
from ...utils.exceptions import PipelineBuildFailedException
from ...utils.project_config import ProjectConfig, DeploymentMode
from ...utils.project_models import StepMetadata, Operation, StepType, Status
from ...utils.utility import custom_print as log


class PipelineDeployment(ABC):
    def summary(self):
        pass

    def headers(self):
        pass

    def build(self):
        pass

    def deploy(self):
        pass

    def build_and_deploy(self):
        pass


class PipelineDeploymentApp(PipelineDeployment, ABC):
    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment,
                 airflow_jobs: AirflowJobDeployment,
                 project_config: ProjectConfig, job_ids: Optional[List[str]] = None,
                 pipelines_to_build: Optional[List[str]] = None):
        self.job_ids = job_ids
        self.pipelines_to_build = pipelines_to_build

        self.databricks_jobs = databricks_jobs
        self.airflow_jobs = airflow_jobs

        self.project = project
        self.project_config = project_config
        self.deployment_mode = project_config.configs_override.mode
        self.are_tests_enabled = project_config.configs_override.tests_enabled

        self.pipeline_id_to_local_path = {}
        self.has_pipelines = False  # in case deployment doesn't have any pipelines.

    @property
    def _pipeline_to_list_fabrics(self) -> Dict[str, List[str]]:
        if self.deployment_mode == DeploymentMode.SelectiveJob:
            pipeline_id_to_fabrics_dict = {}

            for job_id, job_data in self._all_jobs.items():
                for entity in job_data.pipeline_and_fabric_ids:
                    if entity.entity_id not in pipeline_id_to_fabrics_dict:
                        pipeline_id_to_fabrics_dict[entity.entity_id] = set()

                    pipeline_id_to_fabrics_dict[entity.entity_id].add(entity.fabric_id)

            return {pipeline_id: list(fabric_ids) for pipeline_id, fabric_ids in pipeline_id_to_fabrics_dict.items()}

        else:
            pipelines_to_fabrics = {}
            for pipeline_id in self.project.pipelines:
                pipelines_to_fabrics[pipeline_id] = self.project_config.fabric_config.list_all_fabrics()

            return pipelines_to_fabrics

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for pipeline_id in list(self._pipeline_to_list_fabrics.keys()):
            pipeline_name = self.project.get_pipeline_name(pipeline_id)
            pipeline_components[pipeline_id] = pipeline_name

        return pipeline_components

    @property
    def _all_jobs(self) -> Dict[str, JobData]:
        return {
            **self.databricks_jobs.valid_databricks_jobs,
            **self.airflow_jobs.valid_airflow_jobs
        }

    def summary(self):
        summary = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            summary.append(f"Pipeline {pipeline_id} will be build and uploaded.")
        return summary

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            headers.append(StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline",
                                        Operation.Build, StepType.Pipeline))
        return headers

    def upload(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            responses = []

            for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
                log(f"Building pipeline {pipeline_id}", step_id=pipeline_id)
                log(step_id=pipeline_id, step_status=Status.RUNNING)

                pipeline_builder = PackageBuilder(self.project, pipeline_id, pipeline_name, self.project_config,
                                                  are_tests_enabled=self.are_tests_enabled)
                futures.append(executor.submit(lambda p=pipeline_builder: p.build_pipeline()))

            for future in as_completed(futures):
                response = future.result()
                responses.append(response)

                if response.is_right:
                    (pipeline_id, pipeline_package_path) = response.right
                    self.pipeline_id_to_local_path[pipeline_id] = pipeline_package_path

            self.has_pipelines = True
            return responses

    def deploy(self):

        failed_response = []

        if not self.has_pipelines:
            build_response = self.build_and_upload()
            failed_response = [response for response in build_response if response.is_left]

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            for pipeline_id, list_of_entities_to_fabric_id in self._pipeline_to_list_fabrics.items():
                # ignore pipelines that are not in the list of pipelines to build.
                # they are already failed.
                if pipeline_id in self.pipeline_id_to_local_path:
                    pipeline_uploader = PipelineUploadManager(self.project, self.project_config, pipeline_id,
                                                              list_of_entities_to_fabric_id,
                                                              self.pipeline_id_to_local_path[pipeline_id])

                    futures.append(
                        executor.submit(
                            lambda uploader=pipeline_uploader: uploader.upload_pipeline()))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        # merging the failed responses from build and upload.
        return responses + failed_response


class PackageBuilder:

    def __init__(self, project: Project, pipeline_id: str, pipeline_name: str,
                 project_config: ProjectConfig = None, are_tests_enabled: bool = False, is_cmdline: bool = False):

        self._is_cmdline = is_cmdline
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._are_tests_enabled = are_tests_enabled
        self._project = project
        self._project_langauge = project.project_language
        self._base_path = None
        self._project_config = project_config

    def mvn_build(self):
        mvn = "mvn"
        command = [mvn, "package", "-DskipTests"] if not self._are_tests_enabled else [mvn, "package",
                                                                                       "-Dfabric=default"]

        log(f"Running mvn command {command}", step_id=self._pipeline_id)

        self._build(command)

    def wheel_build(self):
        if self._are_tests_enabled:
            test_command = ["python3", "-m", "pytest", "-v", f"{self._base_path}/test/TestSuite.py"]
            self._python_command(test_command)

        command = ["python3", "setup.py", "bdist_wheel"]
        self._python_command(command)

    def _python_command(self, command):
        print(f"\n[bold blue] Running python command {command}[/bold blue]")
        log(f"Running python command {command}", step_id=self._pipeline_id)
        response_code = self._build(command)
        if response_code not in (0, 5):
            raise Exception(f"Python test failed for pipeline {self._pipeline_id}")

    def _build(self, command: list):
        env = dict(os.environ)

        # Set the MAVEN_OPTS variable
        env["MAVEN_OPTS"] = "-Xmx1024m -XX:MaxPermSize=512m -Xss32m"
        env["FABRIC_NAME"] = "default"  # for python test runs.

        log(f"Running command {command} on path {self._base_path}", step_id=self._pipeline_id)
        print(f"Running command {command} on path {self._base_path}")
        process = subprocess.Popen(command, shell=(sys.platform == "win32"), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=env,
                                   cwd=self._base_path)

        def log_output(pipe, log_function):
            while True:
                # Read line from stdout or stderr, break if EOF
                output = pipe.readline()
                if process.poll() is not None and not output:
                    break
                # Decode line and print it
                response = output.decode().strip()

                # stripping unnecessary logs
                if not re.search(r'Progress \(\d+\):', response):
                    log_function(response)

        if self._is_cmdline:
            msg_callback = lambda msg: {
                log(msg, step_id=self._pipeline_id)
            }
        else:
            msg_callback = lambda msg: {
                print(msg)
            }
        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(target=log_output, args=(process.stdout, msg_callback))
        stderr_thread = threading.Thread(target=log_output, args=(process.stderr, msg_callback))

        # Start threads
        stdout_thread.start()
        stderr_thread.start()

        # Wait for both threads to finish
        stdout_thread.join()
        stderr_thread.join()

        # Get the exit code
        return_code = process.wait()

        if return_code in (0, 5):
            print(f"\n[bold green] Build was successful with exit code {return_code} [/bold green]")
            log(f"Build was successful with exit code {return_code}", step_id=self._pipeline_id)
        else:
            print(f"\n[bold red]Build failed with exit code {return_code}[/bold red]")
            log(f"Build failed with exit code {return_code}", step_id=self._pipeline_id)
            raise PipelineBuildFailedException(f"Build failed with exit code {return_code}")

        return return_code
