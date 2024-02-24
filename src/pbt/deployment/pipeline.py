import json
import os
import re
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from . import JobData
from .uploader.PipelineUploaderManager import PipelineUploadManager
from ..client.nexus import NexusClient
from ..deployment.jobs.airflow import AirflowJobDeployment
from ..deployment.jobs.databricks import DatabricksJobsDeployment
from ..entities.project import Project
from ..utility import Either, custom_print as log, is_online_mode
from ..utils.constants import SCALA_LANGUAGE
from ..utils.exceptions import ProjectBuildFailedException
from ..utils.project_config import DeploymentMode, ProjectConfig
from ..utils.project_models import Colors, Operation, Status, StepMetadata, StepType


class PipelineDeployment:
    def __init__(
        self,
        project: Project,
        databricks_jobs: DatabricksJobsDeployment,
        airflow_jobs: AirflowJobDeployment,
        project_config: ProjectConfig,
        job_ids: Optional[List[str]] = None,
        pipelines_to_build: Optional[List[str]] = None,
    ):
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
    def _all_jobs(self) -> Dict[str, JobData]:
        return {**self.databricks_jobs.valid_databricks_jobs, **self.airflow_jobs.valid_airflow_jobs}

    def summary(self):
        summary = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            summary.append(f"Pipeline {pipeline_id} will be build and uploaded.")
        return summary

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            headers.append(
                StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline", Operation.Build, StepType.Pipeline)
            )
        return headers

    def _build_and_upload_online(self, pipeline_jobs):
        responses = []
        max_workers = os.getenv("PBT_MAX_WORKERS", 2)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.build_and_upload_pipeline, pipeline_id, pipeline_name): (
                    pipeline_id,
                    pipeline_name,
                )
                for pipeline_id, pipeline_name in pipeline_jobs
            }

            for future in as_completed(futures):
                responses.append(future.result())
        return responses

    def _build_and_upload_offline(self, pipeline_jobs):
        responses = []
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_jobs, start=1):
            try:
                log(
                    f"{Colors.OKBLUE}\nBuilding pipeline {pipeline_id} [{index}/{len(pipeline_jobs)}]{Colors.ENDC}\n\n",
                    indent=1,
                )
                response = self.build_and_upload_pipeline(pipeline_id, pipeline_name)
                responses.append(response)
            except Exception as exc:
                log(
                    f"{Colors.FAIL}Pipeline {pipeline_id} generated an exception: {exc}{Colors.ENDC}",
                    step_id=pipeline_id,
                    indent=1,
                )
        return responses

    def build_and_upload(self):
        use_threads = is_online_mode()

        if len(self._pipeline_components_from_jobs().items()) > 0:
            log(f"\n\n{Colors.OKCYAN}Building {len(self._pipeline_to_list_fabrics)} Pipelines {Colors.ENDC}\n")

        pipelines_from_job = self._pipeline_components_from_jobs().items()

        if use_threads and len(pipelines_from_job) > 1:
            return self._build_and_upload_online(pipelines_from_job)

        else:
            return self._build_and_upload_offline(pipelines_from_job)

    def build_and_upload_pipeline(self, pipeline_id, pipeline_name):
        log(step_id=pipeline_id, step_status=Status.RUNNING)

        all_available_fabrics = set(self.project_config.fabric_config.list_all_fabrics())
        relevant_fabrics_for_pipeline = [
            fabric for fabric in self._pipeline_to_list_fabrics.get(pipeline_id) if fabric in all_available_fabrics
        ]

        pipeline_builder = PackageBuilderAndUploader(
            self.project,
            pipeline_id,
            pipeline_name,
            self.project_config,
            are_tests_enabled=self.are_tests_enabled,
            fabrics=relevant_fabrics_for_pipeline,
        )
        return pipeline_builder.build_and_upload_pipeline()

    def deploy(self):
        return self.build_and_upload()

    def validate(self, treat_warning_as_errors: bool = False):
        all_pipelines = self._pipeline_components_from_jobs()
        log(f"{Colors.OKBLUE} Validating pipelines {Colors.ENDC}")

        overall_validate_status = True

        for pipeline_id, pipeline_name in all_pipelines.items():
            log(f"\n\nValidating pipeline {pipeline_name} \n")
            rdc = self.project.load_pipeline_folder(pipeline_id)
            workflow = rdc.get(".prophecy/workflow.latest.json", None)

            num_errors = 0
            num_warnings = 0

            if workflow is None:
                log(f"\n{Colors.FAIL}Empty Pipeline Found: {pipeline_name}!{Colors.ENDC}")
            else:
                workflow_json = json.loads(workflow)
                if "diagnostics" in workflow_json:
                    diagnostics = workflow_json["diagnostics"]
                    for diagnostic in diagnostics:
                        if diagnostic.get("severity") == 1:
                            log(f"\n{Colors.FAIL} {pipeline_name}: {diagnostic.get('message')}{Colors.ENDC}")
                            num_errors += 1
                        elif diagnostic.get("severity") == 2:
                            log(f"\n{Colors.WARNING} {pipeline_name}: {diagnostic.get('message')}{Colors.ENDC}")
                            num_warnings += 1
                    log(f"\n{pipeline_name} has {num_errors} errors and {num_warnings} warnings.")
                    if num_errors > 0 or (treat_warning_as_errors and num_warnings > 0):
                        log(f"\n{Colors.FAIL}Pipeline is Broken: {pipeline_name}{Colors.ENDC}")
                        overall_validate_status = False
                else:
                    log(f"\n{Colors.OKBLUE} Pipeline is validated: {pipeline_name}{Colors.ENDC}")

        if not overall_validate_status:
            sys.exit(1)
        else:
            sys.exit(0)

    def test(self):
        log(f"{Colors.OKBLUE}Testing pipelines{Colors.ENDC}")

        responses = {}
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            log(f"{Colors.OKGREEN} Testing pipeline `{pipeline_id}` {Colors.ENDC}", step_id=pipeline_id)
            log(step_id=pipeline_id, step_status=Status.RUNNING)

            pipeline_builder = PackageBuilderAndUploader(
                self.project,
                pipeline_id,
                pipeline_name,
                self.project_config,
                are_tests_enabled=True,
                fabrics=self._pipeline_to_list_fabrics.get(pipeline_id),
            )
            return_code = pipeline_builder.test()
            responses[pipeline_id] = return_code
        for pipeline_id, return_code in responses.items():
            if return_code in (0, 5):
                log(f"{Colors.OKGREEN} Pipeline test succeeded : `{pipeline_id}` {Colors.ENDC}")
            else:
                log(f"{Colors.FAIL} Pipeline test failed : `{pipeline_id}`  {Colors.ENDC}")

    def build(self, pipeline_names: str = "", ignore_build_errors: bool = False, ignore_parse_errors: bool = False):
        # these can be names and ids.
        all_pipeline_ids = self._pipeline_to_list_fabrics_full_deployment.keys()
        pipeline_ids_to_name = {
            pipeline_id: self.project.get_pipeline_name(pipeline_id)
            for pipeline_id in all_pipeline_ids
            if self.project.get_pipeline_name(pipeline_id) is not None
        }

        if pipeline_names is not None and len(pipeline_names) > 0:
            pipelines_set = {pipeline for pipeline in pipeline_names.split(",")}
            pipeline_ids_to_name = {
                pipeline_id: pipeline_name
                for pipeline_id, pipeline_name in pipeline_ids_to_name.items()
                if pipeline_name in pipelines_set
            }

            pipelines_to_skip_build = {
                pipeline_id: pipeline_name
                for pipeline_id, pipeline_name in pipeline_ids_to_name.items()
                if pipeline_name not in pipelines_set
            }

            if len(pipelines_to_skip_build) > 0:
                log(
                    f"{Colors.WARNING}Skipping build for pipelines {pipelines_to_skip_build}, Please check the ids/names of provided pipelines {Colors.ENDC}"
                )

        log(f"\n\n{Colors.OKBLUE}Building pipelines {len(pipeline_ids_to_name)}{Colors.ENDC}\n")

        build_errors = False
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_ids_to_name.items(), start=1):
            log(
                f"\n\n{Colors.OKGREEN}Building pipeline `{pipeline_name}`:[{index}/{len(pipeline_ids_to_name)}] {Colors.ENDC}\n",
                indent=1,
            )
            log(step_id=pipeline_id, step_status=Status.RUNNING)

            code = self.project.load_pipeline_folder(pipeline_id)

            if (len(code) == 0) and ignore_parse_errors:
                log(f"{Colors.WARNING}Pipeline `{pipeline_name}` code is not present {Colors.ENDC}", indent=2)
                pass

            pipeline_builder = PackageBuilderAndUploader(
                self.project,
                pipeline_id,
                pipeline_name,
                self.project_config,
                are_tests_enabled=False,
                fabrics=self._pipeline_to_list_fabrics.get(pipeline_id),
            )
            try:
                build_success = pipeline_builder.build(ignore_build_errors) == 0
            except ProjectBuildFailedException:
                build_success = False

            if not build_success:
                build_errors = True

            if build_success:
                log(f"\n{Colors.OKGREEN}Build for pipeline `{pipeline_name}` succeeded {Colors.ENDC}", indent=1)
            elif ignore_build_errors:
                log(f"\n{Colors.WARNING}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)
            else:
                log(f"\n{Colors.FAIL}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)

        if not build_errors:
            if ignore_build_errors:
                log(f"\n{Colors.WARNING}Ignoring builds Errors as --ignore-build-errors is passed.{Colors.ENDC}")
            else:
                sys.exit(1)

    @property
    def _pipeline_to_list_fabrics_selective_job(self) -> Dict[str, List[str]]:
        pipeline_id_to_fabrics_dict = {}

        for job_id, job_data in self._all_jobs.items():
            for entity in job_data.pipeline_and_fabric_ids:
                if entity.entity_id not in pipeline_id_to_fabrics_dict:
                    pipeline_id_to_fabrics_dict[entity.entity_id] = set()

                pipeline_id_to_fabrics_dict[entity.entity_id].add(entity.fabric_id)

        return {pipeline_id: list(fabric_ids) for pipeline_id, fabric_ids in pipeline_id_to_fabrics_dict.items()}

    @property
    def _pipeline_to_list_fabrics_full_deployment(self) -> Dict[str, List[str]]:
        pipelines_to_fabrics = {}
        for pipeline_id in self.project.pipelines:
            pipelines_to_fabrics[pipeline_id] = self.project_config.fabric_config.list_all_fabrics()

        return pipelines_to_fabrics

    @property
    def _pipeline_to_list_fabrics(self) -> Dict[str, List[str]]:
        if self.deployment_mode == DeploymentMode.SelectiveJob:
            return self._pipeline_to_list_fabrics_selective_job
        elif not is_online_mode():
            return {**self._pipeline_to_list_fabrics_full_deployment, **self._pipeline_to_list_fabrics_selective_job}
        else:
            return self._pipeline_to_list_fabrics_full_deployment

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for pipeline_id in list(self._pipeline_to_list_fabrics.keys()):
            pipeline_name = self.project.get_pipeline_name(pipeline_id)
            pipeline_components[pipeline_id] = pipeline_name

        return pipeline_components


# look at the nexus client, download the jar in target folder or dist folder and return or
# if it's not present in nexus, then build the jar upload to nexus and return it back.
class PackageBuilderAndUploader:
    def __init__(
        self,
        project: Project,
        pipeline_id: str,
        pipeline_name: str,
        project_config: ProjectConfig = None,
        are_tests_enabled: bool = False,
        fabrics: List = [],
    ):
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._are_tests_enabled = are_tests_enabled
        self._project = project
        self._project_langauge = project.project_language
        self._base_path = project.load_pipeline_base_path(pipeline_id)
        self._project_config = project_config
        self.fabrics = fabrics
        self.pipeline_upload_manager = PipelineUploadManager(
            self._project, self._project_config, self._pipeline_id, self._pipeline_name, self.fabrics
        )

    def _initialize_temp_folder(self):
        rdc = self._project.load_pipeline_folder(self._pipeline_id)

        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as f:
                f.write(file_content)
        self._base_path = temp_dir

    def test(self):
        if self._project_langauge == SCALA_LANGUAGE:
            return self.mvn_test()
        else:
            return self.wheel_test()

    def build(self, ignore_build_errors: bool = False):
        if self._project_langauge == SCALA_LANGUAGE:
            return self.mvn_build(ignore_build_errors)
        else:
            return self.wheel_build(ignore_build_errors)

    def build_and_upload_pipeline(self) -> Either:
        pipeline_from_nexus = self._download_pipeline_from_nexus()
        if pipeline_from_nexus is not None and pipeline_from_nexus.is_right:
            return Either(right=(self._pipeline_id, pipeline_from_nexus.right))

        if not is_online_mode() and self._project_config.skip_builds:
            log("Artifact File already exist, ignoring pipeline build", step_id=self._pipeline_id, indent=2)
            return Either(right=True)

        try:
            self._initialize_temp_folder()

            log("Initialized temp folder for building the pipeline package.", step_id=self._pipeline_id, indent=2)

            if self._project_langauge == SCALA_LANGUAGE:
                self.mvn_build()
            else:
                self.wheel_build()

            path = Project.get_pipeline_whl_or_jar(self._base_path)
            log(
                f"{Colors.OKGREEN}Pipeline package built successfully, with path {path}{Colors.ENDC}\n",
                step_id=self._pipeline_id,
                indent=2,
            )

            if self._project_config.system_config.nexus is not None:
                log("Trying to upload pipeline package to nexus.", self._pipeline_id)
                self._uploading_to_nexus(path)

            log(f"Uploading pipeline {self._pipeline_id} from path {path}", indent=2)

            return self._upload_pipeline(path)

        except Exception as e:
            log(
                message=f"{Colors.FAIL}Failed to build the pipeline package.{Colors.ENDC}",
                exception=e,
                step_id=self._pipeline_id,
                indent=1,
            )
            log(step_id=self._pipeline_id, step_status=Status.FAILED)
            return Either(left=e)

    def _upload_pipeline(self, path: str) -> Either:
        return self.pipeline_upload_manager.upload_pipeline(path)

    def _download_pipeline_from_nexus(self) -> Optional[Either]:
        if self._project_config.system_config.nexus is not None:
            log(
                "Project has nexus configured, trying to download the pipeline package.",
                step_id=self._pipeline_id,
                indent=2,
            )
            return self._download_from_nexus()
        else:
            # log("Project does not have nexus configured, building the pipeline package.", step_id=self._pipeline_id, indent=2)
            return None

    def _uploading_to_nexus(self, upload_path):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            client.upload_file(
                upload_path,
                self._project.project_id,
                self._pipeline_id,
                self._project.release_version,
                self._get_package_name(),
            )
            log("Pipeline uploaded to nexus.", step_id=self._pipeline_id)
        except Exception as e:
            log("Failed to upload pipeline to nexus", e, self._pipeline_id)

    def _download_from_nexus(self):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            package_name = self._get_package_name()
            response = client.download_file(
                package_name, self._project.project_id, self._project.release_version, self._pipeline_id
            )
            log("Pipeline downloaded from nexus.", step_id=self._pipeline_id)
            return Either(right=response)
        except Exception as e:
            log("Failed to download pipeline from nexus", e, self._pipeline_id)
            return Either(left=e)

    def _get_package_name(self):
        if self._project_langauge == SCALA_LANGUAGE:
            return f"{self._pipeline_name}.jar"
        else:
            # todo combine in a single regex
            regex_match = r"[^\w\d.]+"
            underscore_regex = r"(_)\1+"
            result = re.sub(regex_match, "_", self._pipeline_name)
            result = re.sub(underscore_regex, "_", result)
            return f"{result}-1.0-py3-none-any.whl"

    def mvn_build(self, ignore_build_errors: bool = False):
        mvn = "mvn"
        command = (
            [mvn, "package", "-DskipTests"] if not self._are_tests_enabled else [mvn, "package", "-Dfabric=default"]
        )

        log(f"Running mvn command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command, ignore_build_errors)

    def mvn_test(self):
        mvn = "mvn"
        command = [mvn, "package", "-Dfabric=default"]
        log(f"Running mvn command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command)

    def wheel_test(self):
        separator = os.sep
        test_command = ["python3", "-m", "pytest", "-v", f"{self._base_path}{separator}test{separator}TestSuite.py"]
        log(f"Running python test {test_command}", step_id=self._pipeline_id, indent=2)
        response_code = self._build(test_command)

        return response_code

    def wheel_build(self, ignore_build_error: bool = False):
        if self._are_tests_enabled:
            response_code = self.wheel_test()

            if response_code not in (0, 5):
                raise Exception(f"Python test failed for pipeline {self._pipeline_id}")

        command = ["python3", "setup.py", "bdist_wheel"]

        log(f"Running python command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command, ignore_build_error)

    # maybe we can try another iteration with yield ?
    def _build(self, command: list, ignore_build_errors: bool = False):
        env = dict(os.environ)

        # Set the MAVEN_OPTS variable
        env["MAVEN_OPTS"] = "-Xmx1024m -XX:MaxPermSize=512m -Xss32m"

        if env.get("FABRIC_NAME", None) is None:
            env["FABRIC_NAME"] = "default"  # for python test runs.

        log(f"Running command {command} on path {self._base_path}", step_id=self._pipeline_id, indent=2)
        process = subprocess.Popen(
            command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=self._base_path
        )

        def log_output(pipe, log_function):
            while True:
                # Read line from stdout or stderr, break if EOF
                output = pipe.readline()
                if process.poll() is not None and not output:
                    break
                # Decode line and print it
                response = output.decode().strip()

                # stripping unnecessary logs
                if not re.search(r"Progress \(\d+\):", response) and len(response) != 0 and response != "\n":
                    log_function(response)

        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(
            target=log_output, args=(process.stdout, lambda msg: log(msg, step_id=self._pipeline_id, indent=2))
        )
        stderr_thread = threading.Thread(
            target=log_output, args=(process.stderr, lambda msg: log(msg, step_id=self._pipeline_id, indent=2))
        )

        # Start threads
        stdout_thread.start()
        stderr_thread.start()

        # Wait for both threads to finish
        stdout_thread.join()
        stderr_thread.join()

        # Get the exit code
        return_code = process.wait()

        if return_code in (0, 5):
            log(f"Build was successful with exit code {return_code}", step_id=self._pipeline_id, indent=2)
        elif ignore_build_errors:
            log(f"Build failed with exit code {return_code}", step_id=self._pipeline_id, indent=2)
        else:
            log(f"Build failed with exit code {return_code}", step_id=self._pipeline_id, indent=2)
            raise ProjectBuildFailedException(f"Build failed with exit code {return_code}")

        return return_code


# Create for Synapse / Azure
