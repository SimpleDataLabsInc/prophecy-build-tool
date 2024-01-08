import json
import os
import re
import subprocess
import sys
import tempfile
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

from requests import HTTPError

from . import JobData
from ..client.nexus import NexusClient
from ..client.rest_client_factory import RestClientFactory
from ..deployment.jobs.airflow import AirflowJobDeployment
from ..deployment.jobs.databricks import DatabricksJobsDeployment, get_fabric_label
from ..entities.project import Project, is_cross_project_pipeline
from ..utility import custom_print as log, Either, is_online_mode
from ..utils.constants import SCALA_LANGUAGE
from ..utils.exceptions import ProjectBuildFailedException
from ..utils.project_config import ProjectConfig, EMRInfo, DataprocInfo, DeploymentMode
from ..utils.project_models import StepMetadata, Operation, StepType, Status, LogLevel, Colors


def get_package_name(project_language: str, pipeline_name: str):
    if project_language == SCALA_LANGUAGE:
        return f'{pipeline_name}.jar'
    else:
        # todo combine in a single regex
        regex_match = r"[^\w\d.]+"
        underscore_regex = r"(_)\1+"
        result = re.sub(regex_match, "_", pipeline_name)
        result = re.sub(underscore_regex, "_", result)
        return f'{result}-1.0-py3-none-any.whl'


class PipelineDeployment:
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

    def _build_and_upload_online(self, pipeline_jobs):
        responses = []
        max_workers = os.getenv("PBT_MAX_WORKERS", 2)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.build_and_upload_pipeline, pipeline_id, pipeline_name): (
                pipeline_id, pipeline_name) for pipeline_id, pipeline_name in pipeline_jobs}

            for future in as_completed(futures):
                responses.append(future.result())
        return responses

    def _build_and_upload_offline(self, pipeline_jobs):
        responses = []
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_jobs, start=1):
            try:
                log(f"{Colors.OKBLUE}\nBuilding pipeline {pipeline_id} [{index}/{len(pipeline_jobs)}]{Colors.ENDC}\n\n",
                    indent=1)
                response = self.build_and_upload_pipeline(pipeline_id, pipeline_name)
                responses.append(response)
            except Exception as exc:
                log(f"{Colors.FAIL}Pipeline {pipeline_id} generated an exception: {exc}{Colors.ENDC}",
                    step_id=pipeline_id, indent=1)
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
        relevant_fabrics_for_pipeline = [fabric for fabric in self._pipeline_to_list_fabrics.get(pipeline_id) if
                                         fabric in all_available_fabrics]

        pipeline_builder = PackageBuilderAndUploader(self.project, pipeline_id, pipeline_name,
                                                     self.project_config,
                                                     are_tests_enabled=self.are_tests_enabled,
                                                     fabrics=relevant_fabrics_for_pipeline)
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
            workflow = rdc.get('.prophecy/workflow.latest.json', None)

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

            pipeline_builder = PackageBuilderAndUploader(self.project, pipeline_id, pipeline_name,
                                                         self.project_config,
                                                         are_tests_enabled=True,
                                                         fabrics=self._pipeline_to_list_fabrics.get(pipeline_id))
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
        pipeline_ids_to_name = {pipeline_id: self.project.get_pipeline_name(pipeline_id) for pipeline_id in
                                all_pipeline_ids if self.project.get_pipeline_name(pipeline_id) is not None}

        if pipeline_names is not None and len(pipeline_names) > 0:
            pipelines_set = {pipeline for pipeline in pipeline_names.split(",")}
            pipeline_ids_to_name = {pipeline_id: pipeline_name for pipeline_id, pipeline_name in
                                    pipeline_ids_to_name.items()
                                    if pipeline_name in pipelines_set}

            pipelines_to_skip_build = {pipeline_id: pipeline_name for pipeline_id, pipeline_name in
                                       pipeline_ids_to_name.items()
                                       if pipeline_name not in pipelines_set}

            if len(pipelines_to_skip_build) > 0:
                log(f"{Colors.WARNING}Skipping build for pipelines {pipelines_to_skip_build}, Please check the ids/names of provided pipelines {Colors.ENDC}")

        log(f"\n\n{Colors.OKBLUE}Building pipelines {len(pipeline_ids_to_name)}{Colors.ENDC}\n")

        build_errors = False
        for index, (pipeline_id, pipeline_name) in enumerate(pipeline_ids_to_name.items(), start=1):
            log(f"\n\n{Colors.OKGREEN}Building pipeline `{pipeline_name}`:[{index}/{len(pipeline_ids_to_name)}] {Colors.ENDC}\n",
                indent=1)
            log(step_id=pipeline_id, step_status=Status.RUNNING)

            code = self.project.load_pipeline_folder(pipeline_id)

            if (len(code) == 0) and ignore_parse_errors:
                log(f"{Colors.WARNING}Pipeline `{pipeline_name}` code is not present {Colors.ENDC}", indent=2)
                pass

            pipeline_builder = PackageBuilderAndUploader(self.project, pipeline_id, pipeline_name,
                                                         self.project_config,
                                                         are_tests_enabled=False,
                                                         fabrics=self._pipeline_to_list_fabrics.get(pipeline_id))
            try:
                build_success = pipeline_builder.build(ignore_build_errors) == 0
            except ProjectBuildFailedException:
                build_success = False

            if build_success:
                build_errors = True

            if build_success:
                log(f"\n{Colors.OKGREEN}Build for pipeline `{pipeline_name}` succeeded {Colors.ENDC}", indent=1)
            elif ignore_build_errors:
                log(f"\n{Colors.WARNING}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)
            else:
                log(f"\n{Colors.FAIL}Build for pipeline `{pipeline_name}` failed {Colors.ENDC}", indent=1)

        if build_errors:
            sys.exit(1)
        else:
            if ignore_build_errors:
                log(f"\n{Colors.WARNING}Ignoring builds Errors as --ignore-build-errors is passed.{Colors.ENDC}")

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
            return {**self._pipeline_to_list_fabrics_full_deployment,
                    **self._pipeline_to_list_fabrics_selective_job}
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

    def __init__(self, project: Project, pipeline_id: str, pipeline_name: str,
                 project_config: ProjectConfig = None, are_tests_enabled: bool = False, fabrics: List = []):

        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._are_tests_enabled = are_tests_enabled
        self._project = project
        self._project_langauge = project.project_language
        self._base_path = project.load_pipeline_base_path(pipeline_id)
        self._project_config = project_config
        self.fabrics = fabrics
        self.pipeline_upload_manager = PipelineUploadManager(self._project, self._project_config, self._pipeline_id,
                                                             self._pipeline_name,
                                                             self.fabrics)

    def _initialize_temp_folder(self):
        rdc = self._project.load_pipeline_folder(self._pipeline_id)

        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
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
            log(f"{Colors.OKGREEN}Pipeline package built successfully, with path {path}{Colors.ENDC}\n",
                step_id=self._pipeline_id, indent=2)

            if self._project_config.system_config.nexus is not None:
                log("Trying to upload pipeline package to nexus.", self._pipeline_id)
                self._uploading_to_nexus(path)

            log(f"Uploading pipeline {self._pipeline_id} from path {path}", indent=2)

            return self._upload_pipeline(path)

        except Exception as e:
            log(message=f"{Colors.FAIL}Failed to build the pipeline package.{Colors.ENDC}", exception=e,
                step_id=self._pipeline_id, indent=1)
            log(step_id=self._pipeline_id, step_status=Status.FAILED)
            return Either(left=e)

    def _upload_pipeline(self, path: str) -> Either:
        return self.pipeline_upload_manager.upload_pipeline(path)

    def _download_pipeline_from_nexus(self) -> Optional[Either]:
        if self._project_config.system_config.nexus is not None:
            log("Project has nexus configured, trying to download the pipeline package.", step_id=self._pipeline_id,
                indent=2)
            return self._download_from_nexus()
        else:
            # log("Project does not have nexus configured, building the pipeline package.", step_id=self._pipeline_id, indent=2)
            return None

    def _uploading_to_nexus(self, upload_path):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            client.upload_file(upload_path, self._project.project_id,
                               self._pipeline_id, self._project.release_version,
                               self._get_package_name())
            log("Pipeline uploaded to nexus.", step_id=self._pipeline_id)
        except Exception as e:
            log("Failed to upload pipeline to nexus", e, self._pipeline_id)

    def _download_from_nexus(self):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            package_name = self._get_package_name()
            response = client.download_file(package_name,
                                            self._project.project_id,
                                            self._project.release_version,
                                            self._pipeline_id)
            log("Pipeline downloaded from nexus.", step_id=self._pipeline_id)
            return Either(right=response)
        except Exception as e:
            log("Failed to download pipeline from nexus", e, self._pipeline_id)
            return Either(left=e)

    def _get_package_name(self):
        if self._project_langauge == SCALA_LANGUAGE:
            return f'{self._pipeline_name}.jar'
        else:
            # todo combine in a single regex
            regex_match = r"[^\w\d.]+"
            underscore_regex = r"(_)\1+"
            result = re.sub(regex_match, "_", self._pipeline_name)
            result = re.sub(underscore_regex, "_", result)
            return f'{result}-1.0-py3-none-any.whl'

    def mvn_build(self, ignore_build_errors: bool = False):
        mvn = "mvn"
        command = [mvn, "package", "-DskipTests"] if not self._are_tests_enabled else [mvn, "package",
                                                                                       "-Dfabric=default"]

        log(f"Running mvn command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command, ignore_build_errors)

    def mvn_test(self):
        mvn = "mvn"
        command = [mvn, "package", "-Dfabric=default"]
        log(f"Running mvn command {command}", step_id=self._pipeline_id, indent=2)

        return self._build(command)

    def wheel_test(self):
        separator = {os.sep}
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

        if env.get('FABRIC_NAME', None) is None:
            env["FABRIC_NAME"] = "default"  # for python test runs.

        log(f"Running command {command} on path {self._base_path}",
            step_id=self._pipeline_id, indent=2)
        process = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
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
                if not re.search(r'Progress \(\d+\):', response) and len(response) != 0 and response != '\n':
                    log_function(response)

        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(target=log_output,
                                         args=(
                                             process.stdout, lambda msg: log(msg, step_id=self._pipeline_id, indent=2)))
        stderr_thread = threading.Thread(target=log_output,
                                         args=(
                                             process.stderr, lambda msg: log(msg, step_id=self._pipeline_id, indent=2)))

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


class PipelineUploader(ABC):

    @abstractmethod
    def upload_pipeline(self, path: str):
        pass

    def exists(self) -> bool:
        pass


class PipelineUploadManager(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str, pipeline_name: str,
                 all_fabrics: List[str]):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.all_fabrics = all_fabrics

    def upload_pipeline(self, from_path: str):
        try:
            if from_path is None:
                raise Exception(f"Pipeline build failed {self.pipeline_id}")

            file_name_with_extension = os.path.basename(from_path)

            if file_name_with_extension.endswith("-1.0.jar"):
                # scala based pipeline
                file_name = file_name_with_extension.replace("-1.0.jar", ".jar")
            else:
                # python based pipeline they are correctly generated.
                file_name = file_name_with_extension

            subscribed_project_id, subscribed_project_release_version, path = is_cross_project_pipeline(
                self.pipeline_id)

            if subscribed_project_id is not None:
                to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
            else:
                to_path = f"{self.project.project_id}/{self.project.release_version}"

            responses = []

            for fabric_id in self.all_fabrics:
                try:
                    fabric_info = self.project_config.fabric_config.get_fabric(fabric_id)
                    fabric_name = fabric_info.name
                    db_info = fabric_info.databricks
                    emr_info = fabric_info.emr
                    dataproc_info = fabric_info.dataproc

                    if db_info is not None:
                        pipeline_uploader = DatabricksPipelineUploader(self.project, self.project_config,
                                                                       self.pipeline_id, to_path, from_path,
                                                                       file_name,
                                                                       fabric_id, fabric_name)

                    elif emr_info is not None:
                        pipeline_uploader = EMRPipelineUploader(self.project, self.project_config,
                                                                self.pipeline_id, from_path, to_path,
                                                                file_name, fabric_id, fabric_name, emr_info)

                    elif dataproc_info is not None:
                        pipeline_uploader = DataprocPipelineUploader(self.project, self.project_config,
                                                                     self.pipeline_id, from_path, to_path,
                                                                     file_name, fabric_id, fabric_name, dataproc_info)
                    else:
                        log(f"{Colors.WARNING}Fabric id `{fabric_id}` with name `{fabric_name}` is not supported for pipeline upload{Colors.ENDC}",
                            step_id=self.pipeline_id, )
                        pipeline_uploader = DummyPipelineUploader()

                    responses.append(pipeline_uploader.upload_pipeline(""))

                except Exception as e:
                    log(f"{Colors.FAIL}Error while uploading pipeline {self.pipeline_id} for fabric {fabric_id}{Colors.ENDC}",
                        step_id=self.pipeline_id, exception=e, level=LogLevel.TRACE, indent=2)
                    log(step_status=Status.FAILED, step_id=self.pipeline_id)
                    responses.append(Either(left=e))

            if all([response.is_right for response in responses]):
                log(step_status=Status.SUCCEEDED, step_id=self.pipeline_id)
                return Either(right=True)
            else:
                log(step_status=Status.FAILED, step_id=self.pipeline_id)
                return Either(left=responses)

        except Exception as e:
            log(f"{Colors.FAIL}Error while uploading pipeline {self.pipeline_id}{Colors.ENDC}",
                step_id=self.pipeline_id,
                exception=e, indent=2)
            log(step_status=Status.FAILED, step_id=self.pipeline_id)
            return Either(left=e)

    def exists(self) -> bool:
        response = True
        file_name = get_package_name(self.project.project_language, self.pipeline_name)

        subscribed_project_id, subscribed_project_release_version, path = self.project.is_cross_project_pipeline(
            self.pipeline_id)

        if subscribed_project_id is not None:
            to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
        else:
            to_path = f"{self.project.project_id}/{self.project.release_version}"

        for fabric_id in self.all_fabrics:
            fabric_info = self.project_config.fabric_config.get_fabric(fabric_id)
            fabric_name = fabric_info.name
            db_info = fabric_info.databricks
            emr_info = fabric_info.emr
            dataproc_info = fabric_info.dataproc

            if db_info is not None:
                pipeline_uploader = DatabricksPipelineUploader(self.project, self.project_config,
                                                               self.pipeline_id, to_path, "", file_name,
                                                               fabric_id, fabric_name)

            elif emr_info is not None:
                pipeline_uploader = EMRPipelineUploader(self.project, self.project_config,
                                                        self.pipeline_id, to_path, "", file_name, fabric_id,
                                                        fabric_name, emr_info)

            elif dataproc_info is not None:
                pipeline_uploader = DataprocPipelineUploader(self.project, self.project_config,
                                                             self.pipeline_id, to_path, "", file_name,
                                                             fabric_id, fabric_name, dataproc_info)
            else:
                pipeline_uploader = DummyPipelineUploader()

            response = response and pipeline_uploader.exists()

        return response


class EMRPipelineUploader(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str, emr_info: EMRInfo):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id

        self.from_path = from_path
        self.to_path = to_path
        self.file_name = file_name
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name

        self.emr_info = emr_info
        self.file_name = file_name

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.base_path = self.project_config.system_config.get_s3_base_path()
        self.upload_path = f"{self.emr_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{self.file_name}".lstrip(
            "/")

    def upload_pipeline(self, path: str):

        try:
            client = self.rest_client_factory.s3_client(self.fabric_id)
            client.upload_file(self.emr_info.bare_bucket(), self.upload_path, self.from_path)

            log(f"{Colors.OKGREEN}Uploaded pipeline to s3, from path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name}{Colors.ENDC}",
                step_id=self.pipeline_id, indent=2)

            if self.project.project_language == "python":
                content = self.project.get_py_pipeline_main_file(self.pipeline_id)
                pipeline_name = self.pipeline_id.split("/")[1]
                launcher_path = f"{self.emr_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{pipeline_name}/launcher.py".lstrip(
                    '/')
                client.upload_content(self.emr_info.bare_bucket(), launcher_path, content)

                log(f"{Colors.OKGREEN}Uploading py pipeline launcher to to-path {launcher_path} for fabric {self.fabric_name}{Colors.ENDC}",
                    step_id=self.pipeline_id, indent=2)
            return Either(right=True)
        except Exception as e:
            log(f"{Colors.WARNING}Unknown Exception while uploading pipeline to emr, from-path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name}, Ignoring{Colors.ENDC}",
                exception=e, step_id=self.pipeline_id, indent=2)
            return Either(right=True)

    def exists(self) -> bool:
        return False


class DatabricksPipelineUploader(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 to_path: str, file_path: str, file_name: str, fabric_id: str, fabric_name: str):
        self.project = project
        self.project_config = project_config
        self.file_name = file_name
        self.file_path = file_path
        self.pipeline_id = pipeline_id
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name
        self.fabric_label = get_fabric_label(fabric_name, fabric_id)

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.base_path = self.project_config.system_config.get_dbfs_base_path()
        self.upload_path = f"{self.base_path}/{self.to_path}/pipeline/{self.file_name}"

    def upload_pipeline(self, path: str) -> Either:
        try:
            client = self.rest_client_factory.databricks_client(self.fabric_id)
            client.upload_src_path(self.file_path, self.upload_path)
            log(f"Uploading pipeline to databricks from-path `{self.file_path}` to path `{self.upload_path}` for fabric `{self.fabric_label}`",
                step_id=self.pipeline_id, indent=2)

            return Either(right=True)

        except HTTPError as e:
            response = e.response.content.decode('utf-8')
            log(step_id=self.pipeline_id, message=response)

            if e.response.status_code == 401 or e.response.status_code == 403:
                log(f'{Colors.WARNING}Error on uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}, but ignoring{Colors.ENDC}',
                    exception=e, step_id=self.pipeline_id, indent=2)
                return Either(right=True)
            else:
                log(f"{Colors.FAIL}HttpError on uploading pipeline to databricks from-path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}{Colors.ENDC}",
                    exception=e, step_id=self.pipeline_id, indent=2)
                return Either(left=e)

        except Exception as e:
            log(f"{Colors.FAIL}Unknown Exception on uploading pipeline to databricks from-path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}, ignoring exception{Colors.ENDC}",
                exception=e, step_id=self.pipeline_id, indent=2)
            return Either(right=True)

    def exists(self) -> bool:
        try:
            log(f"Checking if path {self.upload_path} already exists.", self.pipeline_id, indent=2)
            client = self.rest_client_factory.databricks_client(self.fabric_id)
            return client.path_exist(self.upload_path)
        except Exception as e:
            log(f"{Colors.WARNING} Failed checking path {self.upload_path}{Colors.ENDC}", step_id=self.pipeline_id,
                exception=e,
                indent=2)
            return False


class DummyPipelineUploader(PipelineUploader, ABC):

    def upload_pipeline(self, path: str):
        return Either(right=True)

    def exists(self) -> bool:
        return True


class DataprocPipelineUploader(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str,
                 dataproc_info: DataprocInfo):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id

        self.from_path = from_path
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name
        self.fabric_label = get_fabric_label(fabric_name, fabric_id)

        self.dataproc_info = dataproc_info
        self.file_name = file_name
        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.base_path = self.project_config.system_config.get_s3_base_path()
        self.upload_path = f"{self.dataproc_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{self.file_name}".lstrip(
            "/")

    def upload_pipeline(self, path: str):
        try:
            client = self.rest_client_factory.dataproc_client(self.fabric_name)
            client.put_object_from_file(self.dataproc_info.bare_bucket(), self.upload_path, self.from_path)

            log(f"{Colors.OKGREEN}Uploaded pipeline to data-proc, from-path {self.from_path} to to-path {self.upload_path} for fabric {self.fabric_label}{Colors.ENDC}",
                step_id=self.pipeline_id, indent=2)

            if self.project.project_language == "python":
                content = self.project.get_py_pipeline_main_file(self.pipeline_id)
                pipeline_name = self.pipeline_id.split("/")[1]
                launcher_path = f"{self.dataproc_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{pipeline_name}/launcher.py".lstrip(
                    '/')
                client.put_object(self.dataproc_info.bare_bucket(), launcher_path, content)

                log(f"{Colors.OKGREEN}Uploading py pipeline launcher to to-path {launcher_path} and bucket {self.dataproc_info.bare_bucket()} for fabric {self.fabric_label}{Colors.ENDC}",
                    step_id=self.pipeline_id, indent=2)
            return Either(right=True)

        except Exception as e:
            log(f"{Colors.WARNING}Unknown Exception while uploading pipeline to data-proc, from-path {self.from_path} to to-path {self.upload_path} for fabric {self.fabric_label}, ignoring exception{Colors.ENDC}",
                exception=e, step_id=self.pipeline_id, indent=2)
            return Either(right=True)

    def exists(self) -> bool:
        return True
