import os
import re
import subprocess
import tempfile
import threading

from . import JobData, invert_entity_to_fabric_mapping, EntityIdToFabricId
from ..client.rest_client_factory import RestClientFactory
from ..exceptions import InvalidFabricException
from ..utility import custom_print as log, Either
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

from ..client.nexus import NexusClient
from ..constants import SCALA_LANGUAGE
from ..deployment.jobs.airflow import AirflowJobDeployment
from ..deployment.jobs.databricks import DatabricksJobsDeployment
from ..entities.project import Project
from ..project_models import StepMetadata, Operation, StepType, Status
from ..project_config import ProjectConfig


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

    def build_and_upload(self, pipeline_ids: str):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []

            for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
                log(f"Building pipeline {pipeline_id}", step_id=pipeline_id)
                log(step_id=pipeline_id, step_status=Status.RUNNING)

                pipeline_builder = PackageBuilder(self.project, pipeline_id, pipeline_name, self.project_config)
                futures.append(executor.submit(lambda p=pipeline_builder: p.build_and_get_pipeline()))

            for future in as_completed(futures):
                response = future.result()
                if response.is_right:
                    (pipeline_id, pipeline_package_path) = response.right

                    self.pipeline_id_to_local_path[pipeline_id] = pipeline_package_path

                else:

                    log(step_id=pipeline_id, step_status=Status.FAILED)
                    log(f"Error building pipeline: {response.left()}", step_id=pipeline_id)

            self.has_pipelines = True

    def deploy(self):
        if not self.has_pipelines:
            self.build_and_upload([])

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            for pipeline_id, list_of_entities_to_fabric_id in self._list_all_valid_pipeline_job_id.items():
                pipeline_uploader = PipelineUploader(self.project, self.project_config, pipeline_id,
                                                     list_of_entities_to_fabric_id,
                                                     self.pipeline_id_to_local_path[pipeline_id])

                futures.append(
                    executor.submit(
                        lambda uploader=pipeline_uploader: uploader.upload_pipeline()))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        return responses

    @property
    def _list_all_valid_pipeline_job_id(self) -> Dict[str, List[EntityIdToFabricId]]:
        job_id_to_pipeline_entity_dict = {}

        for job_id, job_data in self._all_jobs.items():
            job_id_to_pipeline_entity_dict[job_id] = job_data.pipeline_and_fabric_ids

        return invert_entity_to_fabric_mapping(job_id_to_pipeline_entity_dict)

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for pipeline_id in list(self._list_all_valid_pipeline_job_id.keys()):
            pipeline_name = self.project.get_pipeline_name(pipeline_id)
            pipeline_components[pipeline_id] = pipeline_name

        return pipeline_components

    def _is_job_or_pipeline_in_positive_list(self, job_id, pipeline_id):
        return (
                (self.job_ids is None or job_id in self.job_ids) or
                (self.pipelines_to_build is None or pipeline_id in self.pipelines_to_build)
        )


# look at the nexus client, download the jar in target folder or dist folder and return or
# if it's not present in nexus, then build the jar upload to nexus and return it back.
class PackageBuilder:

    def __init__(self, project: Project, pipeline_id: str, pipeline_name: str,
                 project_config: ProjectConfig = None, is_tests_enabled: bool = False):

        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._is_tests_enabled = is_tests_enabled
        self._project = project
        self._project_langauge = project.project_language
        self._base_path = None
        self._project_config = project_config

    def _initialize_temp_folder(self):
        rdc = self._project.load_pipeline_folder(self._pipeline_id)

        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                f.write(file_content)
        self._base_path = temp_dir

    def build_and_get_pipeline(self):
        log(step_id=self._pipeline_id, step_status=Status.RUNNING)

        if self._project_config.system_config.nexus is not None:
            log("Project has nexus configured, trying to download the pipeline package.", step_id=self._pipeline_id)
            response = self._download_from_nexus()
        else:
            log("Project does not have nexus configured, building the pipeline package.", step_id=self._pipeline_id)
            response = Either(left=f"Project {self._project.project_id} does not have nexus configured")

        if response.is_right:
            log("Pipeline found in nexus and successfully downloaded it.", step_id=self._pipeline_id)
            return Either(right=(self._pipeline_id, response.right))
        else:
            log(f"Pipeline not found in nexus, building the pipeline package. {response.left}", response.left,
                self._pipeline_id)
            try:
                self._initialize_temp_folder()

                log("Initialized temp folder for building the pipeline package.", step_id=self._pipeline_id)

                if self._project_langauge == SCALA_LANGUAGE:
                    self.mvn_build()
                else:
                    self.wheel_build()

                path = Project.get_pipeline_whl_or_jar(self._base_path)

                if self._project_config.system_config.nexus is not None:
                    log("Trying to upload pipeline package to nexus.", self._pipeline_id)
                    self._uploading_to_nexus(path)

                return Either(right=(self._pipeline_id, path))
            except Exception as e:
                log(message="Failed to build the pipeline package.", exception=e, step_id=self._pipeline_id)
                return Either(left=e)

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

    def mvn_build(self):
        mvn = os.environ.get('MAVEN_HOME', 'mvn')
        command = [mvn, "package", "-DskipTests"] if not self._is_tests_enabled else [mvn, "package"]

        log(f"Running mvn command {command}", step_id=self._pipeline_id)

        self._build(command)

    def wheel_build(self):
        command = ["python3", "setup.py", "bdist_wheel"]

        log(f"Running python command {command}", step_id=self._pipeline_id)

        self._build(command)

    # maybe we can try another iteration with yield ?
    def _build(self, command: list):
        env = dict(os.environ)

        # Set the MAVEN_OPTS variable
        env["MAVEN_OPTS"] = "-Xmx1024m -XX:MaxPermSize=512m -Xss32m"

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
                if not re.search(r'Progress \(\d+\):', response):
                    log_function(response)

        # Create threads to read and log stdout and stderr simultaneously
        stdout_thread = threading.Thread(target=log_output,
                                         args=(process.stdout, lambda msg: log(msg, step_id=self._pipeline_id)))
        stderr_thread = threading.Thread(target=log_output,
                                         args=(process.stderr, lambda msg: log(msg, step_id=self._pipeline_id)))

        # Start threads
        stdout_thread.start()
        stderr_thread.start()

        # Wait for both threads to finish
        stdout_thread.join()
        stderr_thread.join()

        # Get the exit code
        return_code = process.wait()

        if return_code == 0:
            log("Build was successful.", step_id=self._pipeline_id)
        else:
            log(f"Build failed with exit code {return_code}", step_id=self._pipeline_id)


class PipelineUploader:
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 list_of_jobs: List[EntityIdToFabricId],
                 from_path: str):
        self.project = project
        self.project_config = project_config
        self.from_path = from_path
        self.pipeline_id = pipeline_id
        self.list_of_jobs = list_of_jobs
        self.all_fabrics = list(set([entity.fabric_id for entity in list_of_jobs]))
        self.rest_client_factory = RestClientFactory(project_config.deployment_state)

    def upload_pipeline(self):
        try:
            if self.from_path is None:
                raise Exception(f"Pipeline build failed {self.pipeline_id}")

            file_name = os.path.basename(self.from_path)

            subscribed_project_id, subscribed_project_release_version, path = Project.is_cross_project_pipeline(
                self.from_path)

            if subscribed_project_id is not None:
                to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
            else:
                to_path = f"{self.project.project_id}/{self.project.release_version}"

            for fabric_id in self.all_fabrics:
                fabric_info = self.project_config.deployment_state.get_fabric(fabric_id)
                db_info = fabric_info.databricks
                emr_info = fabric_info.emr

                try:
                    if db_info is not None:
                        base_path = self.project_config.system_config.get_dbfs_base_path()
                        upload_path = f"{base_path}/{to_path}/pipeline/{file_name}"
                        client = self.rest_client_factory.databricks_client(fabric_id)
                        client.upload_src_path(self.from_path, upload_path)
                        log(f"Uploading pipeline to databricks from-path {self.from_path} to to-path {upload_path} for fabric {fabric_id}",
                            step_id=self.pipeline_id)

                    else:
                        if emr_info is not None:
                            base_path = self.project_config.system_config.get_s3_base_path()
                            upload_path = f"{base_path}/{to_path}/pipeline/{file_name}"
                            client = self.rest_client_factory.s3_client(fabric_id)
                            client.upload_file(emr_info.bare_bucket(), upload_path, self.from_path)
                            log(f"Uploaded pipeline to s3, from-path {self.from_path} to to-path {upload_path} for fabric {fabric_id}",
                                step_id=self.pipeline_id)

                            if self.project.project_language == "python":
                                content = self.project.get_py_pipeline_main_file(self.pipeline_id)
                                pipeline_name = self.pipeline_id.split("/")[0]
                                launcher_path = f"{upload_path}/{pipeline_name}/launcher.py"
                                client.upload_content(emr_info.bare_bucket(), launcher_path, content)

                                log(f"Uploading py pipeline launcher to to-path {upload_path} for fabric {fabric_id}",
                                    step_id=self.pipeline_id)

                except InvalidFabricException as e:
                    log(f"Wrong fabric to upload pipeline {self.pipeline_id}", step_id=self.pipeline_id,
                        exception=e)
            log(step_status=Status.SUCCEEDED, step_id=self.pipeline_id)
            return Either(right=True)

        except Exception as e:

            log(f"Error while uploading pipeline {self.pipeline_id}", step_id=self.pipeline_id,
                exception=e)

            # this is a deviation from current behaviour where if a fabric is expired and when we upload pipelines we gulp those exceptions.
            # Change is because we are pivoting from blanket all fabric upload to only pivoted to jobs.
            # so in case a job is chosen to upload and a fabric is expired it's okay to fail that step.
            log(step_status=Status.FAILED, step_id=self.pipeline_id)
            return Either(left=e)
