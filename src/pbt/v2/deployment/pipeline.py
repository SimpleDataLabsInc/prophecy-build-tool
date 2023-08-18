import os
import re
import subprocess
import tempfile
from ..utility import custom_print as print
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

from ..client.nexus import NexusClient
from ..constants import SCALA_LANGUAGE
from ..deployment.airflow_jobs import AirflowJobDeployment
from ..deployment.databricks_jobs import DatabricksJobsDeployment
from ..entities.project import Project
from ..project_models import StepMetadata
from ..project_config import ProjectConfig
from ..utility import Either


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

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            headers.append(StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline",
                                        "Build", "Pipeline"))
        return headers

    def build_and_upload(self, pipeline_ids: str):
        log_lines = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
                print(f"Building pipeline {pipeline_id}", step_name=pipeline_id)

                pipeline_builder = PackageBuilder(self.project, pipeline_id, pipeline_name, self.project_config)
                futures.append(executor.submit(lambda: pipeline_builder.build_and_get_pipeline()))

            for future in as_completed(futures):
                pipeline_id, pipeline_path = future.result()
                self.pipeline_id_to_local_path[pipeline_id] = pipeline_path

            self.has_pipelines = True
            print(f"Finished building pipelines {self.pipeline_id_to_local_path}")

    def deploy(self):
        if not self.has_pipelines:
            self.build_and_upload([])

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            all_jobs = {
                **self.databricks_jobs.valid_databricks_jobs,
                **self.airflow_jobs.valid_airflow_jobs
            }

            for job_id, job_data in all_jobs.items():
                for pipeline_id in job_data.pipelines:
                    if self._is_job_or_pipeline_in_positive_list(job_id, pipeline_id):
                        futures.append(
                            executor.submit(lambda: self._upload_pipeline_package(pipeline_id, job_data.fabric_id)))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        return responses

    def _upload_pipeline_package(self, pipeline_id: str, fabric_id: str):
        try:
            from_path = self.pipeline_id_to_local_path[pipeline_id]
            file_name = os.path.basename(from_path)

            subscribed_project_id, subscribed_project_release_version, path = Project.is_cross_project_pipeline(
                from_path)

            base_path = self.project_config.system_config.get_base_path()

            if subscribed_project_id is not None:
                to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
            else:
                to_path = f"{self.project.project_id}/{self.project.release_version}"

            final_path = f"{base_path}/{to_path}/pipeline/{file_name}"

            client = self.databricks_jobs.get_databricks_client(fabric_id)
            client.upload_src_path(from_path, final_path)
            print(f"Uploading pipeline to databricks from-path {from_path} to to-path {final_path}",
                  step_name=pipeline_id)
            return Either(right=True)
        except Exception as e:
            print(f"Error while uploading pipeline {pipeline_id} to fabric {fabric_id}", step_name=pipeline_id,
                  exceptions=e)
            return Either(left=e)

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        all_jobs = {
            **self.databricks_jobs.valid_databricks_jobs,
            **self.airflow_jobs.valid_airflow_jobs
        }

        for job_id, job_data in all_jobs.items():
            for pipeline_id in job_data.pipelines:
                if self._is_job_or_pipeline_in_positive_list(job_id, pipeline_id):
                    pipeline_name = self.project.get_pipeline_name(pipeline_id)
                    pipeline_components[pipeline_id] = pipeline_name

        all_project_pipelines = {
            pipeline_id: self.project.get_pipeline_name(pipeline_id) for pipeline_id in
            self.project.pipelines.keys()
        }

        if self.project_config.system_config.runtime_mode == "all":
            return all_project_pipelines
        else:
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

    ## todo - @pankaj  optimization can we use decorators for printing logs rather then printing in each function?
    ## printing is a cross-cutting concern, but that is the one preferred way to communicate with the scala process.
    def print_decorator(self, generator_func):
        def wrapper(*args, **kwargs):
            gen = generator_func(*args, **kwargs)
            for value in gen:
                if self._project_config.system_config.runtime_mode == "all":
                    print(value.to_json())
                else:
                    print(value.to_text())
                yield value

        return wrapper

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
        if self._project_config.system_config.nexus is not None:
            print("Project has nexus configured, trying to download the pipeline package.", step_name=self._pipeline_id)
            response = self._download_from_nexus()
        else:
            print("Project does not have nexus configured, building the pipeline package.", step_name=self._pipeline_id)
            response = Either(left=f"Project {self._project.project_id} does not have nexus configured")

        if response.is_right:
            print("Pipeline found in nexus and successfully downloaded it.", step_name=self._pipeline_id)
            return self._pipeline_id, response.right
        else:
            print(f"Pipeline not found in nexus, building the pipeline package. {response.left}", response.left,
                  self._pipeline_id)

            self._initialize_temp_folder()

            print("Initialized temp folder for building the pipeline package.", step_name=self._pipeline_id)

            if self._project_langauge == SCALA_LANGUAGE:
                self.mvn_build()
            else:
                self.wheel_build()

            path = Project.get_pipeline_whl_or_jar(self._base_path)

            if self._project_config.system_config.nexus is not None:
                print("Trying to upload pipeline package to nexus.", self._pipeline_id)
                self._uploading_to_nexus(path)

            return self._pipeline_id, path

    def _uploading_to_nexus(self, upload_path):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            response = client.upload_file(upload_path, self._project.project_id,
                                          self._pipeline_id, self._project.release_version,
                                          self._get_package_name())
            print("Pipeline uploaded to nexus.", step_name=self._pipeline_id)
        except Exception as e:
            print("Failed to upload pipeline to nexus", e, self._pipeline_id)

    def _download_from_nexus(self):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            package_name = self._get_package_name()
            response = client.download_file(package_name,
                                            self._project.project_id,
                                            self._project.release_version,
                                            self._pipeline_id)
            print("Pipeline downloaded from nexus.", step_name=self._pipeline_id)
            return Either(right=response)
        except Exception as e:
            print("Failed to download pipeline from nexus", e, self._pipeline_id)
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

        print(f"Running mvn command {command}", step_name=self._pipeline_id)

        self._build(command)

    def wheel_build(self):
        command = ["python3", "setup.py", "bdist_wheel"]

        print(f"Running python command {command}", step_name=self._pipeline_id)

        self._build(command)

    # maybe we can try another iteration with yield ?
    def _build(self, command: list):
        process = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=self._base_path)

        # Loop over stdout line by line
        while True:
            # Read line from stdout, break if EOF
            output = process.stdout.readline()
            if process.poll() is not None and not output:
                break
            # Decode line and print it
            response = output.decode().strip()

            # stripping unnecessary logs
            if not re.search(r'Progress \(\d+\):', response):
                print(response, step_name=self._pipeline_id)

        # Wait for the process to finish and get the exit code
        return_code = process.wait()

        if return_code == 0:
            print("Build was successful.", step_name=self._pipeline_id)
        else:
            print("Build failed.", step_name=self._pipeline_id)
