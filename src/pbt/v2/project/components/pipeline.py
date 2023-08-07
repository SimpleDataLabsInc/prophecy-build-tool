import os
import re
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.pbt.v2.client.nexus import NexusClient
from src.pbt.v2.constants import SCALA_LANGUAGE
from src.pbt.v2.project.components.airflow_jobs import AirflowJobs
from src.pbt.v2.project.components.databricks_jobs import DatabricksJobs
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import StepMetadata
from src.pbt.v2.project_config import ProjectConfig
from src.pbt.v2.utility import Either


class Pipelines:
    def __init__(self, project_parser: ProjectParser, databricks_jobs: DatabricksJobs,
                 airflow_jobs: AirflowJobs,
                 project_config: ProjectConfig):

        self.databricks_jobs = databricks_jobs
        self.airflow_jobs = airflow_jobs

        self.project_parser = project_parser
        self.project_config = project_config

        self.pipeline_id_to_local_path = {}
        self.has_pipelines = False  # in case project doesn't have any pipelines.

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            headers.append(StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline",
                                        "Build", "Pipeline"))
        return headers

    def build_and_upload(self, pipeline_ids: str):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
                print(f"Building pipeline {pipeline_id}")

                pipeline_builder = PackageBuilder(self.project_parser, pipeline_id, pipeline_name, self.project_config)
                futures.append(executor.submit(lambda: pipeline_builder.build_and_get_pipeline()))

            for future in as_completed(futures):
                pipeline_id, pipeline_path = future.result()
                self.pipeline_id_to_local_path[pipeline_id] = pipeline_path

            self.has_pipelines = True
            print(f"Finished building pipelines {self.pipeline_id_to_local_path}")

    def deploy(self):
        if not self.has_pipelines:
            self.build_and_upload([])

        for job_json in self.databricks_jobs.valid_databricks_jobs.values():

            fabric_id = job_json.get_fabric_id

            # Deploy pipelines only if they are referenced in a job.
            for pipeline_id in job_json.pipelines:

                from_path = self.pipeline_id_to_local_path[pipeline_id]
                file_name = os.path.basename(from_path)

                subscribed_project_id, subscribed_project_release_version, path = ProjectParser.is_cross_project_pipeline(
                    from_path)

                base_path = self.project_config.system_config.get_base_path()

                if subscribed_project_id is not None:
                    to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
                else:
                    to_path = f"{self.project_parser.project_id}/{self.project_parser.release_version}"

                final_path = f"{base_path}/{to_path}/pipeline/{file_name}"

                client = self.databricks_jobs.get_databricks_client(fabric_id)
                print(f"Uploading pipeline to databricks from-path {from_path} to to-path {final_path}")
                client.upload_src_path(from_path, final_path)

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for job_id, job_data in self.databricks_jobs.valid_databricks_jobs.items():
            for pipeline_id in job_data.pipelines:
                pipeline_name = self.project_parser.get_pipeline_name(pipeline_id)
                pipeline_components[pipeline_id] = pipeline_name

        for job_id, job_data in self.airflow_jobs.valid_airflow_jobs.items():
            for pipeline_id in job_data.pipelines:
                pipeline_name = self.project_parser.get_pipeline_name(pipeline_id)
                pipeline_components[pipeline_id] = pipeline_name

        all_project_pipelines = {
            pipeline_id: self.project_parser.get_pipeline_name(pipeline_id) for pipeline_id in
            self.project_parser.pipelines.keys()
        }

        return {**pipeline_components, **all_project_pipelines}
        # todo introduce release mode
        # if self.release_mode == "1":
        #     return sorted_pipeline_components
        # else:
        #     project_all_pipelines.extend(sorted_pipeline_components)
        #     return project_all_pipelines


# look at the nexus client, download the jar in target folder or dist folder and return or
# if it's not present in nexus, then build the jar upload to nexus and return it back.
class PackageBuilder:

    def __init__(self, project_parser: ProjectParser, pipeline_id: str, pipeline_name: str,
                 project_config: ProjectConfig = None, is_tests_enabled: bool = False):

        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._is_tests_enabled = is_tests_enabled
        self._project_parser = project_parser
        self._project_langauge = project_parser.project_language
        self._base_path = None
        self._project_config = project_config

    def _initialize_temp_folder(self):
        rdc = self._project_parser.load_pipeline_folder(self._pipeline_id)

        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                f.write(file_content)
        self._base_path = temp_dir

    def build_and_get_pipeline(self):
        response = self._try_download_from_nexus()
        if response.is_right:
            return self._pipeline_id, response.right
        else:
            print('Pipeline not found in nexus, building it', response.left)
            self._initialize_temp_folder()
            if self._project_langauge == SCALA_LANGUAGE:
                self.mvn_build()
            else:
                self.wheel_build()

            path = ProjectParser.get_pipeline_whl_or_jar(self._base_path)
            self._try_uploading_to_nexus(path)
            return self._pipeline_id, path

    def _try_uploading_to_nexus(self, upload_path):
        try:
            client = NexusClient.initialize_nexus_client(self._project_config)
            response = client.upload_file(upload_path, self._project_parser.project_id,
                                          self._pipeline_id, self._project_parser.release_version, self._get_package_name())
            print('Pipeline uploaded to nexus', response)
        except Exception as e:
            print('Error while uploading pipeline to nexus', e)

    def _try_download_from_nexus(self):
        try:
            # atm no reliable support for nexus.
            client = NexusClient.initialize_nexus_client(self._project_config)
            package_name = self._get_package_name()
            response = client.download_file(package_name,
                                            self._project_parser.project_id,
                                            self._project_parser.release_version,
                                            self._pipeline_id)
            return Either(right=response)
        except Exception as e:
            print('Error while downloading pipeline from nexus', e)
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
        command = ["mvn", "package", "-DskipTests"] if not self._is_tests_enabled else ["mvn", "package"]
        self._build(command)

    def wheel_build(self):
        command = ["python3", "setup.py", "bdist_wheel"]
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
            print(output.decode().strip())

        # Wait for the process to finish and get the exit code
        return_code = process.wait()

        if return_code == 0:
            print("Build was successful.")
        else:
            print("Build failed.")
