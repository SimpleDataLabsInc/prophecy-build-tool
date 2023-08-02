import os
import subprocess
import tempfile

from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.constants import SCALA_LANGUAGE
from src.pbt.v2.project.components.airflow_jobs import AirflowJobs
from src.pbt.v2.project.components.databricks_jobs import DatabricksJobs
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import StepMetadata
from src.pbt.v2.state_config import StateConfigAndDBTokens


class Pipelines:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 airflow_jobs: AirflowJobs,
                 state_config_and_db_tokens: StateConfigAndDBTokens):
        self.databricks_jobs = databricks_jobs
        self.airflow_jobs = airflow_jobs

        self.project = project
        self.state_config_and_db_tokens = state_config_and_db_tokens

        self.config = "2"
        self.pipeline_id_to_local_path = {}

    def headers(self):
        headers = []
        for pipeline in self.__pipeline_components_from_jobs():
            headers.append(StepMetadata(pipeline, f"Build {len(self.__pipeline_components_from_jobs())} pipeline",
                                        "Build", "Pipeline"))
        return headers

    def build(self):
        for pipeline in self.__pipeline_components_from_jobs():
            print(f"Building pipeline {pipeline}")

            pipeline_builder = PackageBuilder(self.project, self.project.load_pipeline_folder(pipeline),
                                              self.project.project_language)
            self.pipeline_id_to_local_path[pipeline] = pipeline_builder.build_and_get_pipeline()
        print(f"Finished building pipelines {self.pipeline_id_to_local_path}")
    def deploy(self):
        if len(self.pipeline_id_to_local_path) == 0:
            self.build()

        for job_json in self.databricks_jobs.valid_databricks_jobs.values():
            fabric_id = job_json.get_fabric_id
            for pipeline in job_json.pipelines():
                from_path = self.pipeline_id_to_local_path[pipeline]
                file_name = os.path.basename(from_path)
                cross_project_pipeline_details = ProjectParser.is_cross_project_pipeline(from_path)

                if cross_project_pipeline_details is not None:
                    to_path = f"dbfs:/FileStore/prophecy/artifacts/{cross_project_pipeline_details[0]}/{cross_project_pipeline_details[1]}/pipeline/{file_name}"
                else:
                    to_path = f"dbfs:/FileStore/prophecy/artifacts/dev/execution/{self.project.project_id}/{self.project.release_version}/pipeline/{file_name}"

                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens, str(fabric_id))
                print(f"Uploading pipeline to databricks from-path {from_path} to to-path {to_path}")
                client.upload_src_path(from_path, to_path)

    def __pipeline_components_from_jobs(self):
        pipeline_components = []
        for job_id, job_json in self.databricks_jobs.valid_databricks_jobs.items():
            pipeline_components.extend(job_json.pipelines())

        for job_id, job_json in self.airflow_jobs.valid_airflow_jobs.items():
            pipeline_components.extend(job_json.pipelines())

        sorted_pipeline_components = list(set(pipeline_components))
        project_all_pipelines = list(self.project.pipelines.keys())

        if self.config == "1":
            return sorted_pipeline_components
        else:
            project_all_pipelines.extend(sorted_pipeline_components)
            return project_all_pipelines


# look at the nexus client, download the jar in target folder or dist folder and return or
# if it's not present in nexus, then build the jar upload to nexus and return it back.
class PackageBuilder:

    def __init__(self, project_parser: ProjectParser, rdc: dict, project_language: str,
                 is_tests_enabled: bool = False):
        self.rdc = rdc
        self.is_tests_enabled = is_tests_enabled
        self.project_langauge = project_language
        self.project = project_parser
        self.base_path = None
        self.__initialize_temp_folder()

    def __initialize_temp_folder(self):
        temp_dir = tempfile.mkdtemp()
        for file_name, file_content in self.rdc.items():
            file_path = os.path.join(temp_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                f.write(file_content)
        self.base_path = temp_dir

    def build_and_get_pipeline(self):
        if self.project_langauge == SCALA_LANGUAGE:
            self.mvn_build()
        else:
            self.wheel_build()

        return ProjectParser.get_pipeline_whl_or_jar(self.base_path)

    def mvn_build(self):
        command = "mvn package -DskipTests" if not self.is_tests_enabled else "mvn package"
        self.__build(command)

    def wheel_build(self):
        command = "python3 setup.py bdist_wheel"
        self.__build(command)

    def __build(self, command: str):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=self.base_path)

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
