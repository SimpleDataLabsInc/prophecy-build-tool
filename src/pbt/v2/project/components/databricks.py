import json
from typing import Dict, List

from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.constants import COMPONENTS_LITERAL, SECRET_SCOPE
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import DbtComponentsModel, ScriptComponentsModel, StepMetadata

SCRIPT_COMPONENT = "ScriptComponent"
COMPONENTS = "components"
FABRIC_ID = "fabric_id"
FABRIC_UID = "fabricUID"
DBT_COMPONENT = "DBTComponent"


class DatabricksJobsJson:
    def __init__(self, prophecy_job_json: Dict[str, str], databricks_job_json: Dict[str, str]):
        self.prophecy_job_json = prophecy_job_json
        self.databricks_job_json = databricks_job_json

    def is_valid_job(self):
        return len(self.databricks_job_json) > 0 and len(self.prophecy_job_json) > 0


class DatabricksJobs:

    def __init__(self, project: ProjectParser):
        self.project = project
        self.state_config = project.state_config
        self.db_jobs = {}
        self.pipeline_configurations = self.project.pipeline_configurations
        self.valid_databricks_jobs = {}
        self.databricks_jobs_without_code = {}

        self.config = "1"

        self.__initialize_db_jobs()
        self.__initialize_valid_databricks_jobs()

    def __initialize_db_jobs(self):
        jobs = {}
        for job_id, parsed_job in self.project.jobs.items():
            if 'Databricks' in parsed_job['scheduler']:

                databricks_job = self.project.load_databricks_job(job_id)
                databricks_job_json = {}

                if databricks_job is not None:
                    databricks_job_json = json.loads(databricks_job)

                jobs[job_id] = DatabricksJobsJson(parsed_job, databricks_job_json)

        self.db_jobs = jobs

    def __initialize_valid_databricks_jobs(self):
        for job_id, job_jsons in self.db_jobs.items():
            if job_jsons.is_valid_job():
                self.valid_databricks_jobs[job_id] = job_jsons
            else:
                self.databricks_jobs_without_code[job_id] = job_jsons

    def headers(self) -> List[StepMetadata]:
        headers = []
        for job_id, fabric_id, content in self.__add_jobs():
            headers.append(StepMetadata(job_id, f"Adding job {job_id} for fabric {fabric_id}", "add-job", "job"))

        for job_id, fabric_id in self.__refresh_jobs():
            headers.append(
                StepMetadata(job_id, f"Refreshing job {job_id} for fabric {fabric_id}", "refresh-job", "job"))

        for job_id, fabric_id, job_run_id in self.__delete_jobs():
            headers.append(
                StepMetadata(job_id, f"Deleting job {job_id} for fabric {fabric_id} deployed with id {job_run_id}"))

        return headers

    def deploy(self):
        self.__deploy_add_jobs()
        self.__deploy_refresh_jobs()
        self.__deploy_delete_jobs()
        self.__deploy_pause_jobs()

    @property
    def databricks_job_json_for_refresh_and_new_jobs(self):
        return self.__databricks_job_json(self.__add_and_refresh_jobs())

    def __add_and_refresh_jobs(self):
        job_ids = []
        for job_id, fabric_id in self.__add_jobs():
            job_ids.append(job_id)

        for job_id, fabric_id in self.__refresh_jobs():
            job_ids.append(job_id)

        return job_ids

    def __databricks_job_json(self, jobs: List[str]):
        databricks_jobs = {}

        for job in jobs:
            content = self.project.load_databricks_job(job)
            if content is not None:
                databricks_jobs[job] = json.loads(content)
            else:
                print(f"Job {job} not found in project")
        return databricks_jobs

    def __refresh_jobs(self):
        refresh_jobs = []

        for job_id, job_json in self.valid_databricks_jobs.items():
            if self.state_config.contains_jobs(job_id, job_json.prophecy_job_json[FABRIC_UID]):
                refresh_jobs.append((job_id, job_json.prophecy_job_json[FABRIC_UID]))

        return refresh_jobs

    def __pause_jobs(self):
        pause_jobs = []

        for job in self.state_config.get_jobs():
            exist_job_with_same_fabric = any(
                job.id == job_id and job.fabric_id == job_jsons.prophecy_job_json[FABRIC_UID] for job_id, job_jsons in
                self.valid_databricks_jobs.items())
            if exist_job_with_same_fabric is False and any(
                    job.id == job_id for job_id in self.valid_databricks_jobs.keys()):
                pause_jobs.append((job.id, job.fabric_id))

        return pause_jobs

    def __add_jobs(self):
        add_jobs = []

        for job_id, job_content in self.valid_databricks_jobs.items():
            if self.state_config.contains_jobs(job_id, job_content[FABRIC_UID]) is False:
                add_jobs.append((job_id, job_content[FABRIC_UID], job_content))

        return add_jobs

    def __delete_jobs(self):
        deleted_jobs = []

        for job in self.state_config.jobs:
            if any(job_id == job.id and job.fabric_id == job_content[FABRIC_UID] for job_id, job_content in
                   self.db_jobs.items()) is False:
                deleted_jobs.append((job.id, job.fabric_id, job.scheduler_job_id))

        return deleted_jobs

    ############ Deploy Jobs ############

    def __deploy_add_jobs(self):
        for (job_id, fabric_id, job_content) in self.__add_jobs():
            fabric = self.state_config.get_fabric(fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config, {}, fabric_id)
                response = client.create_job(job_id, job_content)
                print(response)
            else:
                print("In valid fabri")

    def __deploy_refresh_jobs(self):
        for (job_id, fabric_id) in self.__refresh_jobs():
            fabric = self.state_config.get_fabric(fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config, {}, fabric_id)
                response = client.reset_job(job_id)
                print(response)

    def __deploy_delete_jobs(self):
        for (job_id, fabric_id, job_run_id) in self.__delete_jobs():
            fabric = self.state_config.get_fabric(fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config, {}, fabric_id)
                response = client.delete_job(job_id, job_run_id)
                print(response)

    def __deploy_pause_jobs(self):
        for (job_id, scheduler_job_id, fabric_id) in self.__pause_jobs():
            fabric = self.state_config.get_fabric(fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config, {}, fabric_id)
                response = client.pause_job(scheduler_job_id)
                print(response)


class DBTComponents:
    def __init__(self, databricks_jobs: DatabricksJobs):
        self.databricks_jobs = databricks_jobs

    def headers(self):
        pass

    @property
    def dbt_component_from_jobs(self) -> Dict[str, DbtComponentsModel]:
        job_id_to_dbt_components = {}

        for job_id, databricks_job_json in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            component_list = []

            for component in databricks_job_json[COMPONENTS_LITERAL]:
                if DBT_COMPONENT in component:
                    component_list.append(component[DBT_COMPONENT])

            if len(component_list) > 0:
                fabric_id = databricks_job_json[FABRIC_ID]
                # add prophecy_jobs as the default scope.
                secret_scope = databricks_job_json[SECRET_SCOPE]
                job_id_to_dbt_components[job_id] = DbtComponentsModel(fabric_id, secret_scope, component_list)

        return job_id_to_dbt_components


class ScriptComponents:
    def __init__(self, databricks_jobs: DatabricksJobs):
        self.databricks_jobs = databricks_jobs
        self.script_jobs = self.__script_components_from_jobs()

    def __script_components_from_jobs(self):
        job_id_to_script_components = {}

        for jobs_id, databricks_job_json in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            script_component_list = []

            for components in databricks_job_json[COMPONENTS_LITERAL]:
                if SCRIPT_COMPONENT in components:
                    script_component_list.append(components[SCRIPT_COMPONENT])

            fabric_id = databricks_job_json[FABRIC_ID]
            job_id_to_script_components[jobs_id] = ScriptComponentsModel(fabric_id, script_component_list)

        return job_id_to_script_components

    def headers(self):
        pass

    def deploy(self):
        pass


class PipelineConfigurations:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs):
        self.databricks_jobs = databricks_jobs
        self.pipeline_configurations = project.pipeline_configurations

    def headers(self):
        pass

    def deploy(self):
        pass


class Pipelines:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.config = "1"

    def headers(self):
        pass

    def deploy(self):
        pass

    def __pipeline_components_from_jobs(self):
        if self.config == "1":
            pipeline_components = []
            for job_id, databricks_job_json in self.databricks_jobs.valid_databricks_jobs.items():
                pipeline_components.extend(databricks_job_json.prophecy_job_json['pipelines'])

            return list(set(pipeline_components))
        else:
            return self.project.pipelines
