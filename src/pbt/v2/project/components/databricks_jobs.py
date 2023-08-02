import json
import subprocess
from typing import Dict, List

from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.constants import COMPONENTS_LITERAL, SCALA_LANGUAGE
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import DbtComponentsModel, ScriptComponentsModel, StepMetadata
from src.pbt.v2.state_config import JobsInfo, StateConfigAndDBTokens

SCRIPT_COMPONENT = "ScriptComponent"
COMPONENTS = "components"
FABRIC_ID = "fabric_id"
FABRIC_UID = "fabricUID"
DBT_COMPONENT = "DBTComponent"


class DatabricksJobsJson:
    def __init__(self, parsed_job_info: Dict[str, str], databricks_job: str):
        self.parsed_job_info = parsed_job_info
        self.databricks_job = databricks_job
        try:
            self.databricks_job_json = json.loads(databricks_job)
        except Exception as e:
            self.databricks_job_json = {}

    def is_valid_job(self):
        return len(self.databricks_job_json) > 0 and len(self.parsed_job_info) > 0

    @property
    def get_fabric_id(self):
        return self.parsed_job_info.get('fabricUID', None)

    def get_db_job_json(self):
        return self.databricks_job_json.get('request', None)

    def get_secret_scope(self):
        return self.databricks_job_json.get('secret_scope', None)

    def pipelines(self):
        return list(set(self.parsed_job_info.get('pipelines', None)))


class DatabricksJobs:

    def __init__(self, project: ProjectParser, state_config_and_db_tokens: StateConfigAndDBTokens):
        self.project = project

        self.state_config = state_config_and_db_tokens.state_config
        self.db_tokens = state_config_and_db_tokens.db_tokens
        self.state_config_and_db_tokens = state_config_and_db_tokens

        self.db_jobs = {}
        self.pipeline_configurations = self.project.pipeline_configurations
        self.valid_databricks_jobs: Dict[str, DatabricksJobsJson] = {}
        self.databricks_jobs_without_code = {}

        self.config = "1"

        self.__initialize_db_jobs()
        self.__initialize_valid_databricks_jobs()

    def __initialize_db_jobs(self):
        jobs = {}
        for job_id, parsed_job in self.project.jobs.items():
            if 'Databricks' in parsed_job.get('scheduler', None):
                databricks_job = self.project.load_databricks_job(job_id)
                jobs[job_id] = DatabricksJobsJson(parsed_job, databricks_job)

        self.db_jobs = jobs

    def __initialize_valid_databricks_jobs(self):
        for job_id, job_jsons in self.db_jobs.items():
            if job_jsons.is_valid_job():
                self.valid_databricks_jobs[job_id] = job_jsons
            else:
                self.databricks_jobs_without_code[job_id] = job_jsons

    def headers(self) -> List[StepMetadata]:
        headers = []
        for job_id, job_json in self.__add_jobs().items():
            headers.append(
                StepMetadata(job_id, f"Adding job {job_id} for fabric {job_json.get_fabric_id}", "add-job", "job"))

        for job_id, job_json in self.__refresh_jobs().items():
            headers.append(
                StepMetadata(job_id, f"Refreshing job {job_id} for fabric {job_json.get_fabric_id}", "refresh-job",
                             "job"))

        for job_id, jobs_info in self.__delete_jobs().items():
            headers.append(
                StepMetadata(job_id,
                             f"Deleting job {job_id} for fabric {jobs_info.fabric_id} deployed with id {jobs_info.scheduler_job_id}",
                             "delete-job", "job"))

        return headers

    def deploy(self):
        self.__deploy_add_jobs()
        self.__deploy_refresh_jobs()
        self.__deploy_delete_jobs()
        self.__deploy_pause_jobs()

    @property
    def databricks_job_json_for_refresh_and_new_jobs(self) -> Dict[str, DatabricksJobsJson]:
        return self.__databricks_job_json(self.__add_and_refresh_jobs())

    def __add_and_refresh_jobs(self) -> List[str]:
        return list(set(list(self.__add_jobs().keys()) + list(self.__refresh_jobs().keys())))

    def __databricks_job_json(self, jobs: List[str]):
        databricks_jobs = {}

        for job in jobs:
            databricks_jobs[job] = self.valid_databricks_jobs.get(job, {})

        return databricks_jobs

    def __refresh_jobs(self):
        refresh_jobs = {}

        for job_id, job_jsons in self.valid_databricks_jobs.items():
            if self.state_config.contains_jobs(job_id, str(job_jsons.get_fabric_id)):
                refresh_jobs[job_id] = job_jsons

        return refresh_jobs

    def __pause_jobs(self) -> List[JobsInfo]:
        pause_jobs = []
        for job in self.state_config.get_databricks_jobs():

            exist_job_with_same_fabric = any(
                job.id == job_id and job.get_fabric_id == job_jsons.parsed_job_info[FABRIC_UID] for job_id, job_jsons in
                self.valid_databricks_jobs.items())

            if exist_job_with_same_fabric is False and any(
                    job.id == job_id for job_id in self.valid_databricks_jobs.keys()):
                pause_jobs.append(job)

        return pause_jobs

    def __add_jobs(self) -> Dict[str, DatabricksJobsJson]:
        add_jobs = {}

        for job_id, job_jsons in self.valid_databricks_jobs.items():
            if self.state_config.contains_jobs(job_id, str(job_jsons.get_fabric_id)) is False:
                add_jobs[job_id] = job_jsons

        return add_jobs

    def __delete_jobs(self) -> Dict[str, JobsInfo]:
        deleted_jobs = {}

        for job in self.state_config.get_databricks_jobs():
            if any(job_id == job.id and job.get_fabric_id == job_content[FABRIC_UID] for job_id, job_content in
                   self.db_jobs.items()) is False:
                deleted_jobs[job.id] = job

        return deleted_jobs

    ############ Deploy Jobs ############

    def __deploy_add_jobs(self):
        for job_id, job_jsons in self.__add_jobs().items():
            fabric_id = job_jsons.get_fabric_id
            if fabric_id is not None:
                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens, str(fabric_id))
                response = client.create_job(job_jsons.get_db_job_json())
                print(response)
            else:
                print(f"In valid fabric {fabric_id}")

    def __deploy_refresh_jobs(self):
        for (job_id, job_jsons) in self.__refresh_jobs().items():
            fabric_id = job_jsons.get_fabric_id
            if fabric_id is not None:
                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens, str(fabric_id))
                response = client.reset_job(job_id, job_jsons.get_db_job_json())
                print(response)
            else:
                print(f"In valid fabric {fabric_id}")

    def __deploy_delete_jobs(self):
        for (job_id, jobs_info) in self.__delete_jobs():
            fabric = self.state_config.get_fabric(jobs_info.fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens, jobs_info.fabric_id)
                response = client.delete_job(jobs_info.scheduler_job_id)
                print(response)
            else:
                print(f"In valid fabric {fabric}")

    def __deploy_pause_jobs(self):
        for jobs_info in self.__pause_jobs():
            fabric = self.state_config.get_fabric(jobs_info.fabric_id)
            if fabric is not None:
                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens, jobs_info.fabric_id)
                response = client.pause_job(jobs_info.scheduler_job_id)
                print(response)
            else:
                print(f"In valid fabric {fabric}")


class DBTComponents:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 state_config_and_db_tokens: StateConfigAndDBTokens):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.state_config = state_config_and_db_tokens.state_config
        self.state_config_and_dbt_tokens = state_config_and_db_tokens

    def headers(self):
        return self.__dbt_secrets_headers() + self.__dbt_profiles_to_build_headers()

    def deploy(self):
        self.__upload_dbt_secrets()
        self.__upload_dbt_profiles()

    def __upload_dbt_profiles(self):
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:
                if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                      None) is not None:
                    DatabricksClient.from_state_config(self.state_config_and_dbt_tokens, dbt_component_model.fabric_id) \
                        .upload_content(components['profileContent'], components['profilePath'])

    def __upload_dbt_secrets(self):
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for dbt_component in dbt_component_model.components:
                if dbt_component.get('sqlFabricId', None) is not None and dbt_component.get('secretKey',
                                                                                            None) is not None:
                    DatabricksClient.from_state_config(self.state_config_and_dbt_tokens,
                                                       str(dbt_component_model.fabric_id)) \
                        .create_scope(dbt_component_model.secret_scope)
                    url = DatabricksClient.from_state_config(self.state_config_and_dbt_tokens,
                                                             fabric_id=dbt_component['sqlFabricId']).host
                    sql_fabric_token = self.state_config_and_dbt_tokens.db_tokens.get(dbt_component['sqlFabricId'], "")
                    git_token = self.state_config.git_token_for_project(dbt_component['projectId'])

                    master_token = f"{sql_fabric_token};{git_token}"

                    DatabricksClient.from_host_and_token(url, sql_fabric_token) \
                        .create_secret(dbt_component_model.secret_scope, dbt_component['secretKey'], master_token)

                    print(
                        f"Uploaded dbt secrets {dbt_component['secretKey']} to scope {dbt_component_model.secret_scope}")

    def __dbt_profiles_to_build_headers(self):
        total_dbt_profiles = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():

            for components in dbt_component_model.components:
                if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                      None) is not None:
                    total_dbt_profiles = total_dbt_profiles + 1

        if total_dbt_profiles > 0:
            return [StepMetadata("DBTProfileComponents", f"Upload {total_dbt_profiles} dbt profiles", "Upload",
                                 "dbt_profiles")]
        else:
            return []

    def __dbt_secrets_headers(self):
        total_dbt_secrets = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():

            for components in dbt_component_model.components:
                if components.get('sqlFabricId', None) is not None and components.get('secretKey', None) is not None:
                    total_dbt_secrets = total_dbt_secrets + 1

        if total_dbt_secrets > 0:
            return [StepMetadata("DBTSecretsComponents", f"Upload {total_dbt_secrets} dbt secrets", "Upload",
                                 "dbt_secrets")]
        else:
            return []

    @property
    def dbt_component_from_jobs(self) -> Dict[str, DbtComponentsModel]:
        job_id_to_dbt_components = {}

        for job_id, job_jsons in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            component_list = []

            for component in job_jsons.databricks_job_json.get(COMPONENTS_LITERAL, []):
                if DBT_COMPONENT in component:
                    component_list.append(component[DBT_COMPONENT])

            if len(component_list) > 0:
                # add prophecy_jobs as the default scope.
                secret_scope = job_jsons.get_secret_scope()
                job_id_to_dbt_components[job_id] = DbtComponentsModel(job_jsons.get_fabric_id, secret_scope,
                                                                      component_list)

        return job_id_to_dbt_components


class ScriptComponents:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 state_config_and_db_tokens: StateConfigAndDBTokens):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.script_jobs = self.__script_components_from_jobs()
        self.state_config_and_db_tokens = state_config_and_db_tokens
        self.state_config = state_config_and_db_tokens.state_config

    def __script_components_from_jobs(self):
        job_id_to_script_components = {}

        for jobs_id, job_jsons in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            script_component_list = []

            for components in job_jsons.databricks_job_json.get(COMPONENTS_LITERAL, []):
                if SCRIPT_COMPONENT in components:
                    script_component_list.append(components[SCRIPT_COMPONENT])

            fabric_id = job_jsons.get_fabric_id
            job_id_to_script_components[jobs_id] = ScriptComponentsModel(fabric_id, script_component_list)

        return job_id_to_script_components

    def headers(self) -> List[StepMetadata]:
        if len(self.script_jobs) > 0:
            return [StepMetadata("script_components", f"Upload {len(self.script_jobs)} script components", "upload",
                                 "scripts")]
        else:
            return []

    def deploy(self):
        for job_id, script_components in self.__script_components_from_jobs().items():
            for script in script_components.scripts:
                client = DatabricksClient.from_state_config(self.state_config_and_db_tokens,
                                                            str(script_components.fabric_id))
                response = client.upload_content(content=script.get('content'), path=script.get('path'))
                print(response)


class PipelineConfigurations:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 state_config_and_db_tokens: StateConfigAndDBTokens):
        self.databricks_jobs = databricks_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.state_config = state_config_and_db_tokens.state_config
        self.state_config_db_tokens = state_config_and_db_tokens

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0:
            return [StepMetadata("pipeline_configurations", f"Upload {len(all_configs)} pipeline configurations",
                                 "upload", "pipeline")]
        else:
            return []

    def deploy(self):
        for pipeline_id, configurations in self.pipeline_configurations.items():
            path = f"dbfs:/FileStore/prophecy/artifacts/dev/execution/{pipeline_id}"
            for configuration_name, configuration_content in configurations.items():
                actual_path = path + "/" + configuration_name + ".json"
                for fabric_id, token in self.state_config_db_tokens.db_tokens.items():
                    client = DatabricksClient.from_state_config(self.state_config_db_tokens, str(fabric_id))
                    client.upload_content(configuration_content, actual_path)

