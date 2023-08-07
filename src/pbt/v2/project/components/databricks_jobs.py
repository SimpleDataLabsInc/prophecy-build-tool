import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from src.pbt.v2.client.databricks_client.databricks_client import DatabricksClient
from src.pbt.v2.constants import COMPONENTS_LITERAL, SCALA_LANGUAGE
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import DbtComponentsModel, ScriptComponentsModel, StepMetadata
from src.pbt.v2.project_config import JobInfo, ProjectConfig

SCRIPT_COMPONENT = "ScriptComponent"
COMPONENTS = "components"
FABRIC_ID = "fabric_id"
FABRIC_UID = "fabricUID"
DBT_COMPONENT = "DBTComponent"


class DatabricksJobsDataProcessor:
    def __init__(self, job_pbt: Dict[str, str], databricks_job: str):
        self.job_pbt = job_pbt
        self.databricks_job = databricks_job

        try:
            self.databricks_job_json = json.loads(databricks_job)
        except Exception as e:
            self.databricks_job_json = {}

    @property
    def is_valid_job(self):
        return len(self.databricks_job_json) > 0 and len(self.job_pbt) > 0

    @property
    def get_fabric_id(self):
        fabric_id = self.job_pbt.get('fabricUID', None)

        if fabric_id is not None:
            str(fabric_id)

        return fabric_id

    @property
    def databricks_json(self):
        return self.databricks_job_json.get('request', None)

    @property
    def acl(self):
        return self.databricks_job_json.get('request', {}).get('accessControlList', None)

    @property
    def get_secret_scope(self):
        return self.databricks_job_json.get('secret_scope', None)

    @property
    def pipelines(self):
        return list(set(self.job_pbt.get('pipelines', None)))


class DatabricksJobs:

    def __init__(self, project_parser: ProjectParser, project_config: ProjectConfig):
        self.project_parser = project_parser

        self.project_config = project_config

        self._db_jobs = {}
        self._pipeline_configurations = self.project_parser.pipeline_configurations
        self._databricks_jobs_without_code = {}
        self._db_clients = {}

        self.valid_databricks_jobs: Dict[str, DatabricksJobsDataProcessor] = {}
        self.config = "1"

        self._initialize_db_jobs()
        self._initialize_valid_databricks_jobs()

    '''
    job_types = {
            "Remove": self._remove_jobs(),
            "Add": self._add_jobs(),
            "Pause": self._pause_jobs(),
            "Delete": self._rename_jobs(),
            "Skipped": self._skip_jobs()
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f'{len_jobs} Airflow job' if len_jobs == 1 else f'{len_jobs} Airflow jobs'
                all_headers.append(
                    StepMetadata(f"{job_action}AirflowJobs", f"{job_action} {job_header_suffix}",
                                 f"{job_action}-jobs", "Jobs")
                )

        return all_headers'''

    def headers(self) -> List[StepMetadata]:
        job_types = {
            "Add": self._add_jobs(),
            "Refresh": self._refresh_jobs(),
            "Delete": self._delete_jobs()
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f'{len_jobs} Databricks job' if len_jobs == 1 else f'{len_jobs} Databricks jobs'
                all_headers.append(
                    StepMetadata(f"{job_action}DatabricksJobs", f"{job_action} {job_header_suffix}",
                                 f"{job_action}-jobs", "Jobs")
                )

        return all_headers

    def deploy(self):

        self._deploy_add_jobs()
        self._deploy_refresh_jobs()
        self._deploy_delete_jobs()
        self._deploy_pause_jobs()

    def get_databricks_client(self, fabric_id):
        if fabric_id not in self._db_clients:
            self._db_clients[fabric_id] = DatabricksClient.from_state_config(self.project_config,
                                                                             fabric_id)

        return self._db_clients[fabric_id]

    @property
    def databricks_job_json_for_refresh_and_new_jobs(self) -> Dict[str, DatabricksJobsDataProcessor]:
        return self._databricks_job_and_processor_data(self._add_and_refresh_jobs())

    def _initialize_db_jobs(self):
        jobs = {}

        for job_id, parsed_job in self.project_parser.jobs.items():
            if 'Databricks' in parsed_job.get('scheduler', None):
                databricks_job = self.project_parser.load_databricks_job(job_id)
                jobs[job_id] = DatabricksJobsDataProcessor(parsed_job, databricks_job)

        self._db_jobs = jobs

    def _initialize_valid_databricks_jobs(self):

        for job_id, job_data in self._db_jobs.items():
            if job_data.is_valid_job:
                self.valid_databricks_jobs[job_id] = job_data

            else:
                self._databricks_jobs_without_code[job_id] = job_data

    def _add_and_refresh_jobs(self) -> List[str]:
        return list(set(list(self._add_jobs().keys()) + list(self._refresh_jobs().keys())))

    def _databricks_job_and_processor_data(self, jobs: List[str]):

        return {
            job: self.valid_databricks_jobs.get(job, {}) for job in jobs
        }

    def _refresh_jobs(self):
        return {
            job_info.id: job_info for job_info in self.project_config.state_config.get_databricks_jobs()
            if any(
                job_info.id == job_id and job_info.fabric_id == job_data.get_fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()
            )
        }

    def _pause_jobs(self) -> List[JobInfo]:

        return [
            databricks_job for databricks_job in self.project_config.state_config.get_databricks_jobs()
            if any(
                databricks_job.job_id == job_id and databricks_job.fabric_id != job_data.get_fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()  # Check only from valid airflow jobs.
            )
        ]

    def _add_jobs(self) -> Dict[str, DatabricksJobsDataProcessor]:

        return {
            job_id: job_data for job_id, job_data in self.valid_databricks_jobs.items()
            if self.project_config.state_config.contains_jobs(job_id, str(job_data.get_fabric_id)) is False
        }

    def _delete_jobs(self) -> Dict[str, JobInfo]:
        return {
            job.id: job for job in self.project_config.state_config.get_databricks_jobs()
            if any(job.id == job_id and job.fabric_id == job_data.get_fabric_id for job_id, job_data in
                   self.valid_databricks_jobs.items()) is False
        }

    ############ Deploy Jobs ############

    def _deploy_add_jobs(self):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for job_id, job_data in self._add_jobs().items():
                fabric_id = job_data.get_fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(
                        lambda: self.get_databricks_client(fabric_id).create_job(job_data.databricks_json))),
                    # response = self.get_databricks_client(fabric_id).create_job(job_data.databricks_json)
                    # print(response)
                else:
                    print(f"In valid fabric {fabric_id}, skipping job creation for job_id {job_id}")

            for future in as_completed(futures):
                print(future.result())

    def _deploy_refresh_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:

            for (job_id, job_info) in self._refresh_jobs().items():
                fabric_id = job_info.fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(lambda: self._reset_and_patch_jobs(job_id, job_info, fabric_id)))
                else:
                    print(f"In valid fabric {fabric_id}, skipping job refresh for job_id {job_id}")

            for future in as_completed(futures):
                print(future.result())

    def _reset_and_patch_jobs(self, job_id, job_info, fabric_id):
        client = self.get_databricks_client(fabric_id)
        response = client.reset_job(job_info.scheduler_job_id,
                                    self.valid_databricks_jobs.get(
                                        job_id).databricks_json())
        client.patch_job(job_info.scheduler_job_id,
                         self.valid_databricks_jobs.get(job_id).acl())

    def _deploy_delete_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for (job_id, jobs_info) in self._delete_jobs():
                fabric = jobs_info.fabric_id

                if fabric is not None:
                    futures.append(executor.submit(
                        lambda: self.get_databricks_client(jobs_info.fabric_id).delete_job(jobs_info.scheduler_job_id)))
                else:
                    print(
                        f"In valid fabric {fabric} not deleting job {jobs_info.id} and databricks id {jobs_info.scheduler_job_id}")

            for future in as_completed(futures):
                print(future.result())

    def _deploy_pause_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:

            for jobs_info in self._pause_jobs():
                fabric = jobs_info.fabric_id

                if fabric is not None:
                    futures.append(executor.submit(
                        lambda: self.get_databricks_client(jobs_info.fabric_id).pause_job(jobs_info.scheduler_job_id)))
                else:
                    print(f"In valid fabric {fabric} for job_id {jobs_info.id} ")

            for future in as_completed(futures):
                print(future.result())


class DBTComponents:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 state_config_and_db_tokens: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.state_config = state_config_and_db_tokens.state_config
        self.state_config_and_dbt_tokens = state_config_and_db_tokens

    def headers(self):
        return self._dbt_secrets_headers() + self._dbt_profiles_to_build_headers()

    def deploy(self):
        self._upload_dbt_secrets()
        self._upload_dbt_profiles()

    @property
    def dbt_component_from_jobs(self) -> Dict[str, DbtComponentsModel]:
        job_id_to_dbt_components = {}

        for job_id, job_data in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            component_list = []

            for component in job_data.databricks_job_json.get(COMPONENTS_LITERAL, []):
                if DBT_COMPONENT in component:
                    component_list.append(component[DBT_COMPONENT])

            if len(component_list) > 0:
                # add prophecy_jobs as the default scope.
                secret_scope = job_data.get_secret_scope
                job_id_to_dbt_components[job_id] = DbtComponentsModel(job_data.get_fabric_id, secret_scope,
                                                                      component_list)

        return job_id_to_dbt_components

    def _upload_dbt_profiles(self):

        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:

                if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                      None) is not None:
                    client = self.databricks_jobs.get_databricks_client(dbt_component_model.fabric_id)
                    client.upload_content(components['profileContent'], components['profilePath'])

    def _upload_dbt_secrets(self):
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for dbt_component in dbt_component_model.components:
                if dbt_component.get('sqlFabricId', None) is not None and dbt_component.get('secretKey',
                                                                                            None) is not None:
                    client = self.databricks_jobs.get_databricks_client(str(dbt_component_model.fabric_id)) \
                        .check_and_create_secret_scope(dbt_component_model.secret_scope)

                    sqlClient = self.databricks_jobs.get_databricks_client(dbt_component['sqlFabricId'])

                    url = sqlClient.host

                    sql_fabric_token = sqlClient.token
                    git_token = self.state_config.git_token_for_project(dbt_component['projectId'])

                    master_token = f"{sql_fabric_token};{git_token}"

                    sqlClient.create_secret(dbt_component_model.secret_scope, dbt_component['secretKey'], master_token)

                    print(
                        f"Uploaded dbt secrets {dbt_component['secretKey']} to scope {dbt_component_model.secret_scope}")

    def _dbt_profiles_to_build_headers(self):
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

    def _dbt_secrets_headers(self):
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


class ScriptComponents:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 state_config_and_db_tokens: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.script_jobs = self._script_components_from_jobs()
        self.state_config_and_db_tokens = state_config_and_db_tokens
        self.state_config = state_config_and_db_tokens.state_config

    def headers(self) -> List[StepMetadata]:
        if len(self.script_jobs) > 0:
            return [StepMetadata("script_components", f"Upload {len(self.script_jobs)} script components", "upload",
                                 "scripts")]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, script_components in self._script_components_from_jobs().items():
                for script in script_components.scripts:
                    futures.append(executor.submit(self._upload_content, script, script_components.fabric_id))

            for future in as_completed(futures):
                print(future.result())

    def _upload_content(self, script: dict, fabric_id: str):
        client = self.databricks_jobs.get_databricks_client(str(fabric_id))
        response = client.upload_content(content=script.get('content', None), path=script.get('path', None))
        print(response)

    def _script_components_from_jobs(self):
        job_id_to_script_components = {}

        for jobs_id, job_data in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            script_component_list = []

            for components in job_data.databricks_job_json.get(COMPONENTS_LITERAL, []):
                if SCRIPT_COMPONENT in components:
                    script_component_list.append(components[SCRIPT_COMPONENT])

            fabric_id = job_data.get_fabric_id
            job_id_to_script_components[jobs_id] = ScriptComponentsModel(fabric_id, script_component_list)

        return job_id_to_script_components


class PipelineConfigurations:
    def __init__(self, project: ProjectParser, databricks_jobs: DatabricksJobs,
                 project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project_config = project_config

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0:
            return [StepMetadata("pipeline_configurations", f"Upload {len(all_configs)} pipeline configurations",
                                 "upload", "pipeline")]
        else:
            return []

    def deploy(self):

        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for pipeline_id, configurations in self.pipeline_configurations.items():

                path = self.project_config.system_config.get_base_path()
                pipeline_path = f'{path}/{pipeline_id}'

                for configuration_name, configuration_content in configurations.items():
                    configuration_path = f'{pipeline_path}/{configuration_name}.json'

                    for fabric_id in self.project_config.state_config.db_fabrics():
                        client = self.databricks_jobs.get_databricks_client(str(fabric_id))
                        futures.append(
                            executor.submit(lambda: client.upload_content(configuration_content, configuration_path)))

            for future in as_completed(futures):
                print(future.result())
