import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from requests import HTTPError
from tenacity import RetryError

from . import JobInfoAndOperation, OperationType
from ..client.databricks_client import DatabricksClient
from ..constants import COMPONENTS_LITERAL
from ..entities.project import Project
from ..exceptions import InvalidFabricException
from ..project_models import DbtComponentsModel, ScriptComponentsModel, StepMetadata, Operation, StepType, Status
from ..project_config import JobInfo, ProjectConfig
from ..utility import custom_print as log, Either

SCRIPT_COMPONENT = "ScriptComponent"
COMPONENTS = "components"
FABRIC_ID = "fabric_id"
DBT_COMPONENT = "DBTComponent"


class DatabricksJobs:
    def __init__(self, job_pbt: Dict[str, str], databricks_job: str):
        self.job_pbt = job_pbt
        self.databricks_job = databricks_job

        try:
            self.databricks_job_json = json.loads(databricks_job)
        except Exception:
            self.databricks_job_json = {}

    @property
    def is_valid_job(self):
        return len(self.databricks_job_json) > 0 and len(self.job_pbt) > 0

    @property
    def fabric_id(self):
        fabric_id = self.job_pbt.get('fabricUID', None)

        if fabric_id is not None:
            return str(fabric_id)

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

    @property
    def name(self):
        return self.job_pbt.get('name', None)

    @property
    def is_paused(self):
        return self.job_pbt.get('enabled', False)


class DatabricksJobsDeployment:
    _ADD_JOBS_STEP_ID = "add-db-jobs"
    _REFRESH_JOBS_STEP_ID = "refresh-db-jobs"
    _DELETE_JOBS_STEP_ID = "delete-db-jobs"
    _PAUSE_JOBS_STEP_ID = "pause-db-jobs"

    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project

        self.project_config = project_config

        self._pipeline_configurations = self.project.pipeline_configurations
        self._db_clients = {}

        self.config = "1"

        self._db_jobs = self._initialize_db_jobs()
        self.valid_databricks_jobs, self._databricks_jobs_without_code = self._initialize_valid_databricks_jobs()

    def summary(self):

        summary = []

        for id in self._add_jobs().keys():
            summary.append(f"Adding job {id}")

        for id in self._refresh_jobs().keys():
            summary.append(f"Refreshing job {id}")

        for id in self._delete_jobs().keys():
            summary.append(f"Deleting job {id}")

        for job in self._pause_jobs():
            summary.append(f"Pausing job {job.id}")

        return summary

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Refresh: self._REFRESH_JOBS_STEP_ID,
            Operation.Remove: self._DELETE_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID
        }

    def headers(self) -> List[StepMetadata]:
        job_types = {
            Operation.Add: self._add_jobs(),
            Operation.Refresh: self._refresh_jobs(),
            Operation.Remove: self._delete_jobs(),
            Operation.Pause: self._pause_jobs()
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f'{len_jobs} Databricks job' if len_jobs == 1 else f'{len_jobs} Databricks jobs'

                step_id = self._operation_to_step_id[job_action]

                all_headers.append(
                    StepMetadata(step_id, f"{job_action.value} {job_header_suffix}",
                                 job_action, StepType.Job)
                )

        return all_headers

    def deploy(self):
        responses = self._deploy_add_jobs() + \
                    self._deploy_refresh_jobs() + \
                    self._deploy_delete_jobs() + \
                    self._deploy_pause_jobs()

        return responses

    def get_databricks_client(self, fabric_id):
        if fabric_id not in self._db_clients:
            self._db_clients[fabric_id] = DatabricksClient.from_state_config(self.project_config,
                                                                             fabric_id)

        return self._db_clients[fabric_id]

    @property
    def databricks_job_json_for_refresh_and_new_jobs(self) -> Dict[str, DatabricksJobs]:
        return self._databricks_job_and_processor_data(self._add_and_refresh_jobs())

    def _initialize_db_jobs(self):
        jobs = {}

        for job_id, parsed_job in self.project.jobs.items():
            if 'Databricks' in parsed_job.get('scheduler', None):
                databricks_job = self.project.load_databricks_job(job_id)
                jobs[job_id] = DatabricksJobs(parsed_job, databricks_job)

        return jobs

    def _initialize_valid_databricks_jobs(self):

        valid_databricks_jobs = {}
        databricks_jobs_without_code = {}

        for job_id, job_data in self._db_jobs.items():
            if job_data.is_valid_job:
                valid_databricks_jobs[job_id] = job_data

            else:
                databricks_jobs_without_code[job_id] = job_data

        return valid_databricks_jobs, databricks_jobs_without_code

    def _add_and_refresh_jobs(self) -> List[str]:
        return list(set(list(self._add_jobs().keys()) + list(self._refresh_jobs().keys())))

    def _databricks_job_and_processor_data(self, jobs: List[str]):

        return {
            job: self.valid_databricks_jobs.get(job, {}) for job in jobs
        }

    def _refresh_jobs(self):
        return {
            job_info.id: job_info for job_info in self.project_config.state_config.get_databricks_jobs
            if any(
                job_info.id == job_id and job_info.fabric_id == job_data.fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()
            )
        }

    def _pause_jobs(self) -> List[JobInfo]:

        return [
            databricks_job for databricks_job in self.project_config.state_config.get_databricks_jobs
            if any(
                databricks_job.id == job_id and databricks_job.fabric_id != job_data.fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()  # Check only from valid airflow jobs.
            )
        ]

    def _add_jobs(self) -> Dict[str, DatabricksJobs]:

        return {
            job_id: job_data for job_id, job_data in self.valid_databricks_jobs.items()
            if self.project_config.state_config.contains_jobs(job_id, str(job_data.fabric_id)) is False
        }

    def _delete_jobs(self) -> Dict[str, JobInfo]:
        return {
            job.id: job for job in self.project_config.state_config.get_databricks_jobs
            if not any(job.id == job_id for job_id in
                       self.valid_databricks_jobs.keys())
        }

    '''Deploy Jobs '''

    def _deploy_add_job(self, fabric_id, job_id, job_data, step_id):
        try:
            client = self.get_databricks_client(fabric_id)
            response = client.create_job(job_data.databricks_json)
            log(f"Created job {job_id} in fabric {fabric_id} response {response['job_id']}",
                step_id=step_id)

            job_info = JobInfo.create_db_job(job_data.name, job_id, fabric_id, response['job_id'],
                                             self.project_config.state_config.release_tag, job_data.is_paused)
            return Either(right=JobInfoAndOperation(job_info, OperationType.CREATED))
        except Exception as e:
            log(f"Error creating job {job_id} in fabric {fabric_id}", exceptions=e,
                step_id=step_id)
            return Either(left=e)

    def _deploy_add_jobs(self):

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for job_id, job_data in self._add_jobs().items():
                fabric_id = job_data.fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(
                        lambda: self._deploy_add_job(fabric_id, job_id, job_data, self._ADD_JOBS_STEP_ID)))
                else:
                    log(f"In valid fabric {fabric_id}, skipping job creation for job_id {job_id}",
                        step_id=self._ADD_JOBS_STEP_ID)
        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        self._update_state(responses, self._operation_to_step_id[Operation.Add])

        return responses

    def _deploy_refresh_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:

            for (job_id, job_info) in self._refresh_jobs().items():
                fabric_id = job_info.fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(lambda: self._reset_and_patch_jobs(job_id, job_info, fabric_id)))
                else:
                    log(f"In valid fabric {fabric_id}, skipping job refresh for job_id {job_id}")

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        self._update_state(responses, self._operation_to_step_id[Operation.Refresh])

        return responses

    def _reset_and_patch_jobs(self, job_id, job_info, fabric_id):
        try:
            client = self.get_databricks_client(fabric_id)
            job_data = self.valid_databricks_jobs.get(job_id)
            client.reset_job(job_info.external_job_id, job_data.databricks_json)
            acl = job_data.acl

            if acl is not None:
                log(f"Refreshed job {job_id} with external job id {job_info.external_job_id} Patching job acl for job {job_id}",
                    step_id=self._REFRESH_JOBS_STEP_ID)
                client.patch_job_acl(job_info.external_job_id, acl)
            else:
                log(f"Refreshed job {job_id} with external job id {job_info.external_job_id}, No ACL found for job {job_id}",
                    step_id=self._REFRESH_JOBS_STEP_ID)

            return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))
        except RetryError as e:
            underlying_exception = e.last_attempt.exception()

            if underlying_exception is HTTPError:
                response = underlying_exception.response.content.decode('utf-8')
                if underlying_exception.response.status_code == 400 and f"Job {job_info.external_job_id} does not exist." in response:
                    # job deleted.
                    log(f"Job {job_id} external_job_id {job_info.external_job_id} deleted in fabric {fabric_id}, trying to recreate it",
                        step_id=self._REFRESH_JOBS_STEP_ID)
                    return self._deploy_add_job(fabric_id, job_id, self.valid_databricks_jobs.get(job_id),
                                                step_id=self._REFRESH_JOBS_STEP_ID)
            else:
                log(
                    f'Error while deleting job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {job_info.fabric_id}',
                    e, step_id=self._DELETE_JOBS_STEP_ID)
                return Either(left=e)

        except Exception as e:
            log(
                f'Error while resetting job with {job_info.external_job_id} and job-id {job_id} for fabric {fabric_id}',
                e, step_id=self._REFRESH_JOBS_STEP_ID)
            return Either(left=e)

    def _deploy_delete_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, job_info in self._delete_jobs().items():
                futures.append(executor.submit(lambda: self._delete_job(job_info)))

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        self._update_state(responses, self._operation_to_step_id[Operation.Remove])

        return responses

    def _delete_job(self, job_info: JobInfo):
        fabric = job_info.fabric_id
        if fabric is not None:
            try:
                client = self.get_databricks_client(job_info.fabric_id)
                client.delete_job(job_info.external_job_id)
                log(
                    f'Successfully delete job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {job_info.fabric_id}',
                    step_id=self._DELETE_JOBS_STEP_ID)
                return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))
            except HTTPError as e:
                response = e.response.content.decode('utf-8')
                if e.response.status_code == 400 and "does not exist." in response:
                    log("Job already deleted, skipping", step_id=self._DELETE_JOBS_STEP_ID)
                    return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))
                else:
                    log(
                        f'Error while deleting job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {job_info.fabric_id}',
                        e, step_id=self._DELETE_JOBS_STEP_ID)
                    return Either(left=e)
            except Exception as e:
                log(
                    f'Error while deleting job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {job_info.fabric_id}',
                    e, step_id=self._DELETE_JOBS_STEP_ID)
                return Either(left=e)

        else:
            message = f"Invalid fabric {fabric} not deleting job {job_info.id} and databricks id {job_info.external_job_id}"
            log(message, step_id=self._DELETE_JOBS_STEP_ID)
            return Either(left=InvalidFabricException(message))

    def _deploy_pause_jobs(self):
        futures = []

        def pause_job(fabric_id, job_info):
            external_job_id = job_info.external_job_id

            try:
                client = self.get_databricks_client(fabric_id)
                client.pause_job(external_job_id)
                log(f"Paused job {external_job_id} in fabric {fabric_id}", step_id=self._PAUSE_JOBS_STEP_ID)
                return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))
            except Exception as e:
                log(f"Error pausing job {external_job_id} in fabric {fabric_id}, Ignoring this ", exceptions=e,
                    step_id=self._PAUSE_JOBS_STEP_ID)
                return Either(right=True)

        with ThreadPoolExecutor(max_workers=3) as executor:

            for jobs_info in self._pause_jobs():
                fabric = jobs_info.fabric_id

                if fabric is not None:
                    futures.append(executor.submit(lambda: pause_job(fabric, jobs_info)))
                else:
                    log(f"In valid fabric {fabric} for job_id {jobs_info.id} ", step_id=self._PAUSE_JOBS_STEP_ID)

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        self._update_state(responses, self._operation_to_step_id[Operation.Pause])
        return responses

    def _update_state(self, responses: List, step_id: str):
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=step_id)
            else:
                log(step_status=Status.SUCCEEDED, step_id=step_id)


class DBTComponents:
    _DBT_SECRETS_COMPONENT_STEP_NAME = "DBTSecretsComponents"
    _DBT_PROFILES_COMPONENT_STEP_NAME = "DBTProfileComponents"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment,
                 state_config_and_db_tokens: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.state_config = state_config_and_db_tokens.state_config
        self.state_config_and_dbt_tokens = state_config_and_db_tokens

    def summary(self):
        summary = []

        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():

            for components in dbt_component_model.components:
                if components.get('sqlFabricId', None) is not None and components.get('secretKey', None) is not None:
                    summary.append(
                        f"Uploading dbt secrets for job {job_id} component {components.get('nodeName')} ", )

                if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                      None) is not None:
                    summary.append(f"Uploading dbt profiles for job {job_id} component {components.get('nodeName')} ")

        return summary

    def headers(self):
        return self._dbt_secrets_headers() + self._dbt_profiles_to_build_headers()

    def deploy(self):
        self._upload_dbt_secrets() + self._upload_dbt_profiles()

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
                job_id_to_dbt_components[job_id] = DbtComponentsModel(job_data.fabric_id, secret_scope,
                                                                      component_list)

        return job_id_to_dbt_components

    def _upload_dbt_profiles(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
                for components in dbt_component_model.components:

                    if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                          None) is not None:
                        futures.append(executor.submit(
                            lambda: self._upload_dbt_profile(dbt_component_model, components)))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME)
            else:
                log(step_status=Status.SUCCEEDED, step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME)

        return responses

    def _upload_dbt_profile(self, dbt_component_model: DbtComponentsModel, components: Dict[str, str]):
        try:
            client = self.databricks_jobs.get_databricks_client(dbt_component_model.fabric_id)
            client.upload_content(components['profileContent'], components['profilePath'])

            log(f"Successfully uploaded dbt profile {components['profilePath']}",
                step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME)
            return Either(right=True)
        except Exception as e:
            log(f"Error while uploading dbt profile {components['profilePath']}", exceptions=e,
                step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME)
            return Either(left=e)

    def _upload_dbt_secrets(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
                for dbt_component in dbt_component_model.components:
                    if dbt_component.get('sqlFabricId', None) is not None and dbt_component.get('secretKey',
                                                                                                None) is not None:
                        futures.append(
                            executor.submit(lambda: self._upload_dbt_secret(dbt_component_model, dbt_component)))

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME)
            else:
                log(step_status=Status.SUCCEEDED, step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME)

        return responses

    def _upload_dbt_secret(self, dbt_component_model, dbt_component):
        try:
            self.databricks_jobs.get_databricks_client(str(dbt_component_model.fabric_id)) \
                .check_and_create_secret_scope(dbt_component_model.secret_scope)

            sqlClient = self.databricks_jobs.get_databricks_client(dbt_component['sqlFabricId'])

            sql_fabric_token = sqlClient.token
            git_token = self.state_config.git_token_for_project(dbt_component['projectId'])

            master_token = f"{sql_fabric_token};{git_token}"

            sqlClient.create_secret(dbt_component_model.secret_scope, dbt_component['secretKey'], master_token)
            log(f"Successfully uploaded dbt secret for component {dbt_component['nodeName']}",
                step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME)
            return Either(right=True)
        except Exception as e:
            log(f"Error while uploading dbt secret  for component {dbt_component['nodeName']}", exceptions=e,
                step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME)
            return Either(left=e)

    def _dbt_profiles_to_build_headers(self):
        total_dbt_profiles = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():

            for components in dbt_component_model.components:
                if components.get('profilePath', None) is not None and components.get('profileContent',
                                                                                      None) is not None:
                    total_dbt_profiles = total_dbt_profiles + 1

        if total_dbt_profiles > 0:
            return [StepMetadata(self._DBT_PROFILES_COMPONENT_STEP_NAME, f"Upload {total_dbt_profiles} dbt profiles",
                                 Operation.Upload,
                                 StepType.DbtProfile)]
        else:
            return []

    def _dbt_secrets_headers(self):
        total_dbt_secrets = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():

            for components in dbt_component_model.components:
                if components.get('sqlFabricId', None) is not None and components.get('secretKey', None) is not None:
                    total_dbt_secrets = total_dbt_secrets + 1

        if total_dbt_secrets > 0:
            return [
                StepMetadata(self._DBT_SECRETS_COMPONENT_STEP_NAME, f"Upload {total_dbt_secrets} dbt secrets",
                             Operation.Upload,
                             StepType.DbtSecret)]
        else:
            return []


class ScriptComponents:
    _STEP_ID = "script_components"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment,
                 project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.script_jobs = self._script_components_from_jobs()
        self.state_config = project_config.state_config

    def summary(self) -> List[str]:
        script_summary = []

        for job_id, script_job_list in self.script_jobs.items():
            for script_job in script_job_list.scripts:
                script_summary.append(f"Uploading script {script_job['nodeName']} for job {job_id}")

        return script_summary

    def headers(self) -> List[StepMetadata]:
        if len(self.script_jobs) > 0:
            return [StepMetadata(self._STEP_ID, f"Upload {len(self.script_jobs)} script components", Operation.Upload,
                                 StepType.Script)]
        else:
            return []

    def deploy(self):
        futures = []
        log(step_status=Status.RUNNING, step_id=self._STEP_ID)

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, script_components in self._script_components_from_jobs().items():
                for script in script_components.scripts:
                    futures.append(executor.submit(self._upload_content, script, script_components.fabric_id))

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

        # copy paste for now, will refactor later
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=self._STEP_ID)
            else:
                log(step_status=Status.SUCCEEDED, step_id=self._STEP_ID)

        return responses

    def _upload_content(self, script: dict, fabric_id: str):
        node_name = script.get('nodeName')

        try:
            client = self.databricks_jobs.get_databricks_client(str(fabric_id))
            client.upload_content(content=script.get('content', None), path=script.get('path', None))

            log(f"Uploaded script component {node_name} to fabric {fabric_id}", step_id=self._STEP_ID)
            return Either(right=True)
        except Exception as e:
            log(f"Error uploading script component {node_name} to fabric {fabric_id}", step_id=self._STEP_ID,
                exceptions=e)
            return Either(left=e)

    def _script_components_from_jobs(self):
        job_id_to_script_components = {}

        for jobs_id, job_data in self.databricks_jobs.databricks_job_json_for_refresh_and_new_jobs.items():
            script_component_list = []

            for components in job_data.databricks_job_json.get(COMPONENTS_LITERAL, []):
                if SCRIPT_COMPONENT in components:
                    script_component_list.append(components[SCRIPT_COMPONENT])

            fabric_id = job_data.fabric_id
            job_id_to_script_components[jobs_id] = ScriptComponentsModel(fabric_id, script_component_list)

        return job_id_to_script_components


class PipelineConfigurations:
    _STEP_ID = "pipeline_configurations"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment,
                 project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project_config = project_config

    def summary(self) -> List[str]:
        summary = []
        for ids in self.pipeline_configurations.keys():
            summary.append(f"Uploading pipeline configurations for pipeline {ids}")

        return summary

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0:
            return [StepMetadata(self._STEP_ID,
                                 f"Upload {len(all_configs)} pipeline configurations",
                                 Operation.Upload, StepType.PipelineConfiguration)]
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
                        futures.append(executor.submit(
                            lambda: self._upload_configuration(str(fabric_id), configuration_content,
                                                               configuration_path)))

        responses = []

        for future in as_completed(futures):
            responses.append(future.result())

            # copy paste for now, will refactor later
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=self._STEP_ID)
            else:
                log(step_status=Status.SUCCEEDED, step_id=self._STEP_ID)

        return responses

    def _upload_configuration(self, fabric_id, configuration_content, configuration_path):
        try:
            client = self.databricks_jobs.get_databricks_client(str(fabric_id))
            client.upload_content(configuration_content, configuration_path)
            log(f"Uploaded pipeline configuration on path {configuration_path}",
                step_id=self._STEP_ID)
            return Either(right=True)
        except Exception as e:
            log(f"Failed to upload pipeline configuration for path {configuration_path}", e,
                self._STEP_ID)
            return Either(left=e)
