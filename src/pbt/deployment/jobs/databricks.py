import json
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional
import os
from requests import HTTPError
from .utils import modify_databricks_json_for_private_artifactory, extract_dependency_project_ids
from ...client.rest_client_factory import RestClientFactory
from ...deployment import JobInfoAndOperation, OperationType, JobData, EntityIdToFabricId
from ...entities.project import Project
from ...utility import custom_print as log, Either, is_online_mode
from ...utils.constants import COMPONENTS_LITERAL
from ...utils.exceptions import InvalidFabricException
from ...utils.project_config import JobInfo, ProjectConfig, await_futures_and_update_states
from ...utils.project_models import DbtComponentsModel, ScriptComponentsModel, StepMetadata, Operation, StepType, Colors

SCRIPT_COMPONENT = "ScriptComponent"
COMPONENTS = "components"
FABRIC_ID = "fabric_id"
DBT_COMPONENT = "DBTComponent"


def get_fabric_label(name: str, id: str):
    if name is not None and len(name) > 0:
        return name
    else:
        return id


# todo add a new abstract class for DatabricksJobs and AirflowJobs
class DatabricksJobs(JobData, ABC):
    def __init__(self, pbt_job_json: Dict[str, str], databricks_job: str, fabric_override: Optional[str] = None):
        self.pbt_job_json = pbt_job_json
        self.databricks_job = databricks_job
        self.fabric_override = fabric_override

        try:
            self.databricks_job_json = json.loads(databricks_job)
        except Exception:
            self.databricks_job_json = {}

    @property
    def is_valid_job(self):
        return len(self.databricks_job_json) > 0 and len(self.pbt_job_json) > 0

    @property
    def fabric_id(self):
        if self.fabric_override is not None and len(self.fabric_override) > 0:
            return self.fabric_override
        else:
            fabric_id = self.pbt_job_json.get("fabricUID", None)

            if fabric_id is not None:
                return str(fabric_id)

        return fabric_id

    def dag_name(self) -> Optional[str]:
        pass

    def has_dbt_component(self) -> bool:
        pass

    def is_disabled(self) -> bool:
        pass

    def job_files(self):
        pass

    def is_enabled(self) -> bool:
        pass

    def validate_prophecy_managed_checksum(self, salt: str) -> bool:
        pass

    @property
    def pipeline_and_fabric_ids(self) -> List[EntityIdToFabricId]:
        pipeline_and_fabric_ids = []

        for pipeline_id in self.pipelines:
            pipeline_and_fabric_ids.append(EntityIdToFabricId(pipeline_id, self.fabric_id))

        return pipeline_and_fabric_ids

    @property
    def databricks_json(self):
        return self.databricks_job_json.get("request", None)

    @property
    def acl(self):
        return self.databricks_job_json.get("request", {}).get("access_control_list", None)

    @property
    def get_secret_scope(self):
        return self.databricks_job_json.get("secret_scope", None)

    @property
    def pipelines(self):
        return list(set(self.pbt_job_json.get("pipelines", None)))

    @property
    def name(self):
        return self.pbt_job_json.get("name", None)

    @property
    def is_paused(self):
        return not self.pbt_job_json.get("enabled", False)


class DatabricksJobsDeployment:
    _ADD_JOBS_STEP_ID = "add-db-jobs"
    _REFRESH_JOBS_STEP_ID = "refresh-db-jobs"
    _DELETE_JOBS_STEP_ID = "delete-db-jobs"
    _PAUSE_JOBS_STEP_ID = "pause-db-jobs"

    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project

        self.project_config = project_config
        self.deployment_state = project_config.jobs_state
        self.deployment_run_override_config = project_config.configs_override
        self.fabric_configs = project_config.fabric_config

        self._pipeline_configurations = self.project.pipeline_configurations
        self._rest_client_factory = RestClientFactory.get_instance(
            RestClientFactory, fabric_config=self.project_config.fabric_config
        )

        (self._db_jobs, self._skipping_jobs) = self._initialize_db_jobs()
        self.valid_databricks_jobs, self._databricks_jobs_without_code = self._initialize_valid_databricks_jobs()

    def summary(self):
        summary = []

        summary.extend([f"Adding job {id}" for id in list(self._add_jobs().keys())])
        summary.extend([f"Refreshing job {id}" for id in list(self._refresh_jobs().keys())])
        summary.extend([f"Deleting job {id}" for id in list(self._delete_jobs().keys())])
        summary.extend([f"Pausing job {job.id}" for job in list(self._pause_jobs())])

        return summary

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Refresh: self._REFRESH_JOBS_STEP_ID,
            Operation.Remove: self._DELETE_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID,
        }

    def headers(self) -> List[StepMetadata]:
        job_types = {
            Operation.Add: self._add_jobs(),
            Operation.Refresh: self._refresh_jobs(),
            Operation.Remove: self._delete_jobs(),
            Operation.Pause: self._pause_jobs(),
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f"{len_jobs} Databricks job" if len_jobs == 1 else f"{len_jobs} Databricks jobs"

                step_id = self._operation_to_step_id[job_action]

                all_headers.append(
                    StepMetadata(step_id, f"{job_action.value} {job_header_suffix}", job_action, StepType.Job)
                )

        return all_headers

    def deploy(self) -> List[Either]:
        if len(self.headers()) > 0:
            log(f"{Colors.OKBLUE}\nDeploying databricks jobs{Colors.ENDC}\n\n")

        responses = (
            self._deploy_add_jobs()
            + self._deploy_refresh_jobs()
            + self._deploy_delete_jobs()
            + self._deploy_pause_jobs()
            + self._deploy_skipping_jobs()
        )

        return responses

    def get_databricks_client(self, fabric_id):
        return self._rest_client_factory.databricks_client(fabric_id)

    @property
    def databricks_job_json_for_refresh_and_new_jobs(self) -> Dict[str, DatabricksJobs]:
        return self._databricks_jobs_data(self._add_and_refresh_job_ids())

    def _deploy_skipping_jobs(self) -> List[Either]:
        responses = []
        allowed_fabrics = self.fabric_configs.list_all_fabrics()

        if len(self._skipping_jobs) > 0:
            log(f"{Colors.OKBLUE}\n\nSkipping databricks jobs{Colors.ENDC}\n")

        for job_id, job in self._skipping_jobs.items():
            log(
                f"[SKIP] Job {job_id} skipped as it belongs to fabric-id {job.fabric_id} but allowed fabric-ids are {allowed_fabrics}"
            )

        return responses

    def _initialize_db_jobs(self):
        jobs = {}
        skipping_jobs = {}
        for job_id, pbt_job_json in self.project.jobs.items():
            fabric_override = self.deployment_run_override_config.find_fabric_override_for_job(job_id)
            fabric_override = str(fabric_override) if fabric_override is not None else None

            job_fabric = pbt_job_json.get("fabricUID", None)
            job_fabric = str(job_fabric) if job_fabric is not None else None

            does_fabric_exist = (
                self.fabric_configs.get_fabric(job_fabric) is not None
                or self.fabric_configs.get_fabric(fabric_override) is not None
            )

            databricks_job = self.project.load_databricks_job(job_id)

            if "Databricks" in pbt_job_json.get("scheduler", None) and databricks_job is not None:
                if self.deployment_run_override_config.is_job_to_run(job_id):
                    if does_fabric_exist:
                        jobs[job_id] = DatabricksJobs(pbt_job_json, databricks_job, fabric_override)

                    else:
                        skipping_jobs[job_id] = DatabricksJobs(pbt_job_json, databricks_job, fabric_override)

        return jobs, skipping_jobs

    def _initialize_valid_databricks_jobs(self):
        valid_databricks_jobs = {}
        databricks_jobs_without_code = {}

        for job_id, job_data in self._db_jobs.items():
            if job_data.is_valid_job:
                valid_databricks_jobs[job_id] = job_data

            else:
                databricks_jobs_without_code[job_id] = job_data

        return valid_databricks_jobs, databricks_jobs_without_code

    def _add_and_refresh_job_ids(self) -> List[str]:
        return list(set(list(self._add_jobs().keys()) + list(self._refresh_jobs().keys())))

    def _databricks_jobs_data(self, jobs: List[str]) -> Dict[str, DatabricksJobs]:
        return {job: self.valid_databricks_jobs.get(job, {}) for job in jobs}

    def _refresh_jobs(self) -> Dict[str, JobInfo]:
        return {
            job_info.id: job_info
            for job_info in self.deployment_state.databricks_jobs
            if any(
                job_info.id == job_id and job_info.fabric_id == job_data.fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()
            )
        }

    def _pause_jobs(self) -> List[JobInfo]:
        return [
            databricks_job
            for databricks_job in self.deployment_state.databricks_jobs
            if any(
                databricks_job.id == job_id and databricks_job.fabric_id != job_data.fabric_id
                for job_id, job_data in self.valid_databricks_jobs.items()
            )
        ]

    def _add_jobs(self) -> Dict[str, DatabricksJobs]:
        return {
            job_id: job_data
            for job_id, job_data in self.valid_databricks_jobs.items()
            if self.deployment_state.contains_job(job_id, str(job_data.fabric_id)) is False
        }

    def _delete_jobs(self) -> Dict[str, JobInfo]:
        all_jobs = {**self.valid_databricks_jobs, **self._databricks_jobs_without_code}
        return {
            job.id: job
            for job in self.deployment_state.databricks_jobs
            if not any(job.id == job_id for job_id in all_jobs.keys())
        }

    def _update_databricks_json_for_artifactory(self, job_data):
        if self.project_config.artifactory:
            log(
                f"Artifactory URL {self.project_config.artifactory} is passed, "
                f"updating databricks-jobs.json with package"
            )
            job_data.databricks_job_json = modify_databricks_json_for_private_artifactory(
                job_data.databricks_job_json, self.project_config.artifactory
            )
            return job_data
        else:
            return job_data

    """Deploy Jobs """

    def _deploy_add_job(self, job_id, job_data, step_id):
        job_data = self._update_databricks_json_for_artifactory(job_data)
        fabric_id = job_data.fabric_id
        fabric_config = self.project_config.fabric_config.get_fabric(fabric_id)
        fabric_name = fabric_config.name if fabric_config is not None else None

        fabric_label = get_fabric_label(fabric_name, fabric_id)

        try:
            client = self.get_databricks_client(fabric_id)
            operation = OperationType.CREATED

            scheduler_job_id = None
            if not is_online_mode() and not self.project_config.based_on_file:
                scheduler_job_id = client.find_job(job_data.name)
                if scheduler_job_id:
                    client.reset_job(scheduler_job_id, job_data.databricks_json)
                    log(
                        f"{Colors.OKGREEN}Refreshed job {job_id} in fabric {fabric_label} databricks job-id:{scheduler_job_id}{Colors.ENDC}",
                        step_id=step_id,
                    )
                    operation = OperationType.REFRESH

            if not scheduler_job_id:
                response = client.create_job(job_data.databricks_json)
                log(
                    f"{Colors.OKGREEN}Created job {job_id} in fabric {fabric_label} response {response['job_id']}{Colors.ENDC}",
                    step_id=step_id,
                )
                scheduler_job_id = response["job_id"]

            job_info = JobInfo.create_job(
                job_data.name, job_id, fabric_id, scheduler_job_id, self.project.release_tag, job_data.is_paused
            )

            return Either(right=JobInfoAndOperation(job_info, operation))

        except Exception as e:
            log(
                f"{Colors.FAIL}Error creating job {job_id} in fabric {fabric_id}{Colors.ENDC}",
                exception=e,
                step_id=step_id,
            )
            return Either(left=e)

    def _deploy_add_jobs(self):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for job_id, job_data in self._add_jobs().items():
                fabric_id = job_data.fabric_id

                if fabric_id is not None:
                    futures.append(
                        executor.submit(
                            lambda j_id=job_id, j_data=job_data: self._deploy_add_job(
                                j_id, j_data, self._ADD_JOBS_STEP_ID
                            )
                        )
                    )
                else:
                    log(
                        f"{Colors.WARNING} Invalid fabric {fabric_id}, skipping job creation for job_id {job_id}{Colors.ENDC}",
                        step_id=self._ADD_JOBS_STEP_ID,
                    )

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Add])

    def _deploy_refresh_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, job_info in self._refresh_jobs().items():
                fabric_id = job_info.fabric_id

                if fabric_id is not None:
                    futures.append(
                        executor.submit(lambda j_id=job_id, j_info=job_info: self._reset_and_patch_job(j_id, j_info))
                    )
                else:
                    log(
                        f"${Colors.OKCYAN}Invalid fabric {fabric_id}, skipping job refresh for job_id {job_id}{Colors.ENDC}"
                    )

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Refresh])

    def _reset_and_patch_job(self, job_id, job_info):
        fabric_id = job_info.fabric_id
        fabric_config = self.fabric_configs.get_fabric(fabric_id)
        fabric_name = fabric_config.name if fabric_config is not None else None
        fabric_label = get_fabric_label(fabric_name, fabric_id)

        def log_error(message, exc, step_id=self._REFRESH_JOBS_STEP_ID):
            log(message, exception=exc, step_id=step_id)

        def log_success(message):
            log(message, step_id=self._REFRESH_JOBS_STEP_ID)

        try:
            client = self.get_databricks_client(fabric_id)
            job_data = self.valid_databricks_jobs.get(job_id)
            client.reset_job(job_info.external_job_id, job_data.databricks_json)

            if job_data.acl:
                log_success(
                    f"{Colors.OKGREEN}Refreshed job {job_id} with external job id {job_info.external_job_id}, patching job ACL{Colors.ENDC}"
                )
                try:
                    client.patch_job_acl(job_info.external_job_id, {"access_control_list": job_data.acl})
                    log_success(
                        f"{Colors.OKGREEN}Successfully patched job ACL for external job id {job_info.external_job_id} {Colors.ENDC}"
                    )
                except Exception as e:
                    log_error(
                        f"{Colors.FAIL}Error patching job acl for job {job_id} in fabric {fabric_label}{Colors.ENDC}", e
                    )
                    return Either(left=e)
            else:
                log_success(
                    f"{Colors.OKGREEN}Refreshed job {job_id} with external job id {job_info.external_job_id}{Colors.ENDC}"
                )

            return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))

        except HTTPError as e:
            response = e.response.content.decode("utf-8")

            if e.response.status_code == 400 and f"Job {job_info.external_job_id} does not exist." in response:
                log_success(
                    f"{Colors.OKGREEN}Job {job_id} external_job_id {job_info.external_job_id} deleted in fabric {fabric_label}, trying to recreate it{Colors.ENDC}"
                )
                return self._deploy_add_job(
                    job_id, self.valid_databricks_jobs.get(job_id), step_id=self._REFRESH_JOBS_STEP_ID
                )
            else:
                log_error(
                    f"{Colors.FAIL}Error while refreshing job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {fabric_label}{Colors.ENDC}",
                    e,
                )
                return Either(left=e)

        except Exception as e:
            log_error(
                f"{Colors.FAIL}Error while resetting job with {job_info.external_job_id} and job-id {job_id} for fabric {fabric_label}{Colors.ENDC}",
                e,
            )
            return Either(left=e)

    def _deploy_delete_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, job_info in self._delete_jobs().items():
                futures.append(executor.submit(lambda j_info=job_info: self._delete_job(j_info)))

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Remove])

    def _delete_job(self, job_info: JobInfo):
        fabric = job_info.fabric_id

        if fabric is None:
            msg = f"{Colors.WARNING}Invalid fabric {fabric} not deleting job {job_info.id} and databricks id {job_info.external_job_id}{Colors.ENDC}"
            log(msg, step_id=self._DELETE_JOBS_STEP_ID)
            return Either(left=InvalidFabricException(msg))

        def log_error(_msg, exc):
            log(_msg, exception=exc, step_id=self._DELETE_JOBS_STEP_ID)

        def log_success(message):
            log(message, step_id=self._DELETE_JOBS_STEP_ID)

        try:
            client = self.get_databricks_client(fabric)
            client.delete_job(job_info.external_job_id)
            log_success(
                f"{Colors.OKGREEN}Successfully deleted job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {fabric}{Colors.ENDC}"
            )
            return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))

        except HTTPError as e:
            response = e.response.content.decode("utf-8")

            if e.response.status_code == 400 and "does not exist." in response:
                log_success(
                    f"{Colors.OKGREEN}Job:{job_info.id} with external-id:{job_info.external_job_id}  already deleted, skipping{Colors.ENDC}"
                )
                return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))
            log_error(
                f"{Colors.WARNING}Error on deleting job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {fabric}{Colors.ENDC}",
                e,
            )

            return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))

        except Exception as e:
            log_error(
                f"{Colors.FAIL}Error while deleting job with scheduler id {job_info.external_job_id} and id {job_info.id} for fabric {fabric}{Colors.ENDC}",
                e,
            )
            return Either(left=e)

    def _deploy_pause_jobs(self):
        futures = []

        def pause_job(fabric_id, job_info):
            external_job_id = job_info.external_job_id
            fabric_name = get_fabric_label(self.fabric_configs.get_fabric(fabric_id).name, fabric_id)

            try:
                client = self.get_databricks_client(fabric_id)
                client.pause_job(external_job_id)
                log(
                    f"{Colors.OKGREEN}Paused job {external_job_id} in fabric {fabric_name}{Colors.ENDC}",
                    step_id=self._PAUSE_JOBS_STEP_ID,
                )
                return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))

            except Exception as e:
                log(
                    f"{Colors.WARNING}Error pausing job {external_job_id} in fabric {fabric_id}, Ignoring this {Colors.ENDC}",
                    exception=e,
                    step_id=self._PAUSE_JOBS_STEP_ID,
                )
                return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))

        with ThreadPoolExecutor(max_workers=3) as executor:
            for jobs_info in self._pause_jobs():
                fabric = jobs_info.fabric_id

                if fabric is not None:
                    futures.append(executor.submit(lambda f=fabric, j_info=jobs_info: pause_job(f, j_info)))
                else:
                    log(
                        f"{Colors.WARNING}Invalid fabric {fabric} for job_id {jobs_info.id} {Colors.ENDC}",
                        step_id=self._PAUSE_JOBS_STEP_ID,
                    )

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Pause])


class DBTComponents:
    _DBT_SECRETS_COMPONENT_STEP_NAME = "DBTSecretsComponents"
    _DBT_PROFILES_COMPONENT_STEP_NAME = "DBTProfileComponents"
    _DBT_CONTENT_COMPONENT_STEP_NAME = "DBTContentComponents"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment, project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.project_config = project_config
        self.jobs_state = project_config.jobs_state
        self.fabrics_config = project_config.fabric_config

    def summary(self):
        summary = []

        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:
                if components.get("sqlFabricId", None) is not None and components.get("secretKey", None) is not None:
                    summary.append(
                        f"Uploading dbt secrets for job {job_id} component {components.get('nodeName')} ",
                    )

                if (
                    components.get("profilePath", None) is not None
                    and components.get("profileContent", None) is not None
                ):
                    summary.append(f"Uploading dbt profiles for job {job_id} component {components.get('nodeName')} ")

                if components.get("path", None) is not None and components.get("content", None) is not None:
                    summary.append(f"Uploading dbt content for job {job_id} component {components.get('nodeName')} ")

        return summary

    def headers(self):
        return self._dbt_secrets_headers() + self._dbt_profiles_to_build_headers() + self._dbt_content_headers()

    def deploy(self):
        return self._upload_dbt_secrets() + self._upload_dbt_profiles() + self._upload_dbt_contents()

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
                job_id_to_dbt_components[job_id] = DbtComponentsModel(job_data.fabric_id, secret_scope, component_list)

        return job_id_to_dbt_components

    def _upload_dbt_profiles(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            if len(self._dbt_profiles_to_build_headers()) > 0:
                log(f"\n\n{Colors.OKBLUE}Uploading DBT profiles{Colors.ENDC}\n\n")

            for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
                for components in dbt_component_model.components:
                    if (
                        components.get("profilePath", None) is not None
                        and components.get("profileContent", None) is not None
                    ):
                        futures.append(
                            executor.submit(
                                lambda model=dbt_component_model, comp=components: self._upload_dbt_profile(model, comp)
                            )
                        )

        return await_futures_and_update_states(futures, self._DBT_PROFILES_COMPONENT_STEP_NAME)

    def _upload_dbt_profile(self, dbt_component_model: DbtComponentsModel, components: Dict[str, str]):
        try:
            client = self.databricks_jobs.get_databricks_client(dbt_component_model.fabric_id)
            client.upload_content(components["profileContent"], components["profilePath"])

            log(
                f"{Colors.OKGREEN}Successfully uploaded dbt profile {components['profilePath']}{Colors.ENDC}",
                step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME,
            )
            return Either(right=True)
        except Exception as e:
            log(
                f"{Colors.FAIL}Error while uploading dbt profile {components['profilePath']}{Colors.ENDC}",
                exception=e,
                step_id=self._DBT_PROFILES_COMPONENT_STEP_NAME,
            )
            return Either(left=e)

    def _upload_dbt_secrets(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            if len(self._dbt_secrets_headers()) > 0:
                log(f"\n\n{Colors.OKBLUE}Uploading DBT secrets{Colors.ENDC}\n\n")

            for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
                for dbt_component in dbt_component_model.components:
                    if (
                        dbt_component.get("sqlFabricId", None) is not None
                        and dbt_component.get("secretKey", None) is not None
                    ):
                        futures.append(
                            executor.submit(
                                lambda model=dbt_component_model, component=dbt_component: self._upload_dbt_secret(
                                    model, component
                                )
                            )
                        )

        return await_futures_and_update_states(futures, self._DBT_SECRETS_COMPONENT_STEP_NAME)

    def _upload_dbt_secret(self, dbt_component_model, dbt_component):
        try:
            self.databricks_jobs.get_databricks_client(
                str(dbt_component_model.fabric_id)
            ).create_secret_scope_if_not_exist(dbt_component_model.secret_scope)

            sql_client = self.databricks_jobs.get_databricks_client(dbt_component["sqlFabricId"])
            git_token = self.fabrics_config.git_token_for_project(dbt_component["projectId"])
            if sql_client.auth_type is not None and sql_client.auth_type == "oauth":
                master_token = f"{sql_client.oauth_client_secret};{git_token}"
            else:
                master_token = f"{sql_client.token};{git_token}"

            sql_client.create_secret(dbt_component_model.secret_scope, dbt_component["secretKey"], master_token)

            log(
                f"{Colors.OKGREEN}Successfully uploaded dbt secret for component {dbt_component['nodeName']} and for scope {dbt_component_model.secret_scope} {Colors.ENDC}",
                step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME,
            )

            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.FAIL}Error on uploading dbt secret for component {dbt_component['nodeName']} {Colors.ENDC}",
                exception=e,
                step_id=self._DBT_SECRETS_COMPONENT_STEP_NAME,
            )

            return Either(left=e)

    def _upload_dbt_contents(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            if len(self._dbt_content_headers()) > 0:
                log(f"\n\n{Colors.OKBLUE}Uploading DBT content{Colors.ENDC}\n\n")

            for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
                for dbt_component in dbt_component_model.components:
                    if dbt_component.get("path", None) is not None and dbt_component.get("content", None) is not None:
                        futures.append(
                            executor.submit(
                                lambda comp=dbt_component, fabric_id=dbt_component_model.fabric_id: self._upload_dbt_content(
                                    comp, fabric_id
                                )
                            )
                        )

        return await_futures_and_update_states(futures, self._DBT_CONTENT_COMPONENT_STEP_NAME)

    def _dbt_content_headers(self):
        total_dbt_content = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:
                if components.get("path", None) is not None and components.get("content", None) is not None:
                    total_dbt_content = total_dbt_content + 1

        if total_dbt_content > 0:
            return [
                StepMetadata(
                    self._DBT_CONTENT_COMPONENT_STEP_NAME,
                    f"Upload {total_dbt_content} dbt content",
                    Operation.Upload,
                    StepType.DbtContent,
                )
            ]
        else:
            return []

    def _upload_dbt_content(self, component, fabric_id):
        path = component["path"]
        content = component["content"]

        try:
            client = self.databricks_jobs.get_databricks_client(fabric_id)
            client.upload_content(content, path)
            log(
                f"{Colors.OKGREEN}Successfully uploaded dbt content {path}{Colors.ENDC}",
                step_id=self._DBT_CONTENT_COMPONENT_STEP_NAME,
            )
            return Either(right=True)
        except Exception as e:
            log(
                f"{Colors.FAIL}Error while uploading dbt content {path}{Colors.ENDC}",
                exception=e,
                step_id=self._DBT_CONTENT_COMPONENT_STEP_NAME,
            )
            return Either(left=e)

    def _dbt_profiles_to_build_headers(self):
        total_dbt_profiles = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:
                if (
                    components.get("profilePath", None) is not None
                    and components.get("profileContent", None) is not None
                ):
                    total_dbt_profiles = total_dbt_profiles + 1

        if total_dbt_profiles > 0:
            return [
                StepMetadata(
                    self._DBT_PROFILES_COMPONENT_STEP_NAME,
                    f"Upload {total_dbt_profiles} dbt profiles",
                    Operation.Upload,
                    StepType.DbtProfile,
                )
            ]
        else:
            return []

    def _dbt_secrets_headers(self):
        total_dbt_secrets = 0
        for job_id, dbt_component_model in self.dbt_component_from_jobs.items():
            for components in dbt_component_model.components:
                if components.get("sqlFabricId", None) is not None and components.get("secretKey", None) is not None:
                    total_dbt_secrets = total_dbt_secrets + 1

        if total_dbt_secrets > 0:
            return [
                StepMetadata(
                    self._DBT_SECRETS_COMPONENT_STEP_NAME,
                    f"Upload {total_dbt_secrets} dbt secrets",
                    Operation.Upload,
                    StepType.DbtSecret,
                )
            ]
        else:
            return []


class ScriptComponents:
    _STEP_ID = "script_components"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment, project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.project = project
        self.script_jobs = self._script_components_from_jobs()
        self.fabric_config = project_config.fabric_config
        self.deployment_state = project_config.jobs_state

    def summary(self) -> List[str]:
        script_summary = []

        for job_id, script_job_list in self.script_jobs.items():
            for script_job in script_job_list.scripts:
                script_summary.append(f"Uploading script {script_job['nodeName']} for job {job_id}")

        return script_summary

    def headers(self) -> List[StepMetadata]:
        all_scripts = sum(len(v.scripts) for v in self.script_jobs.values())

        if all_scripts > 0:
            return [
                StepMetadata(
                    self._STEP_ID, f"Upload {all_scripts} script components", Operation.Upload, StepType.Script
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        if len(self.headers()) > 0:
            log(f"\n\n{Colors.OKBLUE}Uploading script components from job{Colors.ENDC}\n\n")

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, script_components in self._script_components_from_jobs().items():
                for scripts in script_components.scripts:
                    futures.append(
                        executor.submit(
                            lambda s=scripts, fabric_id=script_components.fabric_id, j_id=job_id: self._upload_content(
                                s, fabric_id, j_id
                            )
                        )
                    )

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _upload_content(self, script: dict, fabric_id: str, job_id: str):
        node_name = script.get("nodeName")
        fabric_config = self.fabric_config.get_fabric(fabric_id)

        fabric_name = fabric_config.name if fabric_config is not None else None
        fabric_label = get_fabric_label(fabric_name, fabric_id)

        try:
            client = self.databricks_jobs.get_databricks_client(str(fabric_id))
            client.upload_content(content=script.get("content", None), path=script.get("path", None))

            log(
                f"{Colors.OKGREEN}Uploaded script component `{node_name}` to fabric `{fabric_label}` for job `{job_id}` to path `{script.get('path', None)}` {Colors.ENDC}",
                step_id=self._STEP_ID,
            )
            return Either(right=True)
        except Exception as e:
            log(
                f"{Colors.FAIL}Error uploading script component `{node_name}` to fabric `{fabric_label}` {Colors.ENDC}",
                step_id=self._STEP_ID,
                exception=e,
            )
            return Either(left=e)

    def _script_components_from_jobs(self) -> Dict[str, ScriptComponentsModel]:
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

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment, project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project_config = project_config
        self.project = project

    def summary(self) -> List[str]:
        summary = []
        for ids in self.pipeline_configurations.keys():
            summary.append(f"Uploading pipeline configurations for pipeline {ids}")

        return summary

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0:
            return [
                StepMetadata(
                    self._STEP_ID,
                    f"Upload {len(all_configs)} pipeline configurations",
                    Operation.Upload,
                    StepType.PipelineConfiguration,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            if len(self.pipeline_configurations.items()) > 0:
                count = sum(len(v) for v in self.pipeline_configurations.values())
                log(f"\n\n{Colors.OKBLUE} Uploading {count} pipeline configurations {Colors.ENDC}\n\n")

            def execute_job(_fabric_id, pipeline_id, config_name, config_content):
                futures.append(
                    executor.submit(
                        lambda f_id=str(
                            _fabric_id
                        ), p_id=pipeline_id, conf_name=config_name, conf_content=config_content: self._upload_configuration(
                            f_id, p_id, conf_name, conf_content
                        )
                    )
                )

            for pipeline_id, configurations in self.pipeline_configurations.items():
                for configuration_name, configuration_content in configurations.items():
                    for fabric_id in self.project_config.fabric_config.db_fabrics():
                        execute_job(fabric_id, pipeline_id, configuration_name, configuration_content)

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _base_path(self, fab_id: Optional[str], pipeline_id):
        if fab_id is None or self.project_config.is_volume_supported(fab_id):
            _base_path = self.project_config.get_db_base_path(fab_id)
        elif fab_id in self.project.fabric_volumes_detected.keys():
            _base_path = self.project.fabric_volumes_detected[fab_id]
        else:
            raise NotImplementedError("volume must either be defined in jobs or in project config (fabrics.yml)")

        # Strip trailing slash to avoid double slashes in path construction
        _base_path = _base_path.rstrip("/")
        pipeline_path = (
            f"{_base_path}/{self.project.project_id}/{self.project.release_version}/configurations/{pipeline_id}"
        )
        return pipeline_path

    def _upload_configuration(self, fabric_id, pipeline_id, config_name, configuration_content):

        try:
            # we are creating path and then uploading the content
            # we need to upload for the scenerios
            # in case volumes is set or not.
            # after some versions most likely databricks will deprecate the dbfs one.
            client = self.databricks_jobs.get_databricks_client(str(fabric_id))
            base_p = self._base_path(None, pipeline_id)  # old dbfs path first.
            # Ensure no double slashes in path construction
            base_p = base_p.rstrip("/")
            configuration_path = f"{base_p}/{config_name}.json"
            client.upload_content(configuration_content, configuration_path)
            log(
                f"{Colors.OKGREEN}Uploaded pipeline configuration on path {configuration_path}{Colors.ENDC}",
                step_id=self._STEP_ID,
            )

            if (
                self.project_config.is_volume_supported(fabric_id)
                or fabric_id in self.project.fabric_volumes_detected.keys()
            ):
                volume_base = self._base_path(fabric_id, pipeline_id).rstrip("/")
                config_path_volume = f"{volume_base}/{config_name}.json"
                client.upload_content(configuration_content, config_path_volume)
                log(
                    f"{Colors.OKGREEN}Uploaded pipeline configuration on path {config_path_volume} with volume support{Colors.ENDC}",
                    step_id=self._STEP_ID,
                )
            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.FAIL}Failed to upload pipeline configuration for path {config_name}{Colors.ENDC}",
                exception=e,
                step_id=self._STEP_ID,
            )
            return Either(left=e)


class ProjectConfigurations:
    _STEP_ID = "project_configurations"

    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment, project_config: ProjectConfig):
        self.databricks_jobs = databricks_jobs
        self.subscribed_project_configurations = project.subscribed_project_configurations
        self.project_configurations = project.project_configurations
        self.project_config = project_config
        self.project = project
        # Load dependency project configurations
        self.dependency_project_configurations = self._load_dependency_project_configurations()

    def _load_dependency_project_configurations(self):
        """Load configurations from dependency projects referenced in jobs"""
        dependency_configs = {}
        # Get all dependency project IDs from jobs
        dependency_project_ids = set()

        for job_id in self.project.jobs.keys():
            # Try to load job folder content
            rdc = self.project.load_airflow_folder_with_placeholder(job_id)
            if rdc and "prophecy-job.json" in rdc:
                prophecy_json = json.loads(rdc["prophecy-job.json"])
                # Use shared utility to extract dependency project IDs
                found_ids = extract_dependency_project_ids(prophecy_json)
                dependency_project_ids.update(found_ids)

        # Load configurations from each dependency project
        for project_id in dependency_project_ids:
            dependency_path = os.path.join(self.project.project_path, ".prophecy", project_id)
            config_path = os.path.join(dependency_path, "projectConfigurations/config/code/configs")

            if os.path.exists(config_path):
                log(f"Config path exists for dependency project {project_id}: {config_path}", step_id=self._STEP_ID)

                configurations = {}
                for file_name in os.listdir(config_path):
                    if file_name.endswith(".json"):
                        file_path = os.path.join(config_path, file_name)
                        with open(file_path, "r") as file:
                            content = file.read()
                            config_name = file_name[:-5]  # Remove .json extension
                            configurations[config_name] = content

                if configurations:
                    dependency_configs[project_id] = configurations
                    # Log the configurations that will be uploaded to Summary
                    for config_name in configurations.keys():
                        log(f"Uploading dependency project {project_id} configuration {config_name}", "Summary")

        return dependency_configs

    def summary(self) -> List[str]:
        summary = []
        for config_name in self.subscribed_project_configurations.keys():
            summary.append(f"Uploading subscribed project configuration {config_name}")

        for config_name in self.project_configurations.keys():
            summary.append(f"Uploading project configuration {config_name}")

        for project_id, configs in self.dependency_project_configurations.items():
            for config_name in configs.keys():
                summary.append(f"Uploading dependency project {project_id} configuration {config_name}")

        return summary

    def headers(self) -> List[StepMetadata]:
        _total_configs = len(self.subscribed_project_configurations) + len(self.project_configurations)
        # Add dependency project configurations to the total
        for configs in self.dependency_project_configurations.values():
            _total_configs += len(configs)

        if _total_configs:
            return [
                StepMetadata(
                    self._STEP_ID,
                    f"Upload {_total_configs} project configurations",
                    Operation.Upload,
                    StepType.PipelineConfiguration,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            if len(self.subscribed_project_configurations.items()) > 0:
                count = len(self.subscribed_project_configurations)
                log(f"\n\n{Colors.OKBLUE} Uploading {count} subscribed project configurations {Colors.ENDC}\n\n")

            if len(self.project_configurations.items()) > 0:
                count = len(self.project_configurations)
                log(f"\n\n{Colors.OKBLUE} Uploading {count} project configurations {Colors.ENDC}\n\n")

            # Log dependency project configurations
            total_dep_configs = sum(len(configs) for configs in self.dependency_project_configurations.values())
            if total_dep_configs > 0:
                log(
                    f"\n\n{Colors.OKBLUE} Uploading {total_dep_configs} dependency project configurations {Colors.ENDC}\n\n"
                )

            def execute_job(_fabric_id, config_name, config_content, project_id=None):
                futures.append(
                    executor.submit(
                        lambda f_id=str(
                            _fabric_id
                        ), conf_name=config_name, conf_content=config_content, proj_id=project_id: self._upload_configuration(
                            f_id, conf_name, conf_content, proj_id
                        )
                    )
                )

            db_fabrics = list(self.project_config.fabric_config.db_fabrics())

            for configuration_name, configuration_content in self.subscribed_project_configurations.items():
                for fabric_id in db_fabrics:
                    execute_job(fabric_id, configuration_name, configuration_content)

            for configuration_name, configuration_content in self.project_configurations.items():
                for fabric_id in db_fabrics:
                    execute_job(fabric_id, configuration_name, configuration_content)

            # Upload dependency project configurations
            for project_id, configurations in self.dependency_project_configurations.items():
                for configuration_name, configuration_content in configurations.items():
                    for fabric_id in db_fabrics:
                        execute_job(fabric_id, configuration_name, configuration_content, project_id)

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _base_path(self, fab_id: Optional[str], project_id: Optional[str] = None):
        if fab_id is None or self.project_config.is_volume_supported(fab_id):
            _base_path = self.project_config.get_db_base_path(fab_id)
        elif fab_id in self.project.fabric_volumes_detected.keys():
            _base_path = self.project.fabric_volumes_detected[fab_id]
        else:
            raise NotImplementedError("volume must either be defined in jobs or in project config (fabrics.yml)")

        # Strip trailing slash to avoid double slashes in path construction
        _base_path = _base_path.rstrip("/")
        # Use dependency project ID if provided, otherwise use main project ID
        actual_project_id = project_id if project_id else self.project.project_id
        project_path = f"{_base_path}/{actual_project_id}/{self.project.release_version}/configurations"
        return project_path

    def _upload_configuration(self, fabric_id, config_name, configuration_content, project_id=None):

        try:
            # we are creating path and then uploading the content
            # we need to upload for the scenerios
            # in case volumes is set or not.
            # after some versions most likely databricks will deprecate the dbfs one.
            client = self.databricks_jobs.get_databricks_client(str(fabric_id))
            base_p = self._base_path(None, project_id)  # old dbfs path first.
            # Ensure no double slashes in path construction
            base_p = base_p.rstrip("/")
            configuration_path = f"{base_p}/{config_name}.json"

            client.upload_content(configuration_content, configuration_path)
            # Include project ID in log message if it's a dependency project
            project_info = f" (dependency project {project_id})" if project_id else ""
            log(
                f"{Colors.OKGREEN}Uploaded project configuration{project_info} on path {configuration_path}{Colors.ENDC}",
                step_id=self._STEP_ID,
            )

            is_volume_supported = self.project_config.is_volume_supported(fabric_id)
            has_fabric_volume = fabric_id in self.project.fabric_volumes_detected.keys()

            if is_volume_supported or has_fabric_volume:
                volume_base = self._base_path(fabric_id, project_id).rstrip("/")
                config_path_volume = f"{volume_base}/{config_name}.json"

                client.upload_content(configuration_content, config_path_volume)
                log(
                    f"{Colors.OKGREEN}Uploaded project configuration{project_info} on path {config_path_volume} with volume support{Colors.ENDC}",
                    step_id=self._STEP_ID,
                )
            return Either(right=True)

        except Exception as e:
            project_info = f" (dependency project {project_id})" if project_id else ""
            log(
                f"{Colors.WARNING}Failed to upload project configuration{project_info} for path {config_name}{Colors.ENDC}",
                exception=e,
                step_id=self._STEP_ID,
            )
            return Either(right=True)
