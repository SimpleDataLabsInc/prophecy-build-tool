import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, List

import yaml

from . import JobInfoAndOperation, OperationType
from ..client.airflow.airflow_utility import create_airflow_client, get_fabric_type
from ..entities.project import Project
from ..constants import FABRIC_UID
from ..project_models import StepMetadata, Operation, StepType, Status
from ..project_config import JobInfo, ProjectConfig
from ..utility import Either, generate_secure_content, calculate_checksum
from ..utility import custom_print as log


def does_dag_file_exist(rdc: Dict[str, str]) -> bool:
    return rdc is not None and 'dag.py' in rdc


def filter_job_files(rdc: Dict[str, str]):
    filtered_files = {file_name: file_content for file_name, file_content in rdc.items()
                      if
                      file_name == 'dag.py' or '__init__.py' or file_name == 'prophecy-job.json' or
                      'tasks/' in file_name}

    return dict(sorted(filtered_files.items()))


def sanitize_job(job_id) -> str:
    return ('_' if job_id is None else job_id).split('/')[-1].replace('\\W', '_')


def get_zipped_dag_name(dag_name: str):
    return f"/tmp/{dag_name}.zip"


def zip_folder(folder_path, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                abs_file_path = os.path.join(root, file)
                zipf.write(abs_file_path, abs_file_path[len(folder_path):])


class AirflowJob:

    def __init__(self, job_pbt: dict, prophecy_job_yaml: str, rdc: Dict[str, str], sha: Optional[str]):
        self.job_pbt = job_pbt
        self.prophecy_job_yaml = prophecy_job_yaml
        self.rdc = rdc
        self.sha = sha

        self.prophecy_job_json_dict = self._initialize_prophecy_job_json()

    @property
    def name(self):
        return self.job_pbt.get('name', None)

    @property
    def is_valid_job(self):
        prophecy_job_yaml_dict = self.prophecy_job_json_dict
        return self.job_pbt is not None and self.prophecy_job_yaml is not None and self.rdc is not None and \
            prophecy_job_yaml_dict.get('metainfo', {}).get('fabricId', None) is not None

    def validate_prophecy_managed_checksum(self, salt: str):
        file_joiner: str = "$$$"
        checksum = calculate_checksum(
            file_joiner.join(file_content for file_content in filter_job_files(self.rdc).values()), salt)
        return self.sha == checksum

    @property
    def job_files(self):
        return filter_job_files(self.rdc)

    @property
    def dag_name(self):
        return self.prophecy_job_json_dict.get('metainfo', {}).get('dagName', None)

    @property
    def is_disabled(self):
        return self.prophecy_job_json_dict.get('enabled', False) is False

    @property
    def is_enabled(self):
        return self.prophecy_job_json_dict.get('enabled', False) is True

    @property
    def fabric_id(self):
        fabric_id = self.job_pbt.get(FABRIC_UID)
        return str(fabric_id) if fabric_id is not None else None

    @property
    def pipelines(self):
        return self.job_pbt.get('pipelines', [])

    @property
    def has_dbt_component(self):
        return any(value.get('component', None) == 'dbt' for value in self.prophecy_job_json_dict['processes'].values())

    def _initialize_prophecy_job_json(self) -> dict:
        try:
            return yaml.unsafe_load(self.prophecy_job_yaml)
        except Exception as e:
            log("Error while loading prophecy job yaml", e)
            return {}


class AirflowJobDeployment:
    _REMOVE_JOBS_STEP_ID = "remove-airflow-jobs"
    _ADD_JOBS_STEP_ID = "add-airflow-jobs"
    _PAUSE_JOBS_STEP_ID = "pause-airflow-jobs"
    _DELETE_JOBS_STEP_ID = "delete-airflow-jobs"
    _SKIP_JOBS_STEP_ID = "skip-airflow-jobs"

    def __init__(self, project: Project, project_config: ProjectConfig):
        self._project = project
        self._project_config = project_config

        self._airflow_clients = {}

        self._airflow_jobs: Dict[str, AirflowJob] = self._initialize_airflow_jobs()

        (self.valid_airflow_jobs, self._invalid_airflow_jobs,
         self._airflow_jobs_without_code) = self._initialize_valid_airflow_jobs()

        self.prophecy_managed_dbt_jobs: Dict[str, AirflowJob] = self._initialize_prophecy_managed_dbt_jobs()

    def get_airflow_client(self, fabric_id):
        if fabric_id not in self._airflow_clients:
            self._airflow_clients[fabric_id] = create_airflow_client(str(fabric_id), self._project_config)
        return self._airflow_clients[fabric_id]

    def summary(self):
        summary = []

        for job in self._remove_jobs():
            summary.append(f"Remove job: {job.id}")
        for job in self._add_jobs().keys():
            summary.append(f"Add job: {job}")
        for job in self._pause_jobs().keys():
            summary.append(f"Pause job: {job}")
        for job in self._rename_jobs():
            summary.append(f"Rename job: {job.id}")
        for job in self._skip_jobs().keys():
            summary.append(f"Skip job: {job}")

        return summary

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Remove: self._DELETE_JOBS_STEP_ID,
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID,
            Operation.Rename: self._ADD_JOBS_STEP_ID,
            Operation.Skipped: self._SKIP_JOBS_STEP_ID
        }

    def headers(self):
        job_types = {
            Operation.Remove: self._remove_jobs(),
            Operation.Add: self._add_jobs(),
            Operation.Pause: self._pause_jobs(),
            Operation.Rename: self._rename_jobs(),
            Operation.Skipped: self._skip_jobs()
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f'{len_jobs} Airflow job' if len_jobs == 1 else f'{len_jobs} Airflow jobs'
                step_id = self._operation_to_step_id[job_action]
                all_headers.append(
                    StepMetadata(step_id, f"{job_action.value} {job_header_suffix}",
                                 job_action, StepType.Job)
                )

        return all_headers

    def deploy(self):
        responses = self._deploy_remove_jobs() + \
                    self._deploy_add_jobs() + \
                    self._deploy_pause_jobs() + \
                    self._deploy_delete_jobs()

        self._deploy_skipped_jobs()

        return responses

    def _initialize_airflow_jobs(self):
        jobs = {}

        for job_id, parsed_job in self._project.jobs.items():
            if 'Databricks' not in parsed_job.get('scheduler', None):

                rdc = self._project.load_airflow_folder(job_id)
                aspects = self._project.load_airflow_aspect(job_id)

                prophecy_job_json = None
                sha = None

                if rdc is not None and 'prophecy-job.json' in rdc:
                    prophecy_job_json = rdc.get('prophecy-job.json', None)
                    rdc.pop('prophecy-job.json', None)

                if aspects is not None:
                    try:
                        sha = yaml.safe_load(aspects).get('sha', None)
                    except Exception as e:
                        log("Error while loading prophecy job yaml", e)

                jobs[job_id] = AirflowJob(parsed_job, prophecy_job_json, rdc, sha)

        return jobs

    def _initialize_valid_airflow_jobs(self):
        valid_airflow_jobs = {}
        invalid_airflow_jobs = {}
        airflow_jobs_without_code = {}

        for job_id, job_data in self._airflow_jobs.items():
            if job_data.is_valid_job:
                response = self._validate_airflow_job(job_id, job_data)
                if response.is_right:
                    valid_airflow_jobs[job_id] = job_data

                else:
                    invalid_airflow_jobs[job_id] = job_data

            else:
                airflow_jobs_without_code[job_id] = job_data

        return valid_airflow_jobs, invalid_airflow_jobs, airflow_jobs_without_code

    def _validate_airflow_job(self, job_id: str, job_data: AirflowJob):

        is_prophecy_managed_fabric = self._project_config.state_config.is_fabric_prophecy_managed(job_data.fabric_id)
        rdc = job_data.rdc

        if does_dag_file_exist(rdc) is False:
            return Either(
                left=Exception(f"Please open the Job `{job_id}` in editor, check diagnostic errors and release again."))

        elif is_prophecy_managed_fabric:
            if job_data.validate_prophecy_managed_checksum(self._project_config.system_config.prophecy_salt):
                return Either(right=job_data.job_files)

            else:
                return Either(left=Exception(
                    f"Job `{job_id}` has been externally edited. Please open the Job in editor and release again."))
        else:
            return Either(right=rdc)

    # we won't be able to completely validated the prophecy_job_json structure to prophecy_job.json.
    def _initialize_prophecy_managed_dbt_jobs(self):
        prophecy_managed_dbt_jobs = {}

        for job_id, job_data in self.valid_airflow_jobs.items():

            is_job_enabled = job_data.is_enabled
            is_prophecy_managed_fabric = self._project_config.state_config.is_fabric_prophecy_managed(
                job_data.fabric_id)

            if is_job_enabled and is_prophecy_managed_fabric and job_data.has_dbt_component:
                prophecy_managed_dbt_jobs[job_id] = job_data

        return prophecy_managed_dbt_jobs

    def _remove_jobs(self):
        return self._jobs_to_be_deleted() + self._jobs_with_fabric_changed()

    def _deploy_remove_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_info in self._remove_jobs():
                futures.append(executor.submit(self._remove_job, job_info))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        self._update_state(responses, self._operation_to_step_id[Operation.Remove])
        return responses

    def _remove_job(self, job_info: JobInfo):
        client = self.get_airflow_client(job_info.fabric_id)

        try:
            client.delete_dag_file(job_info.external_job_id)
            log(f"Successfully deleted job {job_info.external_job_id} from job_id {job_info.id}",
                step_id=self._REMOVE_JOBS_STEP_ID)
            return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))

        except Exception as e:
            log(f"Error while deleting job {job_info.external_job_id} from job_id {job_info.id}", e,
                step_id=self._REMOVE_JOBS_STEP_ID)
            return Either(left=e)

    '''
        Compare what exists in state config to what exists in code.
     '''

    def _jobs_to_be_deleted(self) -> List[JobInfo]:

        return [
            airflow_job for airflow_job in self._project_config.state_config.get_airflow_jobs
            if not any(
                airflow_job.id == job_id
                for job_id in list(self.valid_airflow_jobs.keys())  # check from available valid airflow jobs.
            )
        ]

    '''
        There exist a job in folder whose fabric id is different from the one in state config.
        folder can only have one job with a given job id.
    '''

    def _jobs_with_fabric_changed(self) -> List[JobInfo]:

        return [
            airflow_job for airflow_job in self._project_config.state_config.get_airflow_jobs
            if any(
                airflow_job.id == job_id and airflow_job.fabric_id != job_data.fabric_id
                for job_id, job_data in self.valid_airflow_jobs.items()  # Check only from valid airflow jobs.
            )
        ]

    def _rename_jobs(self) -> List[JobInfo]:
        return [
            airflow_job for airflow_job in self._project_config.state_config.get_airflow_jobs

            if any(
                airflow_job.id == job_id and airflow_job.fabric_id == job_data.fabric_id and
                job_data.is_enabled and airflow_job.external_job_id != job_data.dag_name
                for job_id, job_data in self.valid_airflow_jobs.items()
            )
        ]

    def _airflow_jobs_to_be_added(self) -> List[JobInfo]:
        return self._jobs_to_be_deleted() + self._jobs_with_fabric_changed()

    def _all_removed_airflow_jobs(self) -> List[JobInfo]:
        return self._rename_jobs() + self._jobs_with_fabric_changed() + self._jobs_to_be_deleted()

    def _pause_jobs(self) -> Dict[str, AirflowJob]:
        new_disabled_jobs = {job_id: job_data for job_id, job_data in self.valid_airflow_jobs.items() if
                             job_data.is_disabled}

        disabled_jobs_not_in_removed_jobs = {
            job_id: job_data
            for job_id, job_data in new_disabled_jobs.items()
            if all(
                airflow_job.id == job_id
                for airflow_job in self._all_removed_airflow_jobs()
            ) is False
        }

        disabled_jobs_where_old_was_enabled = {
            job_id: job_data
            for job_id, job_data in disabled_jobs_not_in_removed_jobs.items()
            if any(
                airflow_job.id == job_id and airflow_job.is_paused is False
                for airflow_job in self._project_config.state_config.get_airflow_jobs
            )
        }

        return disabled_jobs_where_old_was_enabled

    def _add_jobs(self):
        return {
            job_id: job_data
            for job_id, job_data in self.valid_airflow_jobs.items()
            if self._validate_airflow_job(job_id, job_data).is_right
        }

    def _deploy_add_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self._add_jobs().items():
                futures.append(executor.submit(self._add_job, job_id, job_data))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        self._update_state(responses, self._operation_to_step_id[Operation.Add])
        return responses

    def _add_job(self, job_id, job_data):
        dag_name = job_data.dag_name
        zipped_dag_name = get_zipped_dag_name(dag_name)
        zip_folder(self._project.load_airflow_base_folder_path(job_id), zipped_dag_name)

        client = self.get_airflow_client(fabric_id=job_data.fabric_id)
        try:
            client.upload_dag(dag_name, zipped_dag_name)
            client.unpause_dag(dag_name)
            log(f"Successfully added job {dag_name} for job_id {job_id}", step_id=self._ADD_JOBS_STEP_ID)

            job_info = JobInfo.create_airflow_job(job_data.name, job_id, job_data.fabric_id, job_data.dag_name,
                                                  self._project_config.state_config.release_tag, job_data.is_disabled,
                                                  get_fabric_type(job_data.fabric_id, self._project_config))

            return Either(right=JobInfoAndOperation(job_info, OperationType.CREATED))
        except Exception as e:
            log(f"Failed to upload_dag for job_id: {job_id} with dag_name {dag_name}", e,
                step_id=self._ADD_JOBS_STEP_ID)

    def _deploy_skipped_jobs(self):
        for job_id, messages in self._skip_jobs().items():
            log(f"Skipping job_id: {job_id} encountered some error ", exceptions=messages,
                step_id=self._SKIP_JOBS_STEP_ID)
        if len(self._skip_jobs()) > 0:
            self._update_state([Either(right=True)], self._operation_to_step_id[Operation.Skipped])

    def _skip_jobs(self):
        jobs_to_be_skipped = {}
        for job_id, job_data in self._airflow_jobs.items():
            job_validation = self._validate_airflow_job(job_id, job_data)

            if job_validation.is_right:
                continue

            jobs_to_be_skipped[job_id] = job_validation

        return jobs_to_be_skipped

    def _deploy_delete_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for jobs_info in self._rename_jobs():
                futures.append(executor.submit(self._delete_job, jobs_info))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())
        self._update_state(responses, self._operation_to_step_id[Operation.Rename])
        return responses

    def _delete_job(self, jobs_info: JobInfo):
        try:
            client = create_airflow_client(jobs_info.fabric_id, self._project_config)
            client.delete_dag(sanitize_job(jobs_info.external_job_id))
            log(f"Successfully deleted dag for job_id: {jobs_info.id}", step_id=self._DELETE_JOBS_STEP_ID)
            return Either(right=JobInfoAndOperation(jobs_info, OperationType.DELETED))
        except Exception as e:
            log(f"Failed to delete dag for job_id: {jobs_info.id}", e, step_id=self._DELETE_JOBS_STEP_ID)
            return Either(left=e)

    def _deploy_pause_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self._pause_jobs().items():
                futures.append(executor.submit(self._pause_job, job_id, job_data))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())
        self._update_state(responses, self._operation_to_step_id[Operation.Pause])
        return responses

    def _pause_job(self, job_id: str, job_data: AirflowJob):
        client = self.get_airflow_client(job_data.fabric_id)

        if len(job_data.dag_name) > 0:
            dag_name = job_data.dag_name
        else:
            dag_name = sanitize_job(job_id)

        try:
            client.pause_dag(dag_name)
            log(f"Successfully paused job {dag_name} for job_id {job_id}", step_id=self._PAUSE_JOBS_STEP_ID)
            job_info = self._project_config.state_config.get_job(job_id, job_data.fabric_id)
            return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))
        except Exception as e:
            log(f"Failed to pause_dag for job_id: {job_id} with dag_name {dag_name}", e,
                step_id=self._PAUSE_JOBS_STEP_ID)
            return Either(left=e)

    def _update_state(self, responses: List, step_id: str):
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=step_id)
            else:
                log(step_status=Status.SUCCEEDED, step_id=step_id)


class AirflowGitSecrets:
    _AIRFLOW_GIT_SECRETS_STEP_ID = "airflow-git-secrets"

    def __init__(self, project: Project, airflow_jobs: AirflowJobDeployment,
                 state_config_and_db_tokens: ProjectConfig):
        self.project = project
        self.airflow_jobs = airflow_jobs
        self.state_config_and_db_tokens = state_config_and_db_tokens
        self.state_config = state_config_and_db_tokens.state_config

    def summary(self) -> List[str]:
        summary = []

        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:

            for project_git_tokens in self.state_config.project_git_tokens:
                git_tokens = project_git_tokens.git_token
                project_id = project_git_tokens.project_id

                if len(git_tokens) > 0:
                    summary.append(f"Creating git secrets for project {project_id} ")

        return summary

    def headers(self) -> List[StepMetadata]:
        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:
            return [
                StepMetadata(self._AIRFLOW_GIT_SECRETS_STEP_ID, "Create git secrets for airflow jobs", Operation.Build,
                             StepType.AirflowGitSecrets)]
        else:
            return []

    def deploy(self):

        futures = []

        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:

            job_data = list(self.airflow_jobs.prophecy_managed_dbt_jobs.values())[0]

            with ThreadPoolExecutor(max_workers=10) as executor:
                for project_git_tokens in self.state_config.project_git_tokens:
                    git_tokens = project_git_tokens.git_token
                    project_id = project_git_tokens.project_id

                    if len(git_tokens) == 0:
                        log(f"No git tokens found for project_id: {project_id}, Ignoring",
                            step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID)

                    else:
                        futures.append(
                            executor.submit(lambda: self._create_git_secrets(project_id, job_data, git_tokens)))

            responses = []
            for future in as_completed(futures):
                responses.append(future.result())

            self._update_state(responses, self._AIRFLOW_GIT_SECRETS_STEP_ID)
            return responses
        else:
            log("No dbt jobs found, Ignoring", step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID)
            return []

    def _create_git_secrets(self, project_id, job_data, git_tokens):
        execution_db_suffix = os.getenv('EXECUTION_DB_SUFFIX', 'dev')
        client = self.airflow_jobs.get_airflow_client(job_data.fabric_id)
        try:
            client.create_secret(
                generate_secure_content(f'{execution_db_suffix}_{project_id}', 'gitSecretSalt'), git_tokens)
            log(f'Successfully created git secrets for project {project_id}',
                step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID)

        except Exception as e:
            log(f'Failed in creating git secrets for project {project_id}', e,
                step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID)

    def _update_state(self, responses: List, step_id: str):
        if responses is not None and len(responses) > 0:
            if any(response.is_left for response in responses):
                log(step_status=Status.FAILED, step_id=step_id)
            else:
                log(step_status=Status.SUCCEEDED, step_id=step_id)
