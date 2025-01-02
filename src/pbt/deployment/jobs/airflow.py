import base64
import hashlib
import os
import re
import zipfile
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import yaml

from .databricks import get_fabric_label
from ...client.airflow.airflow_utility import create_airflow_client, get_fabric_provider_type
from ...client.rest_client_factory import RestClientFactory
from ...deployment import EntityIdToFabricId, JobData, JobInfoAndOperation, OperationType
from ...entities.project import Project
from ...utility import Either, custom_print as log
from ...utils.constants import FABRIC_UID
from ...utils.exceptions import DagNotAvailableException
from ...utils.project_config import FabricInfo, JobInfo, ProjectConfig, await_futures_and_update_states, update_state
from ...utils.project_models import Colors, Operation, StepMetadata, StepType


def generate_secure_content(content: str, salt: str) -> str:
    iterations = 10000
    key_length = 128

    password = content.encode("utf-8")
    salt_bytes = salt.encode("utf-8")

    derived_key = hashlib.pbkdf2_hmac("sha256", password, salt_bytes, iterations, dklen=key_length // 8)

    base64_encoded = base64.b64encode(derived_key).decode("utf-8")
    return re.sub(r"\W+", "_", base64_encoded)


def calculate_checksum(input_str, salt=None):
    salt = salt or "better_salt_than_never"

    md = hashlib.sha256()
    input_bytes = input_str.encode("utf-8")
    salt_bytes = salt.encode("utf-8")

    md.update(input_bytes)
    md.update(salt_bytes)

    digest_bytes = md.digest()

    return "".join(f"{byte:02x}" for byte in digest_bytes)


def does_dag_file_exist(rdc: Dict[str, str]) -> bool:
    return rdc is not None and "dag.py" in rdc


def filter_job_files(rdc: Dict[str, str]):
    filtered_files = {
        file_name: file_content
        for file_name, file_content in rdc.items()
        if file_name == "dag.py"
        or file_name == "prophecy-job.json"
        or "__init__.py" in file_name
        or "tasks/" in file_name
        or "utils.py" in file_name
        or "utils/" in file_name
    }
    k = sorted(filtered_files.items())
    return k


def sanitize_job(job_id) -> str:
    return ("_" if job_id is None else job_id).split("/")[-1].replace("\\W", "_")


def get_zipped_dag_name(dag_name: str):
    return f"/tmp/{dag_name}.zip"


def zip_folder(rdc: dict, output_path):
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_STORED) as zipf:
        for path, content in rdc.items():
            zipf.writestr(path, content)


class AirflowJob(JobData, ABC):
    def __init__(
        self,
        job_pbt: dict,
        prophecy_job_yaml: str,
        rdc: Dict[str, str],
        rdc_with_placeholder: Dict[str, str],
        sha: Optional[str],
        fabric_override: Optional[str] = None,
    ):
        self.job_pbt = job_pbt
        self.prophecy_job_yaml = prophecy_job_yaml
        self.rdc = rdc
        self.rdc_with_placeholder = rdc_with_placeholder
        self.sha = sha
        self.fabric_override = fabric_override

        self.prophecy_job_json_dict = self._initialize_prophecy_job_json()

    @property
    def name(self):
        return self.job_pbt.get("name", None)

    @property
    def is_valid_job(self):
        prophecy_job_yaml_dict = self.prophecy_job_json_dict

        return (
            self.job_pbt is not None
            and self.prophecy_job_yaml is not None
            and self.rdc is not None
            and prophecy_job_yaml_dict.get("metainfo", {}).get("fabricId", None) is not None
        )

    # we can't use pbt file because it doesn't have fabric per pipeline which airflow jobs supports
    @property
    def pipeline_and_fabric_ids(self) -> List[EntityIdToFabricId]:
        pipeline_and_fabric_ids = []
        prophecy_job_yaml_dict = self.prophecy_job_json_dict

        if self.job_pbt is not None and self.prophecy_job_yaml is not None and self.rdc is not None:
            for process_id, process in prophecy_job_yaml_dict.get("processes", {}).items():
                properties = process.get("properties", {})

                cluster_size = properties.get("clusterSize", None)
                pipeline_id = AirflowJob.get_pipeline_id_from_properties(properties)

                if cluster_size and pipeline_id:
                    pipeline_and_fabric_ids.append(EntityIdToFabricId(pipeline_id, cluster_size.split("/")[0]))

        return pipeline_and_fabric_ids

    @staticmethod
    def get_pipeline_id_from_properties(properties: dict):
        pipeline_id = properties.get("pipelineId")
        pipeline_id_dict = properties.get("pipelineId", {})

        if isinstance(pipeline_id_dict, dict) and pipeline_id_dict.get("type") == "literal":
            pipeline_id = pipeline_id_dict.get("value")

        return pipeline_id if isinstance(pipeline_id, str) else None

    def validate_prophecy_managed_checksum(self, salt: str):
        file_joiner: str = "$$$"
        # always use rdc_with_placeholder to calculate checksum
        content = file_joiner.join(content for (name, content) in filter_job_files(self.rdc_with_placeholder))
        checksum = calculate_checksum(content, salt)
        return self.sha == checksum

    @property
    def job_files(self):
        return filter_job_files(self.rdc)

    @property
    def dag_name(self):
        return self.prophecy_job_json_dict.get("metainfo", {}).get("dagName", None)

    @property
    def is_disabled(self):
        return self.prophecy_job_json_dict.get("metainfo", {}).get("enabled", False) is False

    @property
    def is_enabled(self):
        return self.prophecy_job_json_dict.get("metainfo", {}).get("enabled", False) is True

    @property
    def fabric_id(self):
        if self.fabric_override is not None and len(self.fabric_override) > 0:
            return self.fabric_override
        else:
            fabric_id = self.job_pbt.get(FABRIC_UID)
            return str(fabric_id) if fabric_id is not None else None

    @property
    def pipelines(self):
        return self.job_pbt.get("pipelines", [])

    @property
    def has_dbt_component(self):
        return any(
            value.get("component", None) == "Model" for value in self.prophecy_job_json_dict["processes"].values()
        )

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
    _RENAME_JOBS_STEP_ID = "rename-airflow-jobs"
    _SKIP_JOBS_STEP_ID = "skip-airflow-jobs"

    def __init__(self, project: Project, project_config: ProjectConfig):
        self._project = project
        self._project_config = project_config
        self._jobs_state = project_config.jobs_state
        self._fabrics_config = project_config.fabric_config

        self._airflow_clients = {}
        self.deployment_run_override_config = project_config.configs_override
        self._rest_client_factory = RestClientFactory.get_instance(RestClientFactory, self._fabrics_config)
        self._airflow_jobs: Dict[str, AirflowJob] = self._initialize_airflow_jobs()

        (
            self.valid_airflow_jobs,
            self._invalid_airflow_jobs,
            self._airflow_jobs_without_code,
        ) = self._initialize_valid_airflow_jobs()

        self.prophecy_managed_dbt_jobs: Dict[str, AirflowJob] = self._initialize_prophecy_managed_dbt_jobs()

    def get_airflow_client(self, fabric_id):
        return self._rest_client_factory.airflow_client(fabric_id)

    def summary(self):
        summary = []

        summary.extend([f"Airflow job to remove {job.id}" for job in self._list_remove_jobs()])
        summary.extend([f"Airflow job to add {job}" for job in self._add_jobs().keys()])
        summary.extend([f"Airflow job to pause {job}" for job in self._pause_jobs().keys()])
        summary.extend([f"Airflow job to rename {job.id}" for job in self._rename_jobs()])
        summary.extend([f"Airflow job to skip {job}" for job in self._skip_jobs().keys()])

        return summary

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Remove: self._REMOVE_JOBS_STEP_ID,
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID,
            Operation.Rename: self._RENAME_JOBS_STEP_ID,
            Operation.Skipped: self._SKIP_JOBS_STEP_ID,
        }

    def headers(self):
        job_types = {
            Operation.Remove: self._list_remove_jobs(),
            Operation.Add: self._add_jobs(),
            Operation.Pause: self._pause_jobs(),
            Operation.Rename: self._rename_jobs(),
            Operation.Skipped: self._skip_jobs(),
        }

        all_headers = []

        for job_action, jobs in job_types.items():
            if jobs:
                len_jobs = len(jobs)
                job_header_suffix = f"{len_jobs} Airflow job" if len_jobs == 1 else f"{len_jobs} Airflow jobs"
                step_id = self._operation_to_step_id[job_action]
                all_headers.append(
                    StepMetadata(step_id, f"{job_action.value} {job_header_suffix}", job_action, StepType.Job)
                )

        return all_headers

    def deploy(self):
        if len(self.headers()) > 0:
            log(f"{Colors.OKBLUE}\n\nDeploying airflow jobs{Colors.ENDC}\n")

        responses = (
            self._deploy_remove_jobs()
            + self._deploy_pause_jobs()
            + self._deploy_add_jobs()
            + self._deploy_rename_jobs()
        )

        self._deploy_skipped_jobs()

        return responses

    def _initialize_airflow_jobs(self):
        jobs = {}

        for job_id, parsed_job in self._project.jobs.items():
            fabric_override = self.deployment_run_override_config.find_fabric_override_for_job(job_id)
            fabric_override = str(fabric_override) if fabric_override is not None else None

            job_fabric = parsed_job.get("fabricUID", None)
            job_fabric = str(job_fabric) if job_fabric is not None else None

            does_fabric_exist = (
                self._fabrics_config.get_fabric(job_fabric) is not None
                or self._fabrics_config.get_fabric(fabric_override) is not None
            )

            if (
                "Databricks" not in parsed_job.get("scheduler", None)
                and self.deployment_run_override_config.is_job_to_run(job_id)
                and does_fabric_exist
            ):
                rdc_with_placeholders = self._project.load_airflow_folder_with_placeholder(job_id)
                rdc = self._project.load_airflow_folder(job_id)

                aspects = self._project.load_airflow_aspect(job_id)

                prophecy_job_json = None
                sha = None

                if rdc is not None and "prophecy-job.json" in rdc:
                    prophecy_job_json = rdc.get("prophecy-job.json", None)

                if aspects is not None:
                    try:
                        sha = yaml.safe_load(aspects).get("sha", None)
                    except Exception as e:
                        log("Error while loading prophecy job yaml", e)

                jobs[job_id] = AirflowJob(
                    parsed_job,
                    prophecy_job_json,
                    rdc,
                    rdc_with_placeholders,
                    sha,
                    fabric_override=self.deployment_run_override_config.find_fabric_override_for_job(job_id),
                )

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
        is_prophecy_managed_fabric = self._fabrics_config.is_prophecy_managed(job_data.fabric_id)
        rdc = job_data.rdc

        if does_dag_file_exist(rdc) is False:
            return Either(
                left=Exception(f"Please open the Job `{job_id}` in editor, check diagnostic errors and release again.")
            )

        elif is_prophecy_managed_fabric:
            if job_data.validate_prophecy_managed_checksum(self._project_config.system_config.prophecy_salt):
                return Either(right=job_data.job_files)

            else:
                return Either(
                    left=Exception(
                        f"Job `{job_id}` has been externally edited. Please open the Job in editor and release again."
                    )
                )
        else:
            return Either(right=rdc)

    # we won't be able to completely validated the prophecy_job_json structure to prophecy_job.json.
    def _initialize_prophecy_managed_dbt_jobs(self) -> Dict[str, AirflowJob]:
        prophecy_managed_dbt_jobs = {}

        for job_id, job_data in self.valid_airflow_jobs.items():
            is_job_enabled = job_data.is_enabled
            is_prophecy_managed_fabric = self._fabrics_config.is_prophecy_managed(job_data.fabric_id)

            if is_job_enabled and is_prophecy_managed_fabric and job_data.has_dbt_component:
                prophecy_managed_dbt_jobs[job_id] = job_data

        return prophecy_managed_dbt_jobs

    def _list_remove_jobs(self) -> List[JobInfo]:
        return self._jobs_to_be_deleted() + self._jobs_with_fabric_changed()

    def _deploy_remove_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_info in self._list_remove_jobs():
                futures.append(executor.submit(lambda j_info=job_info: self._remove_job(j_info)))

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Remove])

    def _remove_job(self, job_info: JobInfo):
        client = self.get_airflow_client(job_info.fabric_id)

        try:
            client.delete_dag_file(job_info.external_job_id)
            log(
                f"Successfully deleted job {job_info.external_job_id} from job_id {job_info.id}",
                step_id=self._REMOVE_JOBS_STEP_ID,
            )
            return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))

        except Exception as e:
            log(
                f"Error while deleting job {job_info.external_job_id} from job_id {job_info.id}, Please delete the Dag manually from Dag location if required.",
                exception=e,
                step_id=self._REMOVE_JOBS_STEP_ID,
            )
            return Either(right=JobInfoAndOperation(job_info, OperationType.DELETED))

    """
        Compare what exists in state config to what exists in code.
     """

    def _jobs_to_be_deleted(self) -> List[JobInfo]:
        all_jobs = {**self.valid_airflow_jobs, **self._invalid_airflow_jobs, **self._airflow_jobs_without_code}
        return [
            airflow_job
            for airflow_job in self._jobs_state.airflow_jobs
            if not any(
                airflow_job.id == job_id
                for job_id in list(all_jobs.keys())
                # check from available airflow jobs.
            )
            and self._fabrics_config.get_fabric(airflow_job.fabric_id) is not None
        ]

    """
        There exist a job in folder whose fabric id is different from the one in state config.
        folder can only have one job with a given job id.
    """

    def _jobs_with_fabric_changed(self) -> List[JobInfo]:
        return [
            airflow_job
            for airflow_job in self._jobs_state.airflow_jobs
            if any(
                airflow_job.id == job_id and airflow_job.fabric_id != job_data.fabric_id
                for job_id, job_data in self.valid_airflow_jobs.items()
                # Check only from valid airflow jobs.
            )
        ]

    def _rename_jobs(self) -> List[JobInfo]:
        return [
            airflow_job
            for airflow_job in self._jobs_state.airflow_jobs
            if any(
                airflow_job.id == job_id
                and airflow_job.fabric_id == job_data.fabric_id
                and job_data.is_enabled
                and airflow_job.external_job_id != job_data.dag_name
                for job_id, job_data in self.valid_airflow_jobs.items()
            )
        ]

    def _all_removed_airflow_jobs(self) -> List[JobInfo]:
        return self._rename_jobs() + self._jobs_with_fabric_changed() + self._jobs_to_be_deleted()

    # Here we are looking for the flipped state of the job.
    # jobs which are enabled in state config but disabled in code.
    # and always get jobs from deployment state.
    def _pause_jobs(self) -> Dict[str, JobInfo]:
        old_enabled_job = {job.id: job for job in self._jobs_state.airflow_jobs if job.is_paused is False}

        old_enabled_job_which_are_disabled_in_new = {
            job_id: job_info
            for job_id, job_info in old_enabled_job.items()
            if any(
                job_id == _id and job_info.fabric_id == job_data.fabric_id and job_data.is_disabled
                # disabled in new
                for _id, job_data in self.valid_airflow_jobs.items()
            )
        }

        jobs_not_in_removed_jobs = {
            job_id: job_info
            for job_id, job_info in old_enabled_job_which_are_disabled_in_new.items()
            if not self._all_removed_airflow_jobs()
            and not any(airflow_job.id == job_id for airflow_job in self._all_removed_airflow_jobs())
        }

        return jobs_not_in_removed_jobs

    # always add new jobs only if they are enabled.
    # this is equivalent to refresh because airflow merge based on it's dag_name.
    def _add_jobs(self) -> Dict[str, AirflowJob]:
        return {job_id: job_data for job_id, job_data in self.valid_airflow_jobs.items() if job_data.is_enabled is True}

    def _deploy_add_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self._add_jobs().items():
                futures.append(executor.submit(lambda j_id=job_id, j_data=job_data: self._add_job(j_id, j_data)))

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Add])

    def _add_job(self, job_id, job_data):
        dag_name = job_data.dag_name
        zipped_dag_name = get_zipped_dag_name(dag_name)
        zip_folder(self._project.load_airflow_folder(job_id), zipped_dag_name)

        fabric_config = self._fabrics_config.get_fabric(job_data.fabric_id)
        fabric_name = fabric_config.name if fabric_config is not None else None

        fabric_label = get_fabric_label(fabric_name, job_data.fabric_id)

        try:
            client = self.get_airflow_client(fabric_id=job_data.fabric_id)
            client.upload_dag(dag_name, zipped_dag_name)

            # start of try-catch
            try:
                client.unpause_dag(dag_name)
                log(
                    f"{Colors.OKGREEN}Successfully un-paused dag:{dag_name} for job:{job_id} and fabric:{fabric_label}{Colors.ENDC}",
                    step_id=self._ADD_JOBS_STEP_ID,
                )

            except DagNotAvailableException as e:
                log(
                    f"{Colors.FAIL}Dag:{dag_name} for job:{job_id} and fabric:{fabric_label} not available after exhausting all the retries,"
                    f" please check with administrator{Colors.ENDC}",
                    exception=e,
                    step_id=self._ADD_JOBS_STEP_ID,
                )

                return Either(left=e)

            except Exception as e:
                log(
                    f"{Colors.FAIL}Failed to un-pause dag with name:{dag_name} for job:{job_id} and fabric:{fabric_label}{Colors.ENDC}",
                    exception=e,
                    step_id=self._ADD_JOBS_STEP_ID,
                )

                return Either(left=e)

            # end of try-catch

            log(
                f"{Colors.OKGREEN}Successfully added job:{dag_name} for job_id:{job_id} on fabric:{fabric_label}{Colors.ENDC}",
                step_id=self._ADD_JOBS_STEP_ID,
            )

            job_info = JobInfo.create_job(
                job_data.name,
                job_id,
                job_data.fabric_id,
                dag_name,
                self._project.release_tag,
                job_data.is_disabled,
                get_fabric_provider_type(job_data.fabric_id, self._project_config),
            )

            return Either(right=JobInfoAndOperation(job_info, OperationType.CREATED))
        except Exception as e:
            log(
                f"{Colors.FAIL}Failed to upload_dag for job_id:{job_id} with dag_name {dag_name}{Colors.ENDC}",
                e,
                step_id=self._ADD_JOBS_STEP_ID,
            )
            return Either(left=e)

    def _deploy_skipped_jobs(self):
        for job_id, message in self._skip_jobs().items():
            log(
                f"Skipping job_id: {job_id} encountered some error ",
                exception=message.left,
                step_id=self._SKIP_JOBS_STEP_ID,
            )
        if len(self._skip_jobs()) > 0:
            update_state([Either(right=True)], self._operation_to_step_id[Operation.Skipped])

    def _skip_jobs(self):
        return {
            job_id: self._validate_airflow_job(job_id, job_data)
            for job_id, job_data in self._airflow_jobs.items()
            if self._validate_airflow_job(job_id, job_data).is_left
        }

    def _deploy_rename_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_info in self._rename_jobs():
                futures.append(executor.submit(lambda j_info=job_info: self._delete_job_for_renamed_job(j_info)))

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Rename])

    def _delete_job_for_renamed_job(self, jobs_info: JobInfo):
        try:
            client = create_airflow_client(jobs_info.fabric_id, self._project_config)
            client.delete_dag(sanitize_job(jobs_info.external_job_id))

            log(
                f"{Colors.OKGREEN}Successfully deleted dag for job_id: {jobs_info.id}{Colors.ENDC}",
                step_id=self._RENAME_JOBS_STEP_ID,
            )

            return Either(right=JobInfoAndOperation(jobs_info, OperationType.DELETED))
        except Exception as e:
            log(
                f"{Colors.WARNING}Failed to delete dag for job_id: {jobs_info.id} with job name {jobs_info.external_job_id} in fabric {jobs_info.fabric_id}, Please delete the Dag manually from Dag location if required.{Colors.ENDC}",
                exception=e,
                step_id=self._RENAME_JOBS_STEP_ID,
            )
            return Either(right=JobInfoAndOperation(jobs_info, OperationType.DELETED))

    def _deploy_pause_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self._pause_jobs().items():
                futures.append(executor.submit(lambda j_id=job_id, j_data=job_data: self._pause_job(j_id, j_data)))

        return await_futures_and_update_states(futures, self._operation_to_step_id[Operation.Pause])

    def _pause_job(self, job_id: str, job_info: JobInfo):
        if len(job_info.external_job_id) > 0:
            dag_name = job_info.external_job_id
        else:
            dag_name = sanitize_job(job_id)

        job_info.pause(True)

        try:
            client = self.get_airflow_client(job_info.fabric_id)
            client.pause_dag(dag_name)
            log(
                f"{Colors.OKGREEN}Successfully paused job {dag_name} for job_id {job_id}{Colors.ENDC}",
                step_id=self._PAUSE_JOBS_STEP_ID,
            )
            return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))
        except Exception as e:
            log(
                f"{Colors.WARNING}Failed to pause_dag for job_id: {job_id} with dag_name {dag_name}{Colors.ENDC}",
                exception=e,
                step_id=self._PAUSE_JOBS_STEP_ID,
            )
            return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))


class AirflowGitSecrets:
    _AIRFLOW_GIT_SECRETS_STEP_ID = "airflow-git-secrets"

    def __init__(self, project: Project, airflow_jobs: AirflowJobDeployment, project_config: ProjectConfig):
        self.project = project
        self.airflow_jobs = airflow_jobs
        self.project_config = project_config
        self.deployment_state = project_config.jobs_state
        self.fabric_config = project_config.fabric_config

    def summary(self) -> List[str]:
        summary = []

        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:
            for project_git_tokens in self.fabric_config.project_git_tokens:
                git_tokens = project_git_tokens.git_token
                project_id = project_git_tokens.project_id

                if len(git_tokens) > 0:
                    summary.append(f"Creating git secrets for project {project_id} ")

        return summary

    def headers(self) -> List[StepMetadata]:
        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:
            return [
                StepMetadata(
                    self._AIRFLOW_GIT_SECRETS_STEP_ID,
                    "Create git secrets for airflow jobs",
                    Operation.Build,
                    StepType.AirflowGitSecrets,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        if len(self.airflow_jobs.prophecy_managed_dbt_jobs) > 0:
            if len(self.fabric_config.project_git_tokens) > 0:
                log(f"\n\n{Colors.OKBLUE} Uploading project git tokens {Colors.ENDC}\n\n")

            job_data = list(self.airflow_jobs.prophecy_managed_dbt_jobs.values())[0]

            # making this single threaded for now.
            with ThreadPoolExecutor(max_workers=1) as executor:
                log(
                    f"{Colors.OKGREEN}Updating git secrets{Colors.ENDC}",
                    step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID,
                )
                for project_git_tokens in self.fabric_config.project_git_tokens:
                    git_tokens = project_git_tokens.git_token
                    project_id = project_git_tokens.project_id

                    if len(git_tokens) == 0:
                        log(
                            f"{Colors.WARNING}No git tokens found for project_id: {project_id}, Ignoring{Colors.ENDC}",
                            step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID,
                        )

                    else:
                        futures.append(
                            executor.submit(
                                lambda p_id=project_id, j_data=job_data, token=git_tokens: self._create_git_secrets(
                                    p_id, j_data, token
                                )
                            )
                        )

            return await_futures_and_update_states(futures, self._AIRFLOW_GIT_SECRETS_STEP_ID)
        else:
            # log("No dbt jobs found, Ignoring", step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID)
            return []

    def _create_git_secrets(self, project_id, job_data, git_tokens):
        execution_db_suffix = os.getenv("EXECUTION_DB_SUFFIX", "dev")
        try:
            client = self.airflow_jobs.get_airflow_client(job_data.fabric_id)
            key = generate_secure_content(f"{execution_db_suffix}_{project_id}", "gitSecretSalt")
            client.create_secret(key, git_tokens)
            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.WARNING}Failed in creating git secrets for project {project_id}{Colors.ENDC}",
                exception=e,
                step_id=self._AIRFLOW_GIT_SECRETS_STEP_ID,
            )
            return Either(left=e)


class EMRPipelineConfigurations:
    _STEP_ID = "EMR_pipeline_configurations"

    def __init__(self, project: Project, airflow_jobs: AirflowJobDeployment, project_config: ProjectConfig):
        self.airflow_jobs = airflow_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project_config = project_config
        self.project = project
        self._jobs_state = project_config.jobs_state
        self._fabric_config = project_config.fabric_config
        self._rest_client_factory = RestClientFactory.get_instance(RestClientFactory, self._fabric_config)

    def _emr_fabrics(self):
        return self._fabric_config.emr_fabrics()

    def summary(self) -> List[str]:
        summary = []
        for ids in self.pipeline_configurations.keys():
            if len(self._emr_fabrics()) > 0:
                summary.append(f"Uploading pipeline emr-configurations for pipeline {ids}")

        return summary

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0 and len(self._emr_fabrics()) > 0:
            return [
                StepMetadata(
                    self._STEP_ID,
                    f"Upload {len(all_configs)} pipeline emr-configurations",
                    Operation.Upload,
                    StepType.PipelineConfiguration,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            if len(self.headers()) > 0:
                log(f"\n\n{Colors.OKBLUE}Uploading EMR pipeline configurations {Colors.ENDC}\n\n")

            def execute_job(_fabric_info, _config_content, _config_path):
                futures.append(
                    executor.submit(
                        lambda f_info=_fabric_info, conf_content=_config_content, conf_path=_config_path: self._upload_configuration(
                            f_info, conf_content, conf_path
                        )
                    )
                )

            for pipeline_id, configurations in self.pipeline_configurations.items():
                path = self.project_config.system_config.get_s3_base_path()
                pipeline_path = (
                    f"{path}/{self.project.project_id}/{self.project.release_version}/configurations/{pipeline_id}"
                )

                for configuration_name, configuration_content in configurations.items():
                    configuration_path = f"{pipeline_path}/{configuration_name}.jsn"

                    for fabric_info in self._fabric_config.emr_fabrics():
                        if fabric_info.emr is not None:
                            execute_job(fabric_info, configuration_content, configuration_path)

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _upload_configuration(self, fabric_info: FabricInfo, configuration_content, configuration_path):
        upload_path = f"{fabric_info.emr.bare_path_prefix()}/{configuration_path}"
        emr_info = fabric_info.emr

        try:
            client = self._rest_client_factory.s3_client(str(fabric_info.id))
            client.upload_content(emr_info.bare_bucket(), upload_path, configuration_content)

            log(
                f"{Colors.OKGREEN}Uploaded pipeline configuration on path {upload_path} on fabric {fabric_info.id}{Colors.ENDC}",
                step_id=self._STEP_ID,
            )

            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.FAIL}Failed to upload pipeline configuration for path {upload_path} for fabric {fabric_info.id}{Colors.ENDC}",
                exception=e,
                step_id=self._STEP_ID,
            )
            return Either(left=e)


class DataprocPipelineConfigurations:
    _STEP_ID = "DATAPROC_pipeline_configurations"

    def __init__(self, project: Project, airflow_jobs: AirflowJobDeployment, project_config: ProjectConfig):
        self.airflow_jobs = airflow_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project = project
        self.project_config = project_config
        self._deployment_state = project_config.jobs_state
        self._fabric_config = project_config.fabric_config
        self._rest_client_factory = RestClientFactory.get_instance(RestClientFactory, self._fabric_config)

    def _dataproc_fabrics(self):
        return self._fabric_config.dataproc_fabrics()

    def summary(self) -> List[str]:
        summary = []
        for ids in self.pipeline_configurations.keys():
            if len(self._dataproc_fabrics()) > 0:
                summary.append(f"Uploading pipeline dataproc-configurations for pipeline {ids}")

        return summary

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0 and len(self._dataproc_fabrics()) > 0:
            return [
                StepMetadata(
                    self._STEP_ID,
                    f"Upload {len(all_configs)} pipeline dataproc-configurations",
                    Operation.Upload,
                    StepType.PipelineConfiguration,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            if len(self.headers()) > 0:
                log(f"\n\n{Colors.OKBLUE} Uploading dataproc configurations {Colors.ENDC}\n\n")

            def execute_job(fabric_info, configuration_content, configuration_path):
                futures.append(
                    executor.submit(
                        lambda f_info=fabric_info, conf_content=configuration_content, conf_path=configuration_path: self._upload_configuration(
                            f_info, conf_content, conf_path
                        )
                    )
                )

            for pipeline_id, configurations in self.pipeline_configurations.items():
                path = self.project_config.system_config.get_s3_base_path()
                pipeline_path = (
                    f"{path}/{self.project.project_id}/{self.project.release_version}/configurations/{pipeline_id}"
                )
                for configuration_name, configuration_content in configurations.items():
                    configuration_path = f"{pipeline_path}/{configuration_name}.jsn"

                    for fabric_info in self._fabric_config.dataproc_fabrics():
                        if fabric_info.dataproc is not None:
                            execute_job(fabric_info, configuration_content, configuration_path)

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _upload_configuration(self, fabric_info: FabricInfo, configuration_content, configuration_path):
        upload_path = f"{fabric_info.dataproc.bare_path_prefix()}/{configuration_path}"
        dataproc_info = fabric_info.dataproc

        try:
            client = self._rest_client_factory.dataproc_client(str(fabric_info.id))
            client.put_object(dataproc_info.bare_bucket(), upload_path, configuration_content)

            log(
                f"{Colors.OKGREEN}Uploaded pipeline configuration on path {upload_path}{Colors.ENDC}",
                step_id=self._STEP_ID,
            )

            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.FAIL}Failed to upload pipeline configuration for path {upload_path} for fabric {fabric_info.id}{Colors.ENDC}",
                exception=e,
                step_id=self._STEP_ID,
            )
            return Either(left=e)


class SparkSubmitPipelineConfigurations:
    _STEP_ID = "SPARK_SUBMIT_pipeline_configurations"

    def __init__(self, project: Project, airflow_jobs: AirflowJobDeployment, project_config: ProjectConfig):
        self.airflow_jobs = airflow_jobs
        self.pipeline_configurations = project.pipeline_configurations
        self.project = project
        self.project_config = project_config
        self._deployment_state = project_config.jobs_state
        self._fabric_config = project_config.fabric_config
        self._rest_client_factory = RestClientFactory.get_instance(RestClientFactory, self._fabric_config)

    def _spark_submit_fabrics(self):
        return self._fabric_config.airflow_open_source_fabrics()

    def summary(self) -> List[str]:
        summary = []
        for ids in self.pipeline_configurations.keys():
            if len(self._spark_submit_fabrics()) > 0:
                summary.append(f"Uploading pipeline spark-submit-configurations for pipeline {ids}")

        return summary

    def headers(self) -> List[StepMetadata]:
        all_configs = [value for sublist in self.pipeline_configurations.values() for value in sublist]
        if len(all_configs) > 0 and len(self._spark_submit_fabrics()) > 0:
            return [
                StepMetadata(
                    self._STEP_ID,
                    f"Upload {len(all_configs)} pipeline spark-submit-configurations",
                    Operation.Upload,
                    StepType.PipelineConfiguration,
                )
            ]
        else:
            return []

    def deploy(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            if len(self.headers()) > 0:
                log(f"\n\n{Colors.OKBLUE} Uploading spark-submit configurations {Colors.ENDC}\n\n")

            def execute_job(
                fabric_info, configuration_relative_directory, configuration_file_name, configuration_content
            ):
                futures.append(
                    executor.submit(
                        lambda f_info=fabric_info, conf_content=configuration_content, conf_path=f"{configuration_relative_directory}/{configuration_file_name}": self._upload_configuration(
                            f_info, configuration_relative_directory, configuration_file_name, configuration_content
                        )
                    )
                )

            for pipeline_id, configurations in self.pipeline_configurations.items():
                path = self.project_config.system_config.get_hdfs_base_path()
                pipeline_path = (
                    f"{path}/{self.project.project_id}/{self.project.release_version}/configurations/{pipeline_id}"
                )
                for configuration_name, configuration_content in configurations.items():
                    configuration_file_name = f"{configuration_name}.jsn"
                    configuration_relative_directory = pipeline_path

                    for fabric_info in self._spark_submit_fabrics():
                        if fabric_info.airflow_oss is not None:
                            execute_job(
                                fabric_info,
                                configuration_relative_directory,
                                configuration_file_name,
                                configuration_content,
                            )

        return await_futures_and_update_states(futures, self._STEP_ID)

    def _upload_configuration(
        self, fabric_info: FabricInfo, configuration_relative_directory, configuration_file_name, configuration_content
    ):
        upload_directory = f"{fabric_info.airflow_oss.location}/{configuration_relative_directory}"
        try:
            client = self._rest_client_factory.open_source_hdfs_client(str(fabric_info.id))
            client.put_object(upload_directory, configuration_file_name, configuration_content)

            log(
                f"{Colors.OKGREEN}Uploaded pipeline configuration on path {upload_directory}{Colors.ENDC}",
                step_id=self._STEP_ID,
            )

            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.FAIL}Failed to upload pipeline configuration for path {upload_directory} for fabric {fabric_info.id}{Colors.ENDC}",
                exception=e,
                step_id=self._STEP_ID,
            )
            return Either(left=e)
