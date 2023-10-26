from abc import ABC
from concurrent.futures import ThreadPoolExecutor

from . import AirflowJobDeployment
from .. import update_step_state
from ..databricks import get_futures_and_update_steps
from src.pbt.utils.project_models import Operation, StepMetadata, StepType
from src.pbt.utils.utility import Either
from src.pbt.utils.utility import custom_print as log


class AirflowJobs(ABC):
    def deploy(self):
        pass

    def headers(self):
        pass

    def summary(self):
        pass


class AirflowJobsScalaService(AirflowJobs, ABC):
    _REMOVE_JOBS_STEP_ID = "remove-airflow-jobs"
    _ADD_JOBS_STEP_ID = "add-airflow-jobs"
    _PAUSE_JOBS_STEP_ID = "pause-airflow-jobs"
    _RENAME_JOBS_STEP_ID = "rename-airflow-jobs"
    _SKIP_JOBS_STEP_ID = "skip-airflow-jobs"

    def __init__(self, job_deployment: AirflowJobDeployment):
        self.job_deployment = job_deployment

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Remove: self._REMOVE_JOBS_STEP_ID,
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID,
            Operation.Rename: self._RENAME_JOBS_STEP_ID,
            Operation.Skipped: self._SKIP_JOBS_STEP_ID
        }

    def summary(self):
        summary = []

        summary.extend([f"Airflow job to remove {job.id}" for job in self.job_deployment.list_remove_jobs()])
        summary.extend([f"Airflow job to add {job}" for job in self.job_deployment.get_add_jobs().keys()])
        summary.extend([f"Airflow job to pause {job}" for job in self.job_deployment.get_pause_jobs().keys()])
        summary.extend([f"Airflow job to rename {job.id}" for job in self.job_deployment.get_rename_jobs()])
        summary.extend([f"Airflow job to skip {job}" for job in self.job_deployment.skip_jobs().keys()])

        return summary

    def headers(self):
        job_types = {
            Operation.Remove: self.job_deployment.list_remove_jobs(),
            Operation.Add: self.job_deployment.get_add_jobs(),
            Operation.Pause: self.job_deployment.get_pause_jobs(),
            Operation.Rename: self.job_deployment.get_rename_jobs(),
            Operation.Skipped: self.job_deployment.skip_jobs()
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
                    self._deploy_pause_jobs() + \
                    self._deploy_add_jobs() + \
                    self._deploy_rename_jobs()

        self._deploy_skipped_jobs()

        return responses

    def _deploy_remove_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_info in self.job_deployment.list_remove_jobs():
                futures.append(executor.submit(lambda j_info=job_info: self.job_deployment.remove_job(j_info)))

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Remove])
        return responses

    def _deploy_pause_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self.job_deployment.get_pause_jobs().items():
                futures.append(
                    executor.submit(lambda j_id=job_id, j_data=job_data: self.job_deployment.pause_job(j_id, j_data)))

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Pause])
        return responses

    def _deploy_add_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_id, job_data in self.job_deployment.get_add_jobs().items():
                futures.append(
                    executor.submit(lambda j_id=job_id, j_data=job_data: self.job_deployment.add_job(j_id, j_data)))

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Add])
        return responses

    def _deploy_rename_jobs(self):
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for job_info in self.job_deployment.get_rename_jobs():
                futures.append(
                    executor.submit(lambda j_info=job_info: self.job_deployment.delete_job_for_renamed_job(j_info)))

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Rename])
        return responses

    def _deploy_skipped_jobs(self):
        for job_id, message in self.job_deployment.skip_jobs().items():
            log(f"Skipping job_id: {job_id} encountered some error ", exception=message.left,
                step_id=self._SKIP_JOBS_STEP_ID)
        if len(self.job_deployment.skip_jobs()) > 0:
            update_step_state([Either(right=True)], self._operation_to_step_id[Operation.Skipped])


class AirflowJobsCli(AirflowJobs, ABC):
    def deploy(self):
        pass

    def headers(self):
        pass

    def summary(self):
        pass
