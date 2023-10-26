from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from typing import List

from src.pbt.utils.project_models import Operation, StepMetadata, StepType
from src.pbt.utils.utility import Either
from src.pbt.utils.utility import custom_print as log
from . import DatabricksJobs, DatabricksJobsDeployment, get_futures_and_update_steps
from ....v2.deployment import JobInfoAndOperation, OperationType


class DatabrickJobs(ABC):
    def summary(self):
        pass

    def headers(self):
        pass

    def deploy(self) -> List[Either]:
        pass


class DatabricksJobsDeploymentApp(DatabricksJobs, ABC):
    def __init__(self, job_deployment: DatabricksJobsDeployment):
        self.job_deployment = job_deployment

    @property
    def _operation_to_step_id(self):
        return {
            Operation.Add: self._ADD_JOBS_STEP_ID,
            Operation.Refresh: self._REFRESH_JOBS_STEP_ID,
            Operation.Remove: self._DELETE_JOBS_STEP_ID,
            Operation.Pause: self._PAUSE_JOBS_STEP_ID
        }

    def summary(self):
        summary = []

        summary.extend([f"Adding job {id}" for id in list(self.job_deployment._add_jobs().keys())])
        summary.extend([f"Refreshing job {id}" for id in list(self.job_deployment._refresh_jobs().keys())])
        summary.extend([f"Deleting job {id}" for id in list(self.job_deployment._delete_jobs().keys())])
        summary.extend([f"Pausing job {job.id}" for job in list(self.job_deployment._pause_jobs())])

        return summary

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

    def deploy(self) -> List[Either]:
        responses = self._deploy_add_jobs() + \
                    self._deploy_refresh_jobs() + \
                    self._deploy_delete_jobs() + \
                    self.job_deployment._deploy_pause_jobs()

        return responses

    def _deploy_add_jobs(self):

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            for job_id, job_data in self._add_jobs().items():
                fabric_id = job_data.fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(
                        lambda j_id=job_id, j_data=job_data: self._deploy_add_job(j_id, j_data,
                                                                                  self._ADD_JOBS_STEP_ID)))
                else:
                    log(f"In valid fabric {fabric_id}, skipping job creation for job_id {job_id}",
                        step_id=self._ADD_JOBS_STEP_ID)

            (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Add])
            return responses

    def _deploy_refresh_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:

            for job_id, job_info in self._refresh_jobs().items():
                fabric_id = job_info.fabric_id

                if fabric_id is not None:
                    futures.append(executor.submit(
                        lambda j_id=job_id, j_info=job_info: self._reset_and_patch_job(j_id, j_info)))
                else:
                    log(f"In valid fabric {fabric_id}, skipping job refresh for job_id {job_id}")

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Refresh])
        return responses

    def _deploy_delete_jobs(self):
        futures = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            for job_id, job_info in self._delete_jobs().items():
                futures.append(executor.submit(lambda j_info=job_info: self._delete_job(j_info)))

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Remove])
        return responses

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
                log(f"Error pausing job {external_job_id} in fabric {fabric_id}, Ignoring this ", exception=e,
                    step_id=self._PAUSE_JOBS_STEP_ID)
                return Either(right=JobInfoAndOperation(job_info, OperationType.REFRESH))

        with ThreadPoolExecutor(max_workers=3) as executor:

            for jobs_info in self._pause_jobs():
                fabric = jobs_info.fabric_id

                if fabric is not None:
                    futures.append(executor.submit(lambda f=fabric, j_info=jobs_info: pause_job(f, j_info)))
                else:
                    log(f"In valid fabric {fabric} for job_id {jobs_info.id} ", step_id=self._PAUSE_JOBS_STEP_ID)

        (responses, x) = get_futures_and_update_steps(futures, self._operation_to_step_id[Operation.Pause])
        return responses


class DatabricksJobsDeploymentCli(DatabricksJobs, ABC):
    def summary(self):
        pass

    def headers(self):
        pass

    def deploy(self) -> List[Either]:
        pass
