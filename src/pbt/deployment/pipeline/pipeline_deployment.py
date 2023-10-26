from abc import ABC
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict

from .builder import PackageBuilder
from ..jobs import JobData
from ..jobs.airflow import AirflowJobDeployment
from ..jobs.databricks import DatabricksJobsDeployment
from ..project import Project
from ...utils.project_config import ProjectConfig, DeploymentMode
from ...utils.project_models import StepMetadata, Operation, StepType, Status
from ...utils.utility import custom_print as log


class PipelineDeployment(ABC):
    def summary(self):
        pass

    def headers(self):
        pass

    def build(self):
        pass

    def deploy(self):
        pass

    def build_and_deploy(self):
        pass


class PipelineDeploymentApp(PipelineDeployment, ABC):
    def __init__(self, project: Project, databricks_jobs: DatabricksJobsDeployment,
                 airflow_jobs: AirflowJobDeployment,
                 project_config: ProjectConfig, job_ids: Optional[List[str]] = None,
                 pipelines_to_build: Optional[List[str]] = None):
        self.job_ids = job_ids
        self.pipelines_to_build = pipelines_to_build

        self.databricks_jobs = databricks_jobs
        self.airflow_jobs = airflow_jobs

        self.project = project
        self.project_config = project_config
        self.deployment_mode = project_config.configs_override.mode
        self.are_tests_enabled = project_config.configs_override.tests_enabled

        self.pipeline_id_to_local_path = {}
        self.has_pipelines = False  # in case deployment doesn't have any pipelines.

    @property
    def _pipeline_to_list_fabrics(self) -> Dict[str, List[str]]:
        if self.deployment_mode == DeploymentMode.SelectiveJob:
            pipeline_id_to_fabrics_dict = {}

            for job_id, job_data in self._all_jobs.items():
                for entity in job_data.pipeline_and_fabric_ids:
                    if entity.entity_id not in pipeline_id_to_fabrics_dict:
                        pipeline_id_to_fabrics_dict[entity.entity_id] = set()

                    pipeline_id_to_fabrics_dict[entity.entity_id].add(entity.fabric_id)

            return {pipeline_id: list(fabric_ids) for pipeline_id, fabric_ids in pipeline_id_to_fabrics_dict.items()}

        else:
            pipelines_to_fabrics = {}
            for pipeline_id in self.project.pipelines:
                pipelines_to_fabrics[pipeline_id] = self.project_config.fabric_config.list_all_fabrics()

            return pipelines_to_fabrics

    def _pipeline_components_from_jobs(self):
        pipeline_components = {}

        for pipeline_id in list(self._pipeline_to_list_fabrics.keys()):
            pipeline_name = self.project.get_pipeline_name(pipeline_id)
            pipeline_components[pipeline_id] = pipeline_name

        return pipeline_components

    @property
    def _all_jobs(self) -> Dict[str, JobData]:
        return {
            **self.databricks_jobs.valid_databricks_jobs,
            **self.airflow_jobs.valid_airflow_jobs
        }

    def summary(self):
        summary = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            summary.append(f"Pipeline {pipeline_id} will be build and uploaded.")
        return summary

    def headers(self):
        headers = []
        for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
            headers.append(StepMetadata(pipeline_id, f"Build {pipeline_name} pipeline",
                                        Operation.Build, StepType.Pipeline))
        return headers

    def upload(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            responses = []

            for pipeline_id, pipeline_name in self._pipeline_components_from_jobs().items():
                log(f"Building pipeline {pipeline_id}", step_id=pipeline_id)
                log(step_id=pipeline_id, step_status=Status.RUNNING)

                pipeline_builder = PackageBuilder(self.project, pipeline_id, pipeline_name, self.project_config,
                                                  are_tests_enabled=self.are_tests_enabled)
                futures.append(executor.submit(lambda p=pipeline_builder: p.build_pipeline()))

            for future in as_completed(futures):
                response = future.result()
                responses.append(response)

                if response.is_right:
                    (pipeline_id, pipeline_package_path) = response.right
                    self.pipeline_id_to_local_path[pipeline_id] = pipeline_package_path

            self.has_pipelines = True
            return responses

    def deploy(self):

        failed_response = []

        if not self.has_pipelines:
            build_response = self.upload()
            failed_response = [response for response in build_response if response.is_left]

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            for pipeline_id, list_of_entities_to_fabric_id in self._pipeline_to_list_fabrics.items():
                # ignore pipelines that are not in the list of pipelines to build.
                # they are already failed.
                if pipeline_id in self.pipeline_id_to_local_path:
                    pipeline_uploader = PipelineUploadManager(self.project, self.project_config, pipeline_id,
                                                              list_of_entities_to_fabric_id,
                                                              self.pipeline_id_to_local_path[pipeline_id])

                    futures.append(
                        executor.submit(
                            lambda uploader=pipeline_uploader: uploader.upload_pipeline()))

        responses = []
        for future in as_completed(futures):
            responses.append(future.result())

        # merging the failed responses from build and upload.
        return responses + failed_response


class PipelineDeploymentCli(PipelineDeployment, ABC):
    def summary(self):
        pass

    def headers(self):
        pass

    def build(self):
        pass

    def deploy(self):
        pass

    def build_and_deploy(self):
        pass
