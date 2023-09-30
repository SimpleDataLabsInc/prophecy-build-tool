import copy
import os
from typing import List

import yaml

from .gems import GemsDeployment
from ..constants import NEW_JOB_STATE_FILE
from ..deployment.jobs.airflow import AirflowJobDeployment, AirflowGitSecrets, EMRPipelineConfigurations, \
    DataprocPipelineConfigurations
from ..deployment.jobs.databricks import DatabricksJobsDeployment, ScriptComponents, PipelineConfigurations, \
    DBTComponents
from ..deployment.pipeline import PipelineDeployment
from ..entities.project import Project
from ..project_config import ProjectConfig
from ..project_models import StepMetadata, Operation, StepType, Status
from ..utility import custom_print as log, Either
from ..utility import remove_null_items_recursively


class ProjectDeployment:
    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config

        self._databricks_jobs = DatabricksJobsDeployment(project, project_config)
        self._airflow_jobs = AirflowJobDeployment(project, project_config)

        self._script_component = ScriptComponents(project, self._databricks_jobs, project_config)
        self._pipeline_configurations = PipelineConfigurations(project, self._databricks_jobs,
                                                               project_config)
        self._emr_pipeline_configurations = EMRPipelineConfigurations(project, self._airflow_jobs, project_config)

        self._dataproc_pipeline_configurations = DataprocPipelineConfigurations(project, self._airflow_jobs,
                                                                                project_config)

        self._dbt_component = DBTComponents(project, self._databricks_jobs, project_config)
        self._airflow_git_secrets = AirflowGitSecrets(project, self._airflow_jobs, project_config)

        self._pipelines = PipelineDeployment(project, self._databricks_jobs, self._airflow_jobs,
                                             project_config)

        # add gems Deployment.
        self._gems = GemsDeployment(project, project_config)

    def headers(self):
        summary = self._gems.summary() + self._script_component.summary() + \
                  self._dbt_component.summary() + self._airflow_git_secrets.summary() + \
                  self._pipeline_configurations.summary() + self._emr_pipeline_configurations.summary() + \
                  self._dataproc_pipeline_configurations.summary() + self._pipelines.summary() + \
                  self._databricks_jobs.summary() + self._airflow_jobs.summary()

        if len(summary) == 0:
            summary = ["No Job and pipelines to build"]

        summary_header = [StepMetadata("Summary", "Summary", Operation.Build, StepType.Summary)]

        header_components = (
            summary_header,
            self._gems.headers(),
            self._script_component.headers(),
            self._dbt_component.headers(),
            self._airflow_git_secrets.headers(),
            self._pipeline_configurations.headers(),
            self._emr_pipeline_configurations.headers(),
            self._dataproc_pipeline_configurations.headers(),
            self._pipelines.headers(),
            self._databricks_jobs.headers(),
            self._airflow_jobs.headers()
        )

        headers = sum(header_components, [])

        # 1st steps have to be summary
        for header in headers:
            log(step_id=header.id, step_metadata=header)

        for step in summary:
            log(message=step, step_id="Summary")

        log(step_status=Status.SUCCEEDED, step_id="Summary")

        return headers

    def build(self):
        self._pipelines.build_and_upload()

    def validate(self):
        pass

    def test(self, pipeline_name: List):
        pass

    def _deploy_gems(self):
        gems_responses = self._gems.deploy()

        if gems_responses is not None and any(response.is_left for response in gems_responses):
            raise Exception("Gems deployment failed.")

    def _deploy_scripts(self):
        script_responses = self._script_component.deploy()

        if script_responses is not None and any(response.is_left for response in script_responses):
            raise Exception("Script deployment failed.")

    def _deploy_dbt_components(self):
        dbt_responses = self._dbt_component.deploy()

        if dbt_responses is not None and any(response.is_left for response in dbt_responses):
            raise Exception("DBT deployment failed.")

    def _deploy_airflow_git_secrets(self):
        airflow_git_secrets_responses = self._airflow_git_secrets.deploy()

        if airflow_git_secrets_responses is not None and any(
                response.is_left for response in airflow_git_secrets_responses):
            raise Exception("Airflow git secrets deployment failed.")

    def _deploy_pipeline_configs(self):
        pipeline_config_responses = self._pipeline_configurations.deploy()

        if pipeline_config_responses is not None and any(response.is_left for response in pipeline_config_responses):
            raise Exception("Pipeline config deployment failed.")

    def _deploy_emr_pipeline_config(self):
        emr_pipeline_config_responses = self._emr_pipeline_configurations.deploy()

        if emr_pipeline_config_responses is not None and any(
                response.is_left for response in emr_pipeline_config_responses):
            raise Exception("EMR pipeline config deployment failed.")

    def _deploy_dataproc_pipeline_config(self):
        dataproc_pipeline_config_responses = self._dataproc_pipeline_configurations.deploy()

        if dataproc_pipeline_config_responses is not None and any(
                response.is_left for response in dataproc_pipeline_config_responses):
            raise Exception("Dataproc pipeline config deployment failed.")

    def _deploy_pipelines(self):
        pipeline_responses = self._pipelines.deploy()

        if pipeline_responses is not None and any(response.is_left for response in pipeline_responses):
            raise Exception("Pipeline deployment failed.")

    def _deploy_databricks_jobs(self) -> List[Either]:
        databricks_jobs_responses = self._databricks_jobs.deploy()

        return databricks_jobs_responses

    def _deploy_airflow_jobs(self) -> List[Either]:
        airflow_jobs_responses = self._airflow_jobs.deploy()

        return airflow_jobs_responses

    def deploy(self, job_ids):
        self._deploy_gems()
        self._deploy_scripts()
        self._deploy_dbt_components()
        self._deploy_airflow_git_secrets()
        self._deploy_pipeline_configs()
        self._deploy_emr_pipeline_config()
        self._deploy_dataproc_pipeline_config()
        self._deploy_pipelines()

        databricks_responses = self._deploy_databricks_jobs()
        airflow_responses = self._deploy_airflow_jobs()

        new_state_config = copy.deepcopy(self.project_config.jobs_state)

        # only jobs changes state_config.

        new_state_config.update_state(databricks_responses + airflow_responses)
        path = os.path.join(os.getcwd(), NEW_JOB_STATE_FILE)
        yaml_str = yaml.dump(remove_null_items_recursively(new_state_config.dict()))

        with open(path, 'w') as file:
            file.write(yaml_str)

        # Only fail when there is a failure in jobs deployment.
        if databricks_responses is not None and any(response.is_left for response in databricks_responses):
            for response in databricks_responses:
                if response.is_left:
                    print(response.left)
            raise Exception("Databricks jobs deployment failed.")

        if airflow_responses is not None and any(response.is_left for response in airflow_responses):
            raise Exception("Airflow jobs deployment failed.")

        return databricks_responses + airflow_responses
