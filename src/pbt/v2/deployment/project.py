import os
from typing import List
import copy

import yaml

from ..constants import NEW_DEPLOYMENT_STATE_FILE
from ..deployment.jobs.airflow import AirflowJobDeployment, AirflowGitSecrets, EMRPipelineConfigurations
from ..deployment.jobs.databricks import DatabricksJobsDeployment, ScriptComponents, PipelineConfigurations, \
    DBTComponents
from ..deployment.pipeline import PipelineDeployment
from ..entities.project import Project
from ..project_config import ProjectConfig
from ..project_models import StepMetadata, Operation, StepType, Status
from ..utility import remove_null_items_recursively

from ..utility import custom_print as log


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

        self._dbt_component = DBTComponents(project, self._databricks_jobs, project_config)
        self._airflow_git_secrets = AirflowGitSecrets(project, self._airflow_jobs, project_config)

        self._pipelines = PipelineDeployment(project, self._databricks_jobs, self._airflow_jobs,
                                             project_config)

        # add gems Deployment.
        # self._gems = GemsDeployment(project, project_config)

    def headers(self):
        summary = self._script_component.summary() + \
                  self._dbt_component.summary() + self._airflow_git_secrets.summary() + \
                  self._pipeline_configurations.summary() + self._emr_pipeline_configurations.summary() + \
                  self._pipelines.summary() + \
                  self._databricks_jobs.summary() + self._airflow_jobs.summary()

        if len(summary) == 0:
            summary = ["No Job and pipelines to build"]

        summary_header = [StepMetadata("Summary", "Summary", Operation.Build, StepType.Summary)]

        header_components = (
            summary_header,
            self._script_component.headers(),
            self._dbt_component.headers(),
            self._airflow_git_secrets.headers(),
            self._pipeline_configurations.headers(),
            self._emr_pipeline_configurations.headers(),
            self._pipelines.headers(),
            self._databricks_jobs.headers(),
            self._airflow_jobs.headers()
        )

        headers = sum(header_components, [])

        for step in summary:
            log(message=step, step_id="Summary")

        log(step_status=Status.SUCCEEDED, step_id="Summary")

        return headers

    def build(self, pipeline_ids):
        self._pipelines.build_and_upload(pipeline_ids)

    def validate(self):
        pass

    def test(self, pipeline_name: List):
        pass

    def deploy(self, job_ids):
        # pipelines first,
        # then other components

        script_responses = self._script_component.deploy()

        if script_responses is not None and any(response.is_left for response in script_responses):
            raise Exception("Script deployment failed.")

        dbt_component_responses = self._dbt_component.deploy()

        if dbt_component_responses is not None and any(response.is_left for response in dbt_component_responses):
            raise Exception("DBT component deployment failed.")

        airflow_git_secrets = self._airflow_git_secrets.deploy()

        if airflow_git_secrets is not None and any(response.is_left for response in airflow_git_secrets):
            raise Exception("Airflow git Secrets deployment failed.")

        # if dbt_components_responses is not None and any(response.is_left for response in dbt_components_responses):
        #     raise Exception("DBT deployment failed.")

        # pipeline_configurations_responses = self._pipeline_configurations.deploy()
        pipeline_config_responses = self._pipeline_configurations.deploy()

        if pipeline_config_responses is not None and any(response.is_left for response in pipeline_config_responses):
            raise Exception("Pipeline configuration response failed.")

        emr_pipeline_config_responses = self._emr_pipeline_configurations.deploy()

        if emr_pipeline_config_responses is not None and any(
                response.is_left for response in emr_pipeline_config_responses):
            raise Exception("EMR Pipeline configuration response failed.")
        # pipeline_responses = self._pipelines.deploy()

        pipeline_responses = self._pipelines.deploy()

        if pipeline_responses is not None and any(response.is_left for response in pipeline_responses):
            raise Exception("Pipelines deployment failed.")

        new_state_config = copy.deepcopy(self.project_config.deployment_state)

        # only jobs changes state_config.

        databricks_jobs_responses = self._databricks_jobs.deploy()
        airflow_jobs_responses = self._airflow_jobs.deploy()

        new_state_config.update_state(databricks_jobs_responses + airflow_jobs_responses)
        path = os.path.join(os.getcwd(), NEW_DEPLOYMENT_STATE_FILE)
        yaml_str = yaml.dump(remove_null_items_recursively(new_state_config.dict()))

        with open(path, 'w') as file:
            file.write(yaml_str)

        # Only fail when there is a failure in jobs deployment.
        if databricks_jobs_responses is not None and any(response.is_left for response in databricks_jobs_responses):
            for response in databricks_jobs_responses:
                if response.is_left:
                    print(response.left)
            raise Exception("Databricks jobs deployment failed.")

        if airflow_jobs_responses is not None and any(response.is_left for response in airflow_jobs_responses):
            raise Exception("Airflow jobs deployment failed.")

        return databricks_jobs_responses + airflow_jobs_responses
