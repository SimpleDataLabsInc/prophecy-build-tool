import os
from typing import List
import copy

from pydantic_yaml import to_yaml_file

from ..deployment.airflow_jobs import AirflowJobDeployment, AirflowGitSecrets
from ..deployment.databricks_jobs import DatabricksJobsDeployment, ScriptComponents, PipelineConfigurations, \
    DBTComponents
from ..deployment.pipeline import PipelineDeployment
from ..entities.project import Project
from ..project_config import ProjectConfig


def should_include(key):
    # these are in config objects but we need to remove them.
    return key not in ['spark', 'prophecy_spark']


def to_dict_recursive(obj):
    if isinstance(obj, (list, tuple)):
        return [to_dict_recursive(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: to_dict_recursive(value) for key, value in obj.items() if should_include(key)}
    elif hasattr(obj, '__dict__'):
        return to_dict_recursive({key: value for key, value in obj.__dict__.items() if should_include(key)})
    elif hasattr(obj, '__slots__'):
        return to_dict_recursive(
            {slot: getattr(obj, slot) for slot in obj.__slots__ if should_include(slot)})
    else:
        return obj


class ProjectDeployment:
    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config

        self._databricks_jobs = DatabricksJobsDeployment(project, project_config)
        self._airflow_jobs = AirflowJobDeployment(project, project_config)

        self._script_component = ScriptComponents(project, self._databricks_jobs, project_config)
        self._pipeline_configurations = PipelineConfigurations(project, self._databricks_jobs,
                                                               project_config)
        self._dbt_component = DBTComponents(project, self._databricks_jobs, project_config)
        self._airflow_git_secrets = AirflowGitSecrets(project, self._airflow_jobs, project_config)

        self._pipelines = PipelineDeployment(project, self._databricks_jobs, self._airflow_jobs,
                                             project_config)

    def headers(self):
        headers = self._script_component.headers() + \
                  self._dbt_component.headers() + \
                  self._airflow_git_secrets.headers() + \
                  self._pipeline_configurations.headers() + \
                  self._pipelines.headers() + \
                  self._databricks_jobs.headers() + \
                  self._airflow_jobs.headers()

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

        pipeline_responses = self._pipelines.deploy()

        if pipeline_responses is not None and any(response.is_left for response in pipeline_responses):
            raise Exception("Pipelines deployment failed.")

        script_responses = self._script_component.deploy()

        if script_responses is not None and any(response.is_left for response in script_responses):
            raise Exception("Script deployment failed.")

        dbt_components_responses = self._dbt_component.deploy()

        if dbt_components_responses is not None and any(response.is_left for response in dbt_components_responses):
            raise Exception("DBT deployment failed.")

        pipeline_configurations_responses = self._pipeline_configurations.deploy()

        if pipeline_configurations_responses is not None and any(
                response.is_left for response in pipeline_configurations_responses):
            raise Exception("Pipeline configuration deployment failed.")

        airflow_git_secrets_responses = self._airflow_git_secrets.deploy()

        if airflow_git_secrets_responses is not None and any(
                response.is_left for response in airflow_git_secrets_responses):
            raise Exception("Airflow git secrets deployment failed.")

        new_state_config = copy.deepcopy(self.project_config.state_config)

        # only jobs changes state_config.

        databricks_jobs_responses = self._databricks_jobs.deploy()
        airflow_jobs_responses = self._airflow_jobs.deploy()

        new_state_config.update_state(databricks_jobs_responses + airflow_jobs_responses)
        path = os.path.join(os.getcwd(), "new_state_config.yml")
        print(f"new_state_config {path}")
        to_yaml_file(path, new_state_config)
        return databricks_jobs_responses + airflow_jobs_responses
