import json
from typing import List

from src.pbt.v2.constants import JOBS
from src.pbt.v2.project.components.airflow_jobs import AirflowJobs, AirflowGitSecrets
from src.pbt.v2.project.components.databricks_jobs import DatabricksJobs, ScriptComponents, PipelineConfigurations, \
    DBTComponents
from src.pbt.v2.project.components.pipeline import Pipelines
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.project_models import StepMetadata
from src.pbt.v2.state_config import StateConfigAndDBTokens


def should_include(key):
    ## these are in config objects but we need to remove them.
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


class Project:
    def __init__(self, project_parser: ProjectParser, state_config_and_db_tokens: StateConfigAndDBTokens):
        self.project_parser = project_parser

        self._databricks_jobs = DatabricksJobs(project_parser, state_config_and_db_tokens)
        self._airflow_jobs = AirflowJobs(project_parser, state_config_and_db_tokens)

        self._script_component = ScriptComponents(project_parser, self._databricks_jobs, state_config_and_db_tokens)
        self._pipeline_configurations = PipelineConfigurations(project_parser, self._databricks_jobs,
                                                               state_config_and_db_tokens)
        self._dbt_component = DBTComponents(project_parser, self._databricks_jobs, state_config_and_db_tokens)
        self._airflow_git_secrets = AirflowGitSecrets(project_parser, self._airflow_jobs, state_config_and_db_tokens)

        self._pipelines = Pipelines(project_parser, self._databricks_jobs, self._airflow_jobs,
                                    state_config_and_db_tokens)

    def should_include(self, key):
        ## these are in config objects but we need to remove them.
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

    def headers(self):
        headers = self._script_component.headers() + \
                  self._dbt_component.headers() + \
                  self._airflow_git_secrets.headers() + \
                  self._pipeline_configurations.headers() + \
                  self._pipelines.headers() + \
                  self._databricks_jobs.headers() + \
                  self._airflow_jobs.headers()

        for head in headers:
            print(json.dumps(head, default=to_dict_recursive, indent=4))

    def build(self, pipeline_ids):
        self._pipelines.build_and_upload(pipeline_ids)

    def validate(self):
        pass

    def test(self, pipeline_name: List):
        pass

    def deploy(self):
        self._script_component.deploy()
        self._dbt_component.deploy()
        self._pipeline_configurations.deploy()
        self._pipelines.deploy()

        self._airflow_git_secrets.deploy()

        self._airflow_jobs.deploy()
        self._databricks_jobs.deploy()
