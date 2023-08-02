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
    def __init__(self, project: ProjectParser, state_config_and_db_tokens: StateConfigAndDBTokens):
        self.project = project
        self.jobs = self.project.project[JOBS]
        self.__databricks_jobs = DatabricksJobs(project, state_config_and_db_tokens)
        self.__airflow_jobs = AirflowJobs(project, state_config_and_db_tokens)
        self.__script_component = ScriptComponents(project, self.__databricks_jobs, state_config_and_db_tokens)
        self.__pipeline_configurations = PipelineConfigurations(project, self.__databricks_jobs,
                                                                state_config_and_db_tokens)
        self.__pipelines = Pipelines(project, self.__databricks_jobs, self.__airflow_jobs, state_config_and_db_tokens)
        self.__dbt_component = DBTComponents(project, self.__databricks_jobs, state_config_and_db_tokens)
        self.__airflow_git_secrets = AirflowGitSecrets(project, self.__airflow_jobs, state_config_and_db_tokens)

    def should_include(self,  key):
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
        headers = self.__script_component.headers() + \
                  self.__dbt_component.headers() + \
                  self.__airflow_git_secrets.headers() + \
                  self.__pipeline_configurations.headers() + \
                  self.__pipelines.headers() + \
                  self.__databricks_jobs.headers() + \
                  self.__airflow_jobs.headers()

        for head in headers:
            print(json.dumps(head, default=to_dict_recursive, indent=4))

    def build(self):
        self.__pipelines.build()

    def deploy(self):
        self.__script_component.deploy()
        self.__dbt_component.deploy()
        self.__pipeline_configurations.deploy()
        self.__pipelines.deploy()

        self.__airflow_git_secrets.deploy()

        self.__airflow_jobs.deploy()
        self.__databricks_jobs.deploy()
