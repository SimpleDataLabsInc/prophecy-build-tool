import os
from typing import List, Optional, Dict
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

from src.pbt.v2.constants import BASE_PATH


# todo explore https://docs.pydantic.dev/latest/usage/serialization/#subclasses-of-standard-types
# explore https://docs.pydantic.dev/latest/usage/models/#generic-models


class ComposerInfo(BaseModel):
    key_json: str
    version: str
    location: str
    project_id: str
    airflow_url: str
    dag_location: str
    client_id: Optional[str] = None


class MwaaInfo(BaseModel):
    region: str
    version: str
    access_key: str
    secret_key: str
    airflow_url: str
    dag_location: str
    environment_name: str


class Content(BaseModel):
    provider_type: Optional[str]
    composer_info: Optional[ComposerInfo] = None
    mwaa_info: Optional[MwaaInfo] = None


class FabricsInfo(BaseModel):
    id: str
    name: str
    url: str
    provider_type: str
    is_spark: bool
    is_sql: bool
    is_airflow: bool
    content: Optional[Content] = None


class JobsInfo(BaseModel):
    name: str
    type: str
    scheduler_job_id: str
    fabric_id: str
    id: str
    is_paused: bool


class ProjectAndGitTokens(BaseModel):
    project_id: str
    git_token: str = ""
    language: str = ""


class StateConfig(BaseModel):
    name: str
    language: str
    description: str
    version: str
    fabrics: List[FabricsInfo] = []
    jobs: List[JobsInfo] = []
    project_git_tokens: List[ProjectAndGitTokens] = []

    def contains_jobs(self, job_id: str, fabric_uid: str) -> bool:
        return any(job.id == job_id and job.fabric_id == fabric_uid for job in self.jobs)

    def get_jobs(self, job_id: str) -> List[JobsInfo]:
        return [job for job in self.jobs if job.id == job_id]

    def get_job(self, job_id: str, fabric_id: str) -> Optional[JobsInfo]:
        return next((job for job in self.jobs if job.id == job_id and job.fabric_id == fabric_id), None)

    def get_databricks_jobs(self) -> List[JobsInfo]:
        return [job for job in self.jobs if job.type is 'Databricks']

    @property
    def get_airflow_jobs(self) -> List[JobsInfo]:
        return [job for job in self.jobs if job.type is not 'Databricks']

    def contains_fabric(self, fabric_id: str) -> bool:
        return any(fabric.id == fabric_id for fabric in self.fabrics)

    def get_fabric(self, fabric_id: str) -> Optional[FabricsInfo]:
        return next((fabric for fabric in self.fabrics if fabric.id == fabric_id), None)

    def git_token_for_project(self, project_id: str) -> Optional[str]:
        return next((project.git_token for project in self.project_git_tokens if project.project_id == project_id),
                    None)

    def is_fabric_prophecy_managed(self, fabric_id: str) -> bool:
        if fabric_id is not None and self.get_fabric(fabric_id) is not None:
            fabric_info = self.get_fabric(fabric_id)
            return fabric_info.provider_type == 'Prophecy' and fabric_info.is_airflow

        return False

    @classmethod
    def empty_state_config(cls):
        return cls(name="", language="", description="", version="")


class StateConfigAndDBTokens:
    def __init__(self, path: str, db_tokens: Dict[str, str]):
        self.path = path
        self.state_config = None
        self._load_state_config()
        self.db_tokens = db_tokens

    def _load_state_config(self):
        if self.path is not None:
            with open(self.path, "r") as state_config:
                data = state_config.read()
                self.state_config = parse_yaml_raw_as(StateConfig, data)
        else:
            self.state_config = StateConfig.empty_state_config()

    @staticmethod
    def load_from_cli():
        os.getenv()

    #todo fill this up.
    def get_base_path(self):
        return f'{BASE_PATH}/{customer_name}/{control_plane_name}'
