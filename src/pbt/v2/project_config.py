import os
from typing import List, Optional, Dict
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

from src.pbt.v2.constants import DBFS_BASE_PATH


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


class DBFabricInfo(BaseModel):
    url: str
    token: str


class Content(BaseModel):
    provider_type: Optional[str]  # composer/mwaa/Prophecy
    composer_info: Optional[ComposerInfo] = None
    mwaa_info: Optional[MwaaInfo] = None


class FabricInfo(BaseModel):
    id: str
    name: str
    type: str  # databricks/airflow/livy/emr.
    content: Optional[Content] = None
    db_info: Optional[DBFabricInfo] = None


class JobInfo(BaseModel):
    name: str
    type: str
    scheduler_job_id: str
    fabric_id: str
    id: str
    is_paused: Optional[bool] = False


class ProjectAndGitTokens(BaseModel):
    project_id: str
    git_token: str = ""
    language: str = ""


class StateConfig(BaseModel):
    name: str
    language: str
    description: str
    version: str
    fabrics: List[FabricInfo] = []
    jobs: List[JobInfo] = []
    project_git_tokens: List[ProjectAndGitTokens] = []

    def contains_jobs(self, job_id: str, fabric_uid: str) -> bool:
        return any(job.id == job_id and job.fabric_id == fabric_uid for job in self.jobs)

    def get_jobs(self, job_id: str) -> List[JobInfo]:
        return [job for job in self.jobs if job.id == job_id]

    def get_job(self, job_id: str, fabric_id: str) -> Optional[JobInfo]:
        return next((job for job in self.jobs if job.id == job_id and job.fabric_id == fabric_id), None)

    def get_databricks_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs if job.type is 'databricks']

    @property
    def get_airflow_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs if job.type is not 'databricks']

    def contains_fabric(self, fabric_id: str) -> bool:
        return any(fabric.id == fabric_id for fabric in self.fabrics)

    def get_fabric(self, fabric_id: str) -> Optional[FabricInfo]:
        return next((fabric for fabric in self.fabrics if fabric.id == fabric_id), None)

    def git_token_for_project(self, project_id: str) -> Optional[str]:
        return next((project.git_token for project in self.project_git_tokens if project.project_id == project_id),
                    None)

    def is_fabric_prophecy_managed(self, fabric_id: str) -> bool:
        if fabric_id is not None and self.get_fabric(fabric_id) is not None:
            fabric_info = self.get_fabric(fabric_id)
            return fabric_info.type == 'airflow' and fabric_info.content.provider_type == 'prophecy'

        return False

    def db_fabrics(self):
        return [fabric.id for fabric in self.fabrics if fabric.type == 'databricks']

    @classmethod
    def empty_state_config(cls):
        return cls(name="", language="", description="", version="")


class NexusConfig(BaseModel):
    url: str
    username: str
    password: str
    repository:str


class SystemConfig(BaseModel):
    customer_name: Optional[str] = 'dev'
    control_plane_name: Optional[str] = 'execution'
    runtime_mode: Optional[str] = 'test'  # maybe an enum.
    prophecy_salt: Optional[str] = 'execution'
    nexus_config: Optional[NexusConfig]

    def get_base_path(self):
        return f'{DBFS_BASE_PATH}/{self.customer_name}/{self.control_plane_name}'

    @staticmethod
    def empty_system_config():
        return SystemConfig()


# maybe this can grow to read env. variables as well.
class ProjectConfig:
    def __init__(self, state_config_path: str, system_config_path: str):
        self.state_config_path = state_config_path
        self.system_config_path = system_config_path

        self.state_config = None
        self.system_config = None

        self._load_project_config()

    def _load_project_config(self):
        if self.state_config_path is not None and len(self.state_config_path) > 0:
            with open(self.state_config_path, "r") as state_config:
                data = state_config.read()
                self.state_config = parse_yaml_raw_as(StateConfig, data)
        else:
            self.state_config = StateConfig.empty_state_config()

        if self.system_config_path is not None and len(self.system_config_path) > 0:
            with open(self.system_config_path, "r") as system_config:
                data = system_config.read()
                self.system_config = parse_yaml_raw_as(SystemConfig, data)
        else:
            self.system_config = SystemConfig.empty_system_config()
