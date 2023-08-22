import enum
from typing import List, Optional
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

from .constants import DBFS_BASE_PATH
from .deployment import OperationType
from .utility import Either


class SchedulerType(enum.Enum):
    Databricks = "Databricks"
    Airflow = "Airflow"
    Prophecy = "Prophecy"


class FabricType(enum.Enum):
    Spark = "spark"
    Sql = "sql"
    Airflow = "airflow"


class FabricProviderType(enum.Enum):
    Composer = "composer"
    Mwaa = "mwaa"
    Databricks = "databricks"
    Prophecy = "prophecy"


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


class DatabricksInfo(BaseModel):
    url: str
    token: str


class FabricInfo(BaseModel):
    id: str
    name: str
    type: Optional[str]  # sql/ databricks/ airflow
    provider: Optional[str]  # composer/mwaa/prophecy/databricks
    composer: Optional[ComposerInfo] = None
    mwaa: Optional[MwaaInfo] = None
    databricks: Optional[DatabricksInfo] = None


class JobInfo(BaseModel):
    name: str
    type: str
    external_job_id: str
    fabric_id: str
    id: str
    is_paused: Optional[bool] = False
    skip_processing: Optional[bool] = False  # this is useful in case when we deploy from older release tags.
    release_tag: Optional[str] = None

    @staticmethod
    def create_db_job(name: str, id: str, fabric_id: str, external_job_id: str, release_tag: str,
                      is_paused: bool = False):
        return JobInfo(name=name, type=SchedulerType.Airflow, id=id, fabric_id=fabric_id,
                       external_job_id=external_job_id,
                       release_tag=release_tag, is_paused=is_paused)

    @staticmethod
    def create_airflow_job(name: str, id: str, fabric_id: str, external_job_id: str, release_tag: str,
                           is_paused: bool = False):
        return JobInfo(name=name, type=SchedulerType.Airflow, id=id, fabric_id=fabric_id,
                       external_job_id=external_job_id,
                       release_tag=release_tag, is_paused=is_paused)


class ProjectAndGitTokens(BaseModel):
    project_id: str
    git_token: str = ""


class StateConfig(BaseModel):
    name: str
    language: str
    description: str
    version: str
    release_tag: str  # current release tag.
    fabrics: List[FabricInfo] = []
    jobs: List[JobInfo] = []
    project_git_tokens: List[ProjectAndGitTokens] = []

    def contains_jobs(self, job_id: str, fabric_uid: str) -> bool:
        return any(
            job.id == job_id and job.fabric_id == fabric_uid and job.skip_processing is False for job in self.jobs)

    def get_jobs(self, job_id: str) -> List[JobInfo]:
        return [job for job in self.jobs if job.id == job_id and job.skip_processing is False]

    def get_job(self, job_id: str, fabric_id: str) -> Optional[JobInfo]:
        return next(
            (job for job in self.jobs if
             job.id == job_id and job.fabric_id == fabric_id and job.skip_processing is False),
            None)

    @property
    def get_databricks_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs if job.type == SchedulerType.Databricks and job.skip_processing is False]

    @property
    def get_airflow_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs if job.type != SchedulerType.Databricks and job.skip_processing is False]

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
            return fabric_info.type == FabricType.Airflow and fabric_info.provider == FabricProviderType.Databricks

        return False

    def db_fabrics(self):
        return [fabric.id for fabric in self.fabrics if
                (fabric.type == FabricType.Spark and fabric.provider == FabricProviderType.Databricks) or
                (fabric.type == FabricType.Sql and fabric.provider == FabricProviderType.Databricks)]

    def update_state(self, jobs_and_operation_types: List[Either]):
        for jobsAndOperationType in jobs_and_operation_types:
            if jobsAndOperationType.is_right:
                job = jobsAndOperationType.right.job

                if jobsAndOperationType.right.operation_type == OperationType.CREATED:
                    self.jobs.append(job)

                if jobsAndOperationType.right.operation_type == OperationType.DELETED:
                    self.jobs = [job if job.id == job.id and job.fabric_id == job.fabric_id else job for job in
                                 self.jobs]

                # for others we don't care.


class NexusConfig(BaseModel):
    url: str
    username: str
    password: str
    repository: str


class SystemConfig(BaseModel):
    customer_name: Optional[str] = 'dev'
    control_plane_name: Optional[str] = 'execution'
    runtime_mode: Optional[str] = 'test'  # maybe an enum.
    prophecy_salt: Optional[str] = 'execution'
    nexus: Optional[NexusConfig] = None

    def get_base_path(self):
        return f'{DBFS_BASE_PATH}/{self.customer_name}/{self.control_plane_name}'

    @staticmethod
    def empty():
        return SystemConfig()


class ProjectConfig:
    def __init__(self, state_config: StateConfig, system_config: SystemConfig):

        self.state_config = state_config
        self.system_config = system_config

    @staticmethod
    def from_path(state_config_path: str, system_config_path: str):
        def load_state_config():
            if state_config_path is not None and len(state_config_path) > 0:
                with open(state_config_path, "r") as state_config:
                    data = state_config.read()
                    return parse_yaml_raw_as(StateConfig, data)
            else:
                raise Exception("State config path is not provided")

        def load_system_config():
            if system_config_path is not None and len(system_config_path) > 0:
                with open(system_config_path, "r") as system_config:
                    data = system_config.read()
                    return parse_yaml_raw_as(SystemConfig, data)
            else:
                raise Exception("System config path is not provided")

        return ProjectConfig(load_state_config(), load_system_config())
