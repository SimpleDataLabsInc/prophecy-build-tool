import enum
from typing import List, Optional
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

from .constants import PROPHECY_ARTIFACTS, DBFS_FILE_STORE
from .deployment import OperationType, JobInfoAndOperation
from .utility import Either


class SchedulerType(enum.Enum):
    Composer = "Composer"
    MWAA = "MWAA"
    Databricks = "Databricks"
    Prophecy = "Prophecy"
    EMR = "EMR"

    @staticmethod
    def from_fabric_provider(provider_type: str):
        # Convert the provider_type to its equivalent Enum if exists, otherwise None
        return SchedulerType[provider_type]


class FabricType(enum.Enum):
    Spark = "Spark"
    Sql = "Sql"
    Airflow = "Airflow"


class FabricProviderType(enum.Enum):
    Composer = "Composer"
    MWAA = "MWAA"
    Databricks = "Databricks"
    Prophecy = "Prophecy"
    EMR = "EMR"


class RuntimeMode(enum.Enum):
    Regular = "Regular"
    Partial = "Partial"
    RegularWithTests = "RegularWithTests"
    PartialWithTests = "PartialWithTests"
    Test = "Test"


class EMRInfo(BaseModel):
    region: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None

    def bare_bucket(self):
        return self.bucket.replace("s3://", "")


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
    type: Optional[FabricType]  # sql/ databricks/ airflow
    provider: Optional[FabricProviderType]  # composer/mwaa/prophecy/databricks
    composer: Optional[ComposerInfo] = None
    mwaa: Optional[MwaaInfo] = None
    databricks: Optional[DatabricksInfo] = None
    emr: Optional[EMRInfo] = None


class JobInfo(BaseModel):
    name: str
    type: SchedulerType
    external_job_id: str
    fabric_id: str
    id: str
    is_paused: Optional[bool] = False
    skip_processing: Optional[bool] = False  # this is useful in case when we deploy from older release tags.
    release_tag: Optional[str] = None

    def with_release_tag(self, release_tag):
        self.release_tag = release_tag

    @staticmethod
    def create_db_job(name: str, id: str, fabric_id: str, external_job_id: str, release_tag: str,
                      is_paused: bool = False):
        return JobInfo(name=name, type=SchedulerType.Databricks, id=id, fabric_id=fabric_id,
                       external_job_id=external_job_id,
                       release_tag=release_tag, is_paused=is_paused)

    @staticmethod
    def create_airflow_job(name: str, id: str, fabric_id: str, external_job_id: str, release_tag: str,
                           is_paused: bool = False, fabric_provider_type: str = ""):
        return JobInfo(name=name, type=SchedulerType.from_fabric_provider(fabric_provider_type), id=id,
                       fabric_id=fabric_id,
                       external_job_id=external_job_id,
                       release_tag=release_tag, is_paused=is_paused)

    def is_exactly_same_as(self, job_response):
        return self.external_job_id == job_response.external_job_id and self.fabric_id == job_response.fabric_id and \
            self.id == job_response.id and self.name == job_response.name and self.type == job_response.type

    def pause(self, flag: bool):
        self.is_paused = flag


class ProjectAndGitTokens(BaseModel):
    project_id: str
    git_token: str = ""


class DeploymentState(BaseModel):
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

    def is_fabric_db_fabric(self, fabric_id: str) -> bool:
        return any((fabric for fabric in self.fabrics if
                    fabric.id == fabric_id and fabric.provider == FabricProviderType.Databricks))

    def is_fabric_emr_fabric(self, fabric_id: str) -> bool:
        return any((fabric for fabric in self.fabrics if
                    fabric.id == fabric_id and fabric.provider == FabricProviderType.EMR))

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
            return fabric_info.type == FabricType.Airflow and fabric_info.provider == FabricProviderType.Prophecy

        return False

    def db_fabrics(self):
        return [fabric.id for fabric in self.fabrics if
                (fabric.type == FabricType.Spark and fabric.provider == FabricProviderType.Databricks) or
                (fabric.type == FabricType.Sql and fabric.provider == FabricProviderType.Databricks)]

    def emr_fabrics(self) -> List[FabricInfo]:
        return [fabric for fabric in self.fabrics if
                (fabric.type == FabricType.Spark and fabric.provider == FabricProviderType.EMR)]

    def update_state(self, jobs_and_operation_types: List[Either]):
        jobs_and_operation_types_filtered: List[JobInfoAndOperation] = [job_and_operation_type.right for
                                                                        job_and_operation_type
                                                                        in jobs_and_operation_types if
                                                                        job_and_operation_type.is_right]

        for job_and_operation_type in jobs_and_operation_types_filtered:
            job_response = job_and_operation_type.job_info
            if job_and_operation_type.operation_type == OperationType.DELETED:
                self.jobs = [job for job in self.jobs if
                             not (job.id == job_response.id and job.fabric_id == job_response.fabric_id)]

        for job_and_operation_type in jobs_and_operation_types_filtered:
            job_response = job_and_operation_type.job_info
            if job_and_operation_type.operation_type == OperationType.REFRESH:
                new_jobs = [job for job in self.jobs if
                            not (job.id == job_response.id and job.fabric_id == job_response.fabric_id)]
                new_jobs.append(job_response)
                self.jobs = new_jobs

        for job_and_operation_type in jobs_and_operation_types_filtered:
            job_response = job_and_operation_type.job_info
            if job_and_operation_type.operation_type == OperationType.CREATED:
                matching_job = next((job for job in self.jobs if job.id == job_response.id), None)

                new_jobs = [job for job in self.jobs if
                            not (job.id == job_response.id and job.fabric_id == job_response.fabric_id)]

                if matching_job is not None:
                    # super important to preserve the release tag.
                    job_response.with_release_tag(matching_job.release_tag)

                new_jobs.append(job_response)

                self.jobs = new_jobs


class NexusConfig(BaseModel):
    url: str
    username: str
    password: str
    repository: str


class SystemConfig(BaseModel):
    customer_name: Optional[str] = 'dev'
    control_plane_name: Optional[str] = 'execution'
    runtime_mode: Optional[RuntimeMode] = RuntimeMode.Regular  # maybe an enum.
    prophecy_salt: Optional[str] = 'execution'
    nexus: Optional[NexusConfig] = None

    def get_dbfs_base_path(self):
        return f'{DBFS_FILE_STORE}/{PROPHECY_ARTIFACTS}/{self.customer_name}/{self.control_plane_name}'

    def get_s3_base_path(self):
        return f"{self.customer_name}/{self.control_plane_name}"

    @staticmethod
    def empty():
        return SystemConfig()


class ProjectConfig:
    def __init__(self, deployment_state: DeploymentState, system_config: SystemConfig):

        self.deployment_state = deployment_state
        self.system_config = system_config

    @staticmethod
    def from_path(deployment_state_path: str, system_config_path: str):
        def load_deployment_state():
            if deployment_state_path is not None and len(deployment_state_path) > 0:
                with open(deployment_state_path, "r") as deployment_state:
                    data = deployment_state.read()
                    return parse_yaml_raw_as(DeploymentState, data)
            else:
                raise Exception("State config path is not provided")

        def load_system_config():
            if system_config_path is not None and len(system_config_path) > 0:
                with open(system_config_path, "r") as system_config:
                    data = system_config.read()
                    return parse_yaml_raw_as(SystemConfig, data)
            else:
                raise Exception("System config path is not provided")

        return ProjectConfig(load_deployment_state(), load_system_config())
