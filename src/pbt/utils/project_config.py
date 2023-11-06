import enum
import os
from typing import List, Optional

from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

from .constants import PROPHECY_ARTIFACTS, DBFS_FILE_STORE
from .exceptions import ConfigFileNotFoundException
from .project_models import Status
from ..deployment import OperationType, JobInfoAndOperation
from ..utility import Either, custom_print as log


class SchedulerType(enum.Enum):
    Composer = "Composer"
    MWAA = "MWAA"
    Databricks = "Databricks"
    Prophecy = "Prophecy"
    EMR = "EMR"

    @staticmethod
    def from_type(provider_type: str):
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
    Dataproc = "Dataproc"


class DeploymentMode(enum.Enum):
    FullProject = "FullProject"
    SelectiveJob = "SelectiveJob"


class BucketAndPathExtractor:
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix

    def path(self):
        return self.bucket.replace(self.prefix, "").split("/")

    def bare_bucket(self):
        return self.path()[0]

    def bare_path_prefix(self):
        return "/".join(self.path()[1:])


#
class EMRInfo(BaseModel):
    region: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None

    def bucket_path_extractor(self):
        return BucketAndPathExtractor(self.bucket, "s3://")

    def bare_bucket(self):
        return self.bucket_path_extractor().bare_bucket()

    def bare_path_prefix(self):
        return self.bucket_path_extractor().bare_path_prefix()


class DataprocInfo(BaseModel):
    bucket: str
    project_id: str
    key_json: str
    location: str

    def bucket_path_extractor(self):
        return BucketAndPathExtractor(self.bucket, "gs://")

    def bare_bucket(self):
        return self.bucket_path_extractor().bare_bucket()

    def bare_path_prefix(self):
        return self.bucket_path_extractor().bare_path_prefix()


class ComposerInfo(BaseModel):
    key_json: str
    version: str
    location: Optional[str] = None
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
    provider: Optional[FabricProviderType]  # composer/mwaa/prophecy/databricks/dataproc
    composer: Optional[ComposerInfo] = None
    mwaa: Optional[MwaaInfo] = None
    databricks: Optional[DatabricksInfo] = None
    emr: Optional[EMRInfo] = None
    dataproc: Optional[DataprocInfo] = None


class JobInfo(BaseModel):
    name: str
    type: SchedulerType
    external_job_id: str
    fabric_id: str
    id: str
    is_paused: Optional[bool] = False
    skip_processing: Optional[bool] = False  # this is useful in case when we deploy from older release tags.
    release_tag: Optional[str] = None

    def update_release_tag(self, release_tag):
        self.release_tag = release_tag

    def is_job_same_as(self, job_info) -> bool:
        return self.external_job_id == job_info.external_job_id and self.fabric_id == job_info.fabric_id and \
            self.id == job_info.id and self.name == job_info.name and self.type == job_info.type

    def pause(self, flag: bool):
        self.is_paused = flag

    @staticmethod
    def create_job(name: str, id: str, fabric_id: str, external_job_id: str, release_tag: str,
                   is_paused: bool = False, fabric_provider_type: str = "Databricks"):
        return JobInfo(name=name,
                       type=SchedulerType.from_type(fabric_provider_type),
                       id=id,
                       fabric_id=fabric_id,
                       external_job_id=external_job_id,
                       release_tag=release_tag,
                       is_paused=is_paused)


class ProjectAndGitTokens(BaseModel):
    project_id: str
    git_token: str = ""


class FabricConfig(BaseModel):
    fabrics: List[FabricInfo] = []
    project_git_tokens: List[ProjectAndGitTokens] = []

    def get_fabric(self, fabric_id: str) -> Optional[FabricInfo]:
        return next((fabric for fabric in self.fabrics if fabric.id == fabric_id), None)

    def does_fabric_exist(self, fabric_id: str) -> bool:
        return self.get_fabric(fabric_id) is not None

    def git_token_for_project(self, project_id: str) -> Optional[str]:
        return next((entity.git_token for entity in self.project_git_tokens if entity.project_id == project_id),
                    None)

    def is_prophecy_managed(self, fabric_id: str) -> bool:
        if self.get_fabric(fabric_id) is not None:
            fabric_info = self.get_fabric(fabric_id)
            return fabric_info.type == FabricType.Airflow and fabric_info.provider == FabricProviderType.Prophecy

        return False

    def is_fabric_emr_fabric(self, fabric_id: str) -> bool:
        return any((fabric for fabric in self.fabrics if
                    fabric.id == fabric_id and fabric.provider == FabricProviderType.EMR))

    def db_fabrics(self) -> List[str]:
        return [fabric.id for fabric in self.fabrics if fabric.provider == FabricProviderType.Databricks]

    def is_databricks_fabric(self, fabric_id: str) -> bool:
        return fabric_id in self.db_fabrics()

    def emr_fabrics(self) -> List[FabricInfo]:
        return [fabric for fabric in self.fabrics if
                (fabric.type == FabricType.Spark and fabric.provider == FabricProviderType.EMR)]

    def dataproc_fabrics(self) -> List[FabricInfo]:
        return [fabric for fabric in self.fabrics if
                (fabric.type == FabricType.Spark and fabric.provider == FabricProviderType.Dataproc)]

    def list_all_fabrics(self) -> List[str]:
        return [fabric.id for fabric in self.fabrics]


class JobsState(BaseModel):
    version: str
    jobs: List[JobInfo] = []

    @property
    def jobs_to_process(self) -> List[JobInfo]:
        return [job for job in self.jobs if not job.skip_processing]

    @property
    def databricks_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs_to_process if job.type == SchedulerType.Databricks]

    @property
    def airflow_jobs(self) -> List[JobInfo]:
        return [job for job in self.jobs_to_process if job.type != SchedulerType.Databricks]

    def contains_job(self, job_id: str, fabric_uid: str) -> bool:
        return any(
            job.id == job_id and job.fabric_id == fabric_uid for job in self.jobs_to_process)

    def all_jobs_for_id(self, job_id: str) -> List[JobInfo]:
        return [job for job in self.jobs_to_process if job.id == job_id]

    def job_for_id_and_fabric(self, job_id: str, fabric_id: str) -> Optional[JobInfo]:
        return next(
            (job for job in self.all_jobs_for_id(job_id) if job.fabric_id == fabric_id), None)

    def filter_job(self, job_info) -> List[JobInfo]:
        return [job for job in self.jobs if
                not (job.id == job_info.id and job.fabric_id == job_info.fabric_id)]

    def update_state(self, jobs_responses: List[Either]):
        filtered_response: List[JobInfoAndOperation] = [response.right for
                                                        response
                                                        in jobs_responses if
                                                        response.is_right]
        # Important to do all operations in this order,
        # first we delete
        # then we refresh
        # then we create

        for job_response in filtered_response:
            job_info = job_response.job_info

            if job_response.operation == OperationType.DELETED:
                self.jobs = self.filter_job(job_info)

        for job_response in filtered_response:
            job_info = job_response.job_info

            if job_response.operation == OperationType.REFRESH:
                new_jobs = self.filter_job(job_info)
                new_jobs.append(job_info)
                self.jobs = new_jobs

        for job_response in filtered_response:
            job_info = job_response.job_info

            if job_response.operation == OperationType.CREATED:
                matching_job = next((job for job in self.jobs if job.id == job_info.id), None)

                new_jobs = self.filter_job(job_info)

                if matching_job is not None:
                    # super important to preserve the release tag.
                    job_info.update_release_tag(matching_job.release_tag)

                new_jobs.append(job_info)

                self.jobs = new_jobs


class JobAndFabric(BaseModel):
    job_id: str
    fabric_id: str


class ConfigsOverride(BaseModel):
    tests_enabled: Optional[bool] = False
    mode: Optional[DeploymentMode] = DeploymentMode.FullProject
    jobs_and_fabric: Optional[List[JobAndFabric]] = None

    def find_fabric_override_for_job(self, job_id: str) -> Optional[str]:
        if self.mode == DeploymentMode.SelectiveJob and self.jobs_and_fabric is not None:
            return next(
                (entity.fabric_id for entity in self.jobs_and_fabric if entity.job_id == job_id),
                None
            )
        return None

    def is_job_to_run(self, job_id) -> bool:
        if self.mode == DeploymentMode.SelectiveJob and self.jobs_and_fabric is not None:
            return any(job_and_fabric.job_id == job_id for job_and_fabric in self.jobs_and_fabric)
        return True

    @staticmethod
    def empty():
        return ConfigsOverride()


class NexusConfig(BaseModel):
    url: str
    username: str
    password: str
    repository: str


class SystemConfig(BaseModel):
    customer_name: Optional[str] = 'dev'
    control_plane_name: Optional[str] = 'execution'
    prophecy_salt: Optional[str] = 'execution'
    nexus: Optional[NexusConfig] = None

    def get_dbfs_base_path(self):
        return f'{DBFS_FILE_STORE}/{PROPHECY_ARTIFACTS}/{self.customer_name}/{self.control_plane_name}'

    def get_s3_base_path(self):
        return f"{PROPHECY_ARTIFACTS}/{self.customer_name}/{self.control_plane_name}"

    @staticmethod
    def empty():
        return SystemConfig()


class ProjectConfig:
    def __init__(self, jobs_state: JobsState, fabric_config: FabricConfig, system_config: SystemConfig,
                 config_override: ConfigsOverride):

        self.jobs_state = jobs_state
        self.fabric_config = fabric_config
        self.system_config = system_config
        self.configs_override = config_override

    @staticmethod
    def from_path(job_state_path: str, system_config_path: str, configs_override_path: str,
                  fabric_config_path: str):
        def load_jobs_state():
            if job_state_path is not None and len(job_state_path) > 0:
                with open(job_state_path, "r") as job_state:
                    data = job_state.read()
                    return parse_yaml_raw_as(JobsState, data)
            else:
                raise ConfigFileNotFoundException("Job state config path is not provided")

        def load_system_config():
            if system_config_path is not None and len(system_config_path) > 0:
                with open(system_config_path, "r") as system_config:
                    data = system_config.read()
                    return parse_yaml_raw_as(SystemConfig, data)
            else:
                raise ConfigFileNotFoundException("System config path is not provided")

        def load_configs_override():
            if configs_override_path is not None and len(configs_override_path) > 0:
                with open(configs_override_path, "r") as config_override:
                    data = config_override.read()
                    return parse_yaml_raw_as(ConfigsOverride, data)
            else:
                return ConfigsOverride.empty()

        def load_fabric_config():
            if fabric_config_path is not None and len(fabric_config_path) > 0:
                with open(fabric_config_path, "r") as fabric_config:
                    data = fabric_config.read()
                    return parse_yaml_raw_as(FabricConfig, data)
            else:
                raise ConfigFileNotFoundException("Fabric config path is not provided")

        return ProjectConfig(load_jobs_state(), load_fabric_config(), load_system_config(), load_configs_override())

    # best used when invoking from execution.
    @classmethod
    def from_conf_folder(cls, conf_folder):
        jobs_state = os.path.join(conf_folder, "jobs_state.yml")
        system_config = os.path.join(conf_folder, "system.yml")
        config_override = os.path.join(conf_folder, "config_override.yml")
        fabric_config = os.path.join(conf_folder, "fabrics.yml")

        return ProjectConfig.from_path(jobs_state, system_config, config_override, fabric_config)


def update_state(responses: List, step_id: str):
    if responses is not None and len(responses) > 0:
        if any(response.is_left for response in responses):
            log(step_status=Status.FAILED, step_id=step_id)
        else:
            log(step_status=Status.SUCCEEDED, step_id=step_id)
