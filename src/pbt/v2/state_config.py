from typing import List, Optional, Dict
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_raw_as

# todo explore https://docs.pydantic.dev/latest/usage/serialization/#subclasses-of-standard-types
# explore https://docs.pydantic.dev/latest/usage/models/#generic-models

'''
 more thoughts to explore 
 
 from pydantic import BaseModel, ValidationError
from typing import Optional, List, Union

class User(BaseModel):
    id: int
    name: str
    email: Optional[str]
    friends: List[int] = []
    type: str

class AdminUser(User):
    admin_rights: List[str]

class GuestUser(User):
    guest_since: str

def create_user(data: dict) -> Union[User, AdminUser, GuestUser]:
    user_type = data.get('type')
    if user_type == 'admin':
        return AdminUser(**data)
    elif user_type == 'guest':
        return GuestUser(**data)
    else:
        return User(**data)

'''


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
    expiry: Optional[str] = None
    pool_name: Optional[str] = None
    composer_info: Optional[ComposerInfo] = None
    provider_type: Optional[str]
    mwaa_info: Optional[MwaaInfo] = None


class FabricsInfo(BaseModel):
    id: str
    name: str
    url: str
    type: str
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

    def get_databricks_jobs(self) -> List[JobsInfo]:
        return [job for job in self.jobs if job.type is 'Databricks']

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
        fabric = self.get_fabric(fabric_id)
        if fabric is None:
            return False
        return True

    @classmethod
    def empty_state_config(cls):
        return cls(name="", language="", description="", version="")


class StateConfigAndDBTokens:
    def __init__(self, path: str, db_tokens: Dict[str, str]):
        self.path = path
        self.state_config = None
        self.__load_state_config()
        self.db_tokens = db_tokens

    def __load_state_config(self):
        if self.path is not None:
            with open(self.path, "r") as state_config:
                data = state_config.read()
                self.state_config = parse_yaml_raw_as(StateConfig, data)
        else:
            self.state_config = StateConfig.empty_state_config()
