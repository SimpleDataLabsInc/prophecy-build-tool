from typing import List, Optional

from pydantic import BaseModel


class StepMetadata:

    def __init__(self, step_id: str, header: str, operation: str, step_type: str):
        self.step_id = step_id
        self.header = header
        self.operation = operation
        self.step_type = step_type


class Component(BaseModel):
    id: str
    node_name: str
    path: Optional[str]
    language: str


class JobContent(BaseModel):
    job_id: str
    components: List[Component]
    enabled: bool
    fabric_id: str
    secret_scope: Optional[str]

    def is_script_job(self) -> bool:
        pass


class DbtComponentsModel:
    def __init__(self, fabric_id: str, secret_scope: str, components: List):
        self.fabric_id = str(fabric_id)
        self.secret_scope = secret_scope
        self.components = components


class ScriptComponentsModel:
    def __init__(self, fabric_id: str, scripts: List):
        self.scripts = scripts
        self.fabric_id = fabric_id


class DAG:

    def __init__(self, dag_id: str, description: Optional[str] = None, file_token: Optional[str] = None,
                 fileloc: Optional[str] = None,
                 is_active: Optional[bool] = None, is_paused: bool = True, is_subdag: Optional[bool] = None,
                 owners: List[str] = None,
                 root_dag_id: Optional[str] = None, schedule_interval: Optional[str] = None,
                 next_dagrun: Optional[str] = None,
                 tags: List[str] = []):
        self.dag_id = dag_id
        self.description = description
        self.file_token = file_token
        self.fileloc = fileloc
        self.is_active = is_active
        self.is_paused = is_paused
        self.is_subdag = is_subdag
        self.owners = owners
        self.root_dag_id = root_dag_id
        self.schedule_interval = schedule_interval
        self.next_dagrun = next_dagrun
        self.tags = tags

    @staticmethod
    def create(responses: dict):
        dag_id = responses.get('dag_id')
        description = responses.get('description', None)
        file_token = responses.get('file_token', None)
        fileloc = responses.get('fileloc', None)
        is_active = responses.get('is_active', None)
        is_paused = responses.get('is_paused', None)
        is_subdag = responses.get('is_subdag', None)
        owners = responses.get('owners', [])
        root_dag_id = responses.get('root_dag_id', None)
        schedule_interval = responses.get('schedule_interval', {}).get('value', None)
        next_dagrun = responses.get('next_dagrun', None)
        tags = responses.get('tags', [])
        return DAG(dag_id, description, file_token, fileloc, is_active, is_paused, is_subdag, owners, root_dag_id,
                   schedule_interval, next_dagrun, tags)

    # different from the scala release.
    @staticmethod
    def create_from_mwaa(response: dict):
        dag_id = response.get('dag_id')
        fileloc = response.get('filepath', None)
        is_paused = response.get('paused', None)
        owners = response.get('owners', None)
        return DAG(dag_id, fileloc=fileloc, is_paused=is_paused, owners=owners)
