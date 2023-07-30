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

    def update_path(self, project_id: str, version: str, release_tag: str):
        if self.path is not None:
            return (self.path
                    .replace("ProjectIdPlaceHolder", project_id)
                    .replace("ProjectVersionPlaceHolder", version)
                    .replace("ProjectReleaseTagPlaceHolder", release_tag))
        else:
            return None


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
        self.fabric_id = fabric_id
        self.secret_scope = secret_scope
        self.components = components


class ScriptComponentsModel:
    def __init__(self, fabric_id: str, scripts: List):
        self.scripts = scripts
        self.fabric_id = fabric_id


class DAG:

    def __init__(self, dag_id: str, description: Optional[str], file_token: Optional[str], fileloc: Optional[str],
                 is_active: Optional[bool], is_paused: bool, is_subdag: Optional[bool], owners: List[str],
                 root_dag_id: Optional[str], schedule_interval: Optional[str], next_dagrun: Optional[str],
                 tags: List[str]):
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
