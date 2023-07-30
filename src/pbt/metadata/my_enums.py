# my_enum.py
from abc import ABC
from enum import Enum
from typing import List, Optional
from dataclasses import dataclass
from pydantic import BaseModel


class ReleaseMode(Enum):
    TEST = 1
    ONLINE = 2
    OFFLINE = 3


def find_mode(mode: str = None) -> ReleaseMode:
    if mode is None:
        try:
            ReleaseMode[mode.upper()]
        except KeyError:
            print("Invalid mode. Defaulting to TEST")
            return ReleaseMode.TEST
    else:
        return ReleaseMode.TEST



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


class DbtComponents:
    def __init__(self, fabric_id: str, secret_scope: str, components: List):
        self.fabric_id = fabric_id
        self.secret_scope = secret_scope
        self.components = components
