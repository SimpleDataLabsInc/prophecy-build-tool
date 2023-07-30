import zipfile

import pydantic
from pydantic.dataclasses import dataclass
import yaml
import collections
import json
import os
import re
import subprocess
import sys
from glob import glob
from os import listdir
from os.path import basename, isfile, join, dirname
from typing import Dict, Optional
import base64
import hashlib
import re
from typing import Optional
from zipfile import ZipFile
# from cryptography.hazmat.primitives import hashes, kdf
# from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
# from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
# from cryptography.hazmat.backends import default_backend
from pydantic_yaml import parse_yaml_raw_as
import hashlib
from src.pbt.metadata.my_enums import find_mode, StepMetadata, DbtComponents
from typing import List
from abc import ABC, abstractmethod

from src.pbt.v2.client.airflow_client import AirflowRestClient
from src.pbt.v2.client.databricks_client import DatabricksClient
from src.pbt.v2.project_models import ScriptComponentsModel
from src.pbt.v2.state_config import StateConfig
from src.pbt.v2.constants import *

'''
 containing context on the current project release as well as the older jobs too.
 
 structure of release_helper 
 
 - fabrics:
     - id 
       name 
       kind {airflow/databricks}
       url {remote url} ## should be in-memory  or we can marshal it 
       token {token} ## should be in-memory or we can marshal it
     - id 
       name    
       kind {airflow/databricks}
       url {remote url}
       token {token}
 - jobs 
     - fabricId 
       jobID // optional here means that the jobs wasn't deployed in the last iteration but present in the code. 
       scheduler {databricks/composer}
       created_at
       created_by 
  - pipelines 
     - id 
     - id
     - id
          
'''


class ProjectParser:

    def __init__(self, project_path: str, release_mode: str = None, state_config_path: str = None,
                 project_id: str = None):

        self.project_path = project_path
        self.release_mode = find_mode(release_mode)

        self.project = None
        self.project_language = None
        self.jobs = None
        self.pipelines = None
        self.pipeline_configurations = {}

        self.databricks_jobs = None
        self.airflow_jobs = None

        self.state_config = None

        self.load_state_config(state_config_path)

        self.load_project_config()

        self.project_id = project_id

    def load_state_config(self, state_config_path: str = None):
        if state_config_path is not None:
            with open(state_config_path, "r") as state_config:
                data = state_config.read()
                self.state_config = parse_yaml_raw_as(StateConfig, data)
        else:
            self.state_config = StateConfig.empty_state_config()

    def load_project_config(self):
        pbt_project_path = os.path.join(self.project_path, PBT_FILE_NAME)
        with open(pbt_project_path, "r") as project_to_release:
            self.project = yaml.safe_load(project_to_release)

        self.extract_project_info()
        self.load_pipeline_configurations()

    def extract_project_info(self):
        self.project_language = self.project[LANGUAGE]
        self.jobs = self.project[JOBS]
        self.pipelines = self.project[PIPELINES]

    def load_pipeline_configurations(self):
        pipeline_conf = dict(self.project.get(PIPELINE_CONFIGURATIONS, []))

        for pipeline_config_path, pipeline_config_object in pipeline_conf.items():
            configurations = {}

            for configurations_key in pipeline_config_object[CONFIGURATIONS]:
                config_name = pipeline_config_object[CONFIGURATIONS][configurations_key]["name"]
                file_path = os.path.join(self.project_path, configurations_key + JSON_EXTENSION)

                with open(file_path, 'r') as file:
                    data = file.read()

                configurations[config_name] = data

            self.pipeline_configurations[pipeline_config_object[BASE_PIPELINE]] = configurations

    def pipeline_headers(self) -> List[StepMetadata]:
        sorted_pipelines = sorted(self.pipelines)
        step_metadata_list = []

        for i, pipeline in enumerate(sorted_pipelines):
            header = f"Building pipeline {pipeline}"
            step_metadata_list.append(StepMetadata(pipeline, header, "build", "pipeline"))

        return step_metadata_list

    def get_files(self, pipeline_path: str, aspect: str):
        return os.path.join(self.project_path, pipeline_path + '/' + aspect)

    def load_databricks_job(self, job_id: str) -> Optional[str]:
        content = None
        with open(os.path.join(self.project_path, job_id, "code", "databricks-job.json"), "r") as file:
            content = file.read()
        return content

    def load_airflow_folder(self, job_id):
        rdc = {}
        base_path = os.path.join(self.project_path, job_id, "code")
        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                full_path = os.path.join(dir_path, filename)
                with open(full_path, 'r') as file:
                    content = file.read()
                    rdc[full_path] = content
                    # Do something with the content
        return rdc

    def load_airflow_aspect(self, job_id: str) -> Optional[str]:
        content = None
        with open(os.path.join(self.project_path, job_id, "pbt_aspects.yml"), "r") as file:
            content = file.read()
        return content



