import base64
import json
import os
import tempfile

from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import DbfsService, JobsService, SecretService
from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi

from src.pbt.v2.state_config import StateConfig


class DatabricksClient:
    def __init__(self, host: str = None, token: str = None):
        self.host = host
        self.token = token
        self.dbfs = DbfsApi(ApiClient(host=host, token=token, api_version="2.0"))
        self.job = JobsApi(ApiClient(host=host, token=token, api_version="2.1"))
        self.job_client = JobsService(ApiClient(host=host, token=token, api_version="2.1"))
        self.secret = SecretApi(ApiClient(host=host, token=token, api_version="2.0"))

    @classmethod
    def from_state_config(cls, state_config: StateConfig, db_fabrics_to_token: dict = {}, fabric_id: str = None):
        if fabric_id is None or fabric_id == "" or state_config.contains_fabric(
                fabric_id) is False or db_fabrics_to_token.get(fabric_id) is None:
            # todo improve the error messages.
            raise ValueError("fabric_id must be provided")

        return cls(state_config.get_fabric(fabric_id).url, db_fabrics_to_token.get(fabric_id))

    @classmethod
    def from_environment_variables(cls):
        config = EnvironmentVariableConfigProvider()
        host = config.get_config().host
        token = config.get_config().token
        return cls(host, token)

    @classmethod
    def from_host_and_token(cls, host: str, token: str):
        return cls(host, token)

    def upload_content(self, content: str, path: str):
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(content.encode())
            self.upload_src_path(src_path=temp_file.name, destination_path=path)

    def upload_src_path(self, src_path: str, destination_path: str):
        self.dbfs.put_file(src_path=src_path, dbfs_path=destination_path, overwrite=True)

    def path_exist(self, path: str):
        return self.dbfs.get_status(path) is not None

    def delete(self, path: str):
        self.dbfs.delete(path, recursive=True)

    def create_scope(self, secret_scope: str):
        response = self.secret.list_scopes()
        if any(scope['name'] == secret_scope for scope in response['scopes']) is False:
            self.secret.create_scope(secret_scope)
        else:
            print(f"Scope {secret_scope} already exists.")
            return True

    def create_secret(self, scope: str, key: str, value: str):
        self.secret.put_secret(scope, key, value)

    def create_job(self, content:dict):
        return self.job.create_job(**content)

    def get_job(self, job_id: str):
        return self.job.get_job(job_id)

    def delete_job(self, job_id: str):
        self.job.delete_job(job_id)

    def pause_job(self, job_id: str):
        response = self.job.get_job(job_id)
        if response['settings']['schedule'] is not 'Paused':
            self.job_client.update_job(job_id, schedule='Paused')

    def reset_job(self, job_id:str, update_request:str):
        self.job.reset_job(job_id, update_request)