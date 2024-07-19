import os
import tempfile
from typing import Dict, Optional

from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import JobsService
from databricks_cli.secrets.api import SecretApi
from requests import HTTPError
from tenacity import retry_if_exception_type, retry, stop_after_attempt, wait_exponential

from ..utility import Either
from ..utils.exceptions import DuplicateJobNameException


class DatabricksClient:
    def __init__(self, host: str = None, token: str = None, user_agent: Optional[str] = None):
        self.host = host
        self.token = token
        self.headers = {"User-Agent": user_agent or "Prophecy"}

        verify = True
        if os.environ.get("SSL_DISABLED_EXECUTION", "").lower() == "true":
            verify = False

        api_client_2 = ApiClient(host=host, token=token, api_version="2.0", verify=verify)
        api_client_2_1 = ApiClient(host=host, token=token, api_version="2.1", verify=verify)

        self.dbfs = DbfsApi(api_client_2)
        self.job = JobsApi(api_client_2_1)
        self.job_client = JobsService(api_client_2_1)
        self.secret = SecretApi(api_client_2)
        self.permission = PermissionsApi(api_client_2)

    @classmethod
    def from_environment_variables(cls):
        config = EnvironmentVariableConfigProvider()
        host = config.get_config().host
        token = config.get_config().token
        return cls(host, token)

    @classmethod
    def from_host_and_token(cls, host: str, token: str, user_agent: Optional[str]):
        return cls(host, token, user_agent)

    def upload_content(self, content: str, path: str):
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(content.encode())
            # copy-pasted from gpt.
            temp_file.flush()  # Ensure that the content is flushed to disk
            temp_file.seek(0)  # Reset file's pointer to the beginning
            #
            # print(temp_file.read().decode())

            self.upload_src_path(src_path=temp_file.name, destination_path=path)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def upload_src_path(self, src_path: str, destination_path: str):
        self.dbfs.put_file(src_path=src_path, dbfs_path=DbfsPath(destination_path, False), overwrite=True,
                           headers=self.headers)

    def path_exist(self, path: str) -> bool:
        return self.dbfs.get_status(DbfsPath(path), headers=self.headers) is not None

    def delete(self, path: str):
        self.dbfs.delete(path, recursive=True, headers=self.headers)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def create_secret_scope_if_not_exist(self, secret_scope: str):
        response = self.secret.list_scopes()

        if any(scope["name"] == secret_scope for scope in response["scopes"]) is False:
            self.secret.create_scope(
                secret_scope, initial_manage_principal="users", scope_backend_type=None, backend_azure_keyvault=None
            )
        else:
            return True

    def create_secret(self, scope: str, key: str, value: str):
        self.secret.put_secret(scope, key, value, None)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def create_job(self, content: Dict[str, str]):
        return self.job.create_job(content, headers=self.headers)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def get_job(self, scheduler_job_id: str):
        return self.job.get_job(scheduler_job_id, headers=self.headers)

    def delete_job(self, job_id: str):
        self.job.delete_job(job_id, headers=self.headers)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def pause_job(self, scheduler_job_id: str) -> Either:
        response = self.job.get_job(scheduler_job_id, headers=self.headers)

        try:
            pause_status = response["settings"]["schedule"]["pause_status"]
        except KeyError as e:
            print(f"No pause_status found in job {scheduler_job_id} settings")
            return Either(left=e)

        if pause_status != "Paused":
            updated_settings = dict(response)
            updated_settings["settings"]["schedule"]["pause_status"] = "Paused"
            self.job_client.update_job(scheduler_job_id, new_settings=updated_settings, headers=self.headers)
            print(f"Job {scheduler_job_id} has been paused.")
            return Either(right=True)
        else:
            print(f"Job {scheduler_job_id} is already paused.")
            return Either(right=False)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def reset_job(self, scheduler_job_id: str, update_request: Dict[str, str]):
        new_settings = {"job_id": scheduler_job_id, "new_settings": update_request}

        self.job.reset_job(new_settings, headers=self.headers)

    def patch_job_acl(self, scheduler_job_id: str, acl):
        print(f"patching job acl {scheduler_job_id} and acl {acl}")
        self.permission.patch_job(scheduler_job_id, acl, headers=self.headers)

    @retry(
        retry=retry_if_exception_type(HTTPError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, max=30),
        reraise=True,
    )
    def find_job(self, job_name) -> Optional[str]:
        try:
            jobs = self.job.list_jobs(name=job_name, headers=self.headers)["jobs"]
            if jobs is not None:
                if len(jobs) == 1:
                    return jobs[0]["job_id"]
                else:
                    raise DuplicateJobNameException(f"Found more than one job with name {job_name}")
            return None
        except Exception:
            return None


class PermissionsApi(object):
    def __init__(self, client: ApiClient):
        self.client = client

    def patch_job(self, scheduler_job_id: str, data, headers=None):
        return self.client.perform_query("PATCH", f"/2.0/preview/permissions/jobs/{scheduler_job_id}", data=data,
                                         headers=headers)
