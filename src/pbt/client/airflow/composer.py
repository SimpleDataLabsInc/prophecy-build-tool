import json
import os
import re
import tempfile
from abc import ABC
from pathlib import Path
from typing import Optional

import requests
from google.auth.transport.requests import AuthorizedSession
from google.cloud import secretmanager, storage
from google.cloud.storage import Blob
from google.oauth2 import service_account
from requests import HTTPError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from . import AirflowRestClient
from ...utils.exceptions import DagUploadFailedException, DagFileDeletionFailedException
from ...utils.project_models import DAG

pattern = re.compile(r"^g[c]?s://([a-z0-9][-a-z0-9.]*[a-z0-9]):?/(.*)?$")


class GCSPathInfo:
    def __init__(self, bucket: str, path: Optional[str]):
        self.bucket = bucket
        self.path = path

    @staticmethod
    def get_gcs_path_info(dag_location: str):
        match = pattern.match(dag_location)

        if match:
            bucket, path = match.groups()
            gcs_path_info = GCSPathInfo(bucket.strip("/"), "/".join(path.split("/")).rstrip("/"))
        else:
            gcs_path_info = None

        return gcs_path_info


class ComposerRestClient(AirflowRestClient, ABC):
    _SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
    _CONNECTIONS_SECRET_PREFIX = "airflow-connections-"
    _pattern = re.compile(pattern)
    _headers = {"Content-Type": "application/json", "Accept": "application/json"}

    def __init__(
        self,
        airflow_url: str,
        project_id: str,
        client_id: Optional[str],
        key_json: str,
        dag_location: str,
        location: Optional[str] = None,
    ):
        self.airflow_url = airflow_url
        self.project_id = project_id
        self.client_id = client_id
        self.key_json = json.loads(key_json)
        self.dag_location = dag_location
        self.location = location

        self.file_path = self._create_key_json_file(key_json)

        self.storage_handler = storage.Client.from_service_account_info(self.key_json, project=project_id)

    def delete_dag_file(self, dag_id: str):
        gcs_path_info = GCSPathInfo.get_gcs_path_info(self.dag_location)
        blob: Blob = Blob(self._get_dag_location(gcs_path_info, dag_id), gcs_path_info.bucket)
        try:
            self.storage_handler.bucket(gcs_path_info.bucket).delete_blob(blob.name)
        except Exception as e:
            raise DagFileDeletionFailedException(f"Failed to delete DAG file: {blob.name} ", e)

    def pause_dag(self, dag_id: str):
        self._set_pause_state(dag_id, True)

    def upload_dag(self, dag_id: str, file_path: str):
        gcs_path_info = GCSPathInfo.get_gcs_path_info(self.dag_location)
        bucket = self.storage_handler.get_bucket(gcs_path_info.bucket)
        dag_location = self._get_dag_location(gcs_path_info, dag_id)

        blob: Blob = Blob(dag_location, gcs_path_info.bucket)

        try:
            bucket.blob(blob_name=blob.name).upload_from_filename(file_path)
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload DAG file: {dag_location} ", e)
        finally:
            if Path(file_path).exists():
                os.remove(file_path)

    def unpause_dag(self, dag_id: str):
        self._set_pause_state(dag_id, False)

    # todo maybe we can add labmda function to print in right format
    def create_secret(self, key: str, value: str) -> bool:
        try:
            client = self._get_secrets_manager()
            secret_name = f"projects/{self.project_id}/secrets/{key}"

            try:
                client.get_secret(name=secret_name)
                client.add_secret_version(
                    parent=secret_name,
                    payload=secretmanager.SecretPayload(data=value.encode("UTF-8")),
                )

            except Exception as e:
                print(f"Failed to get secret: {secret_name}, trying to create default one", e)

                parent = f"projects/{self.project_id}"
                secret = client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": key,
                        "secret": {"replication": {"automatic": {}}},
                    }
                )
                client.add_secret_version(
                    parent=secret.name,
                    payload=secretmanager.SecretPayload(data=value.encode("UTF-8")),
                )

        except Exception as e:
            print("Failed to create Secret manager client", e)
            return False

        return True

    def get_dag(self, dag_id: str) -> DAG:
        response = requests.request("GET", f"{self.airflow_url}/api/v1/dags/{dag_id}", headers=self._headers)
        response.raise_for_status()
        response_data = response.json()

        # Parse JSON into Dag class
        dag = DAG(**response_data)
        return dag

    def delete_dag(self, dag_id: str) -> str:
        response = requests.request("DELETE", f"{self.airflow_url}/api/v1/dags/{dag_id}")
        response.raise_for_status()
        return response.text

    def _create_key_json_file(self, key_json: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(prefix="composer", suffix=".json", delete=False)

        # Write content to the temporary file
        Path(temp_file.name).write_text(key_json)

        return temp_file.name

    def _get_dag_location(self, gcp_path_info: GCSPathInfo, dag_id: str) -> str:
        if gcp_path_info is not None and gcp_path_info.path is not None:
            return f"{gcp_path_info.path}/{dag_id}.zip"
        else:
            print(f"Shouldn't be here: {gcp_path_info}")
            return f"{dag_id}.zip"

    def _get_secrets_manager(self):
        # fill up how to get the credentials
        creds = service_account.Credentials.from_service_account_info(self.key_json)
        client = secretmanager.SecretManagerServiceClient(credentials=creds)
        return client

    def _get_authenticated_session(self):
        if self.client_id is not None and len(self.client_id) > 0:
            print(f"Client Id {self.client_id}")
            credentials = (
                service_account.IDTokenCredentials.from_service_account_info(self.key_json)
                .with_target_audience(self.client_id)
                .with_scopes(self._SCOPES)
            )

        else:
            credentials = service_account.Credentials.from_service_account_info(self.key_json).with_scopes(self._SCOPES)
        return AuthorizedSession(credentials)

    @retry(retry=retry_if_exception_type(HTTPError), stop=stop_after_attempt(20), wait=wait_fixed(15), reraise=True)
    def _set_pause_state(self, dag_id: str, is_paused: bool) -> DAG:
        session = self._get_authenticated_session()
        response = session.request(
            method="PATCH",
            url=f"{self.airflow_url}/api/v1/dags/{dag_id}",
            data=json.dumps({"is_paused": is_paused}),
            headers=self._headers,
        )
        response.raise_for_status()
        response_data = response.json()
        return DAG.create(response_data)

    def put_object_from_file(self, bucket: str, key: str, file_path: str):
        try:
            self.storage_handler.bucket(bucket).blob(key).upload_from_filename(file_path)
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload file: {file_path} ", e)

    def put_object(self, bucket: str, key: str, content: str):
        try:
            self.storage_handler.bucket(bucket).blob(key).upload_from_string(content)
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload content to bucket: {bucket} ", e)
