import os
import re
import tempfile
from abc import ABC
from pathlib import Path
from typing import Optional

from google.cloud import secretmanager, storage
from google.cloud.storage import Blob
from google.oauth2 import service_account
from google.oauth2.gdch_credentials import ServiceAccountCredentials
from requests import HTTPError

from src.pbt.v2.client.airflow_client.mwaa_client import retry
from src.pbt.v2.project_models import DAG
from google.auth.transport.requests import AuthorizedSession
import requests
import json
from src.pbt.v2.client.airflow_client.airflow_rest_client import AirflowRestClient


class GCSPathInfo:
    def __init__(self, bucket: str, path: Optional[str]):
        self.bucket = bucket
        self.path = path


class ComposerRestClient(AirflowRestClient, ABC):
    _SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
    _CONNECTIONS_SECRET_PREFIX = "airflow-connections-"

    def __init__(self, airflow_url: str, project_id: str, client_id: Optional[str], key_json: str, dag_location: str):
        self.airflow_url = airflow_url
        self.project_id = project_id
        self.client_id = client_id
        self.key_json_file = key_json
        self.dag_location = dag_location

        self.file_path = ComposerRestClient.__create_key_json_file(key_json)

        self.storage_handler = storage.Client.from_service_account_info(json.loads(key_json), project=project_id)

    def delete_dag_file(self, dag_id: str) -> bool:
        gcs_path_info = self.__get_gcs_path_info()
        blob: Blob = Blob(gcs_path_info.bucket, self.__get_dag_location(gcs_path_info, dag_id))
        print("DELETE DAG: ${blobId.getBucket}/${blobId.getName}")
        try:
            self.storage_handler.bucket(gcs_path_info.bucket).delete_blob(blob.name)
            return True
        except Exception as e:
            print(f"Failed to delete DAG file: ", e)
            return False

    def pause_dag(self, dag_id: str) -> DAG:
        return self.__set_pause_state(dag_id, True)

    def upload_dag(self, dag_id: str, file_path: str):
        gcs_path_info = self.__get_gcs_path_info()
        bucket = self.storage_handler.get_bucket(gcs_path_info.bucket)
        blob: Blob = Blob(self.__get_dag_location(gcs_path_info, dag_id), gcs_path_info.bucket)
        try:
            bucket.blob(blob_name=blob.name).upload_from_filename(file_path)
        except Exception as e:
            print(f"Failed to upload DAG file: ", e)
            raise e
        finally:
            if Path(file_path).exists():
                os.remove(file_path)

    def unpause_dag(self, dag_id: str) -> DAG:
        return self.__set_pause_state(dag_id, False)

    def create_secret(self, key: str, value: str) -> bool:
        client = None
        try:
            client = self.__get_secrets_manager()
            secret_name = f"projects/{self.project_id}/secrets/{key}"

            try:
                client.get_secret(name=secret_name)
                client.add_secret_version(
                    request={"parent": secret_name, "payload": {"data": value.encode('UTF-8')}}
                )
            except Exception as e:
                parent = f"projects/{self.project_id}"
                secret = client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": key,
                        "secret": {
                            "replication": {"automatic": {}}
                        },
                    }
                )
                client.add_secret_version(request={
                    "parent": secret.name,
                    "payload": value.encode('UTF-8'),
                })

        except Exception as e:
            print("Failed to create Secret manager client", e)
            return False
        finally:
            if client is not None:
                client.__exit__()

        return True

    def get_dag(self, dag_id: str) -> DAG:
        response = requests.request('GET', f"{self.airflow_url}/api/v1/dags/{dag_id}",
                                    headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
        response.raise_for_status()
        response_data = response.json()

        # Parse JSON into Dag class
        dag = DAG(**response_data)
        return dag

    def delete_dag(self, dag_id: str) -> str:
        response = requests.request('DELETE', f"{self.airflow_url}/api/v1/dags/{dag_id}")
        response.raise_for_status()
        return response.text

    @staticmethod
    def __create_key_json_file(key_json: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(prefix="composer", suffix=".json", delete=False)

        # Write content to the temporary file
        Path(temp_file.name).write_text(key_json)

        return temp_file.name

    @staticmethod
    def __get_dag_location(gcp_path_info: GCSPathInfo, dag_id: str) -> str:
        if gcp_path_info is not None and gcp_path_info.path is not None:
            return f"{gcp_path_info.path}/{dag_id}.zip"
        else:
            print(f"Shouldn't be here: {gcp_path_info}")
            return f"{dag_id}.zip"

    def __get_secrets_manager(self):
        # fill up how to get the credentials
        credentials = ServiceAccountCredentials.from_service_account_file(self.key_json_file)
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        return client

    def __get_gcs_path_info(self) -> GCSPathInfo:
        pattern = re.compile(r"^g[c]?s://([a-z0-9][-a-z0-9.]*[a-z0-9]):?/(.*)?$")
        match = pattern.match(self.dag_location)
        if match:
            bucket, path = match.groups()
            gcs_path_info = GCSPathInfo(bucket, "/".join(path.split("/")))
        else:
            gcs_path_info = None
        return gcs_path_info

    def get_authenticated_session(self):

        if self.client_id is not None and len(self.client_id) > 0:
            print(f'Client Id {self.client_id}')
            credentials = service_account.IDTokenCredentials \
                .from_service_account_info(json.loads(self.key_json_file)) \
                .with_target_audience(self.client_id) \
                .with_scopes(self._SCOPES)


        else:
            credentials = service_account.Credentials.from_service_account_info(json.loads(self.key_json_file)) \
                .with_scopes(self._SCOPES)
        return AuthorizedSession(credentials)

    @retry((HTTPError,), total_tries=5, delay_in_seconds=10, backoff=0)
    def __set_pause_state(self, dag_id: str, is_paused: bool) -> DAG:
        session = self.get_authenticated_session()
        response = session.request(method='PATCH', url=f"{self.airflow_url}/api/v1/dags/{dag_id}",
                                   data=json.dumps({"is_paused": is_paused}),
                                   headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
        response.raise_for_status()
        response_data = response.json()
        return DAG.create(response_data)
