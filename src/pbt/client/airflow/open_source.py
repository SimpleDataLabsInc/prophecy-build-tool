import json
import os
from abc import ABC
from urllib.parse import urljoin
from urllib.parse import urlparse
from pathlib import Path

import requests
from requests import HTTPError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from . import AirflowRestClient
from ...utils.exceptions import DagFileDeletionFailedException, DagUploadFailedException
from ...utils.project_models import DAG


def is_hdfs_path(path) -> bool:
    parsed = urlparse(path)
    return parsed.scheme == "hdfs"


base_path = "api/v1"
_hdfs_upload_path = f"{base_path}/hdfs/upload"
_hdfs_delete_path = f"{base_path}/hdfs/delete"
_file_upload_path = f"{base_path}/file/upload"
_file_delete_path = f"{base_path}/file/delete"
_json_headers = {"Accept": "application/json", "Content-Type": "application/json"}
_file_headers = {"Accept": "application/json"}


class OpenSourceRestClient(AirflowRestClient, ABC):
    def __init__(
        self,
        airflow_url: str,
        airflow_username: str,
        airflow_password: str,
        uploader_url: str,
        uploader_username: str,
        uploader_password: str,
        dag_location: str,
        artifacts_location: str,
    ):
        self.airflow_url = airflow_url
        self.airflow_auth = (airflow_username, airflow_password)
        self.uploader_url = uploader_url
        self.uploader_auth = (uploader_username, uploader_password)
        self.dag_home = dag_location
        self.artifacts_location = artifacts_location
        self._is_hdfs = is_hdfs_path(dag_location)

    def delete_dag_file(self, dag_id: str):
        current_dag_location = self._get_dag_location(dag_id)
        rel_path = _hdfs_delete_path if self._is_hdfs else _file_delete_path
        uploader_url_with_path = urljoin(urljoin(self.uploader_url, rel_path), f"?file_path={current_dag_location}")
        try:
            requests.post(uploader_url_with_path, auth=self.uploader_auth, headers=_json_headers)
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(f"Failed to delete DAG file {current_dag_location}. {response_info}", e)
        except Exception as e:
            raise DagFileDeletionFailedException(f"Failed to delete DAG file {current_dag_location}", e)

    def pause_dag(self, dag_id: str):
        self._set_pause_state(dag_id, True)

    def upload_dag(self, dag_id: str, file_path: str):
        try:
            self.put_object_from_file(self.dag_home, f"{dag_id}.zip", file_path)
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload DAG file from path {file_path}", e)
        finally:
            if Path(file_path).exists():
                os.remove(file_path)

    def unpause_dag(self, dag_id: str):
        self._set_pause_state(dag_id, False)

    def get_dag(self, dag_id: str) -> DAG:
        response = requests.get(f"{self.airflow_url}/api/v1/dags/{dag_id}", headers=_json_headers)
        response.raise_for_status()
        response_data = response.json()

        # Parse JSON into Dag class
        dag = DAG(**response_data)
        return dag

    def delete_dag(self, dag_id: str) -> str:
        response = requests.delete(f"{self.airflow_url}/api/v1/dags/{dag_id}")
        response.raise_for_status()
        return response.text

    def create_secret(self, key: str, value: str) -> bool:
        raise Exception("Cannot create secrets in open source Airflow")

    def _get_dag_location(self, dag_id: str) -> str:
        return f"{self.dag_home}/{dag_id}.zip"

    @retry(retry=retry_if_exception_type(HTTPError), stop=stop_after_attempt(30), wait=wait_fixed(15), reraise=True)
    def _set_pause_state(self, dag_id: str, is_paused: bool) -> DAG:
        response = requests.patch(
            url=f"{self.airflow_url}/api/v1/dags/{dag_id}",
            auth=self.airflow_auth,
            data=json.dumps({"is_paused": is_paused}),
            headers=_json_headers,
        )
        response.raise_for_status()
        response_data = response.json()
        return DAG.create(response_data)

    def put_object_from_file(self, upload_directory: str, local_file_name: str, local_file_path: str):
        rel_path = _hdfs_upload_path if self._is_hdfs else _file_upload_path
        uploader_url_with_path = urljoin(urljoin(self.uploader_url, rel_path), f"?destination_dir={upload_directory}")
        try:
            with open(local_file_path, "rb") as binary_file:
                files = {"file": (local_file_name, binary_file, "application/zip")}
                requests.post(
                    uploader_url_with_path,
                    auth=self.uploader_auth,
                    files=files,
                    headers=_file_headers,
                )
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(
                f"Failed to upload file {local_file_path} to {upload_directory}. {response_info}", e
            )
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload file {local_file_path} to {upload_directory}", e)

    def put_object(self, upload_directory: str, file_name: str, content: str):
        rel_path = _hdfs_upload_path if self._is_hdfs else _file_upload_path
        uploader_url_with_path = urljoin(urljoin(self.uploader_url, rel_path), f"?destination_dir={upload_directory}")
        files = {"file": (file_name, content.encode())}
        try:
            requests.post(uploader_url_with_path, auth=self.uploader_auth, files=files, headers=_file_headers)
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(
                f"Failed to upload content to {upload_directory}/{file_name}. {response_info}", e
            )
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload content to {upload_directory}/{file_name} ", e)
