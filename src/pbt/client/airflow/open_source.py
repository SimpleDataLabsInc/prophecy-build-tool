import json
import os
from abc import ABC
from pathlib import Path
from urllib.parse import urljoin
from urllib.parse import urlparse

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
_file_headers = {"Accept": "application/json", "Content-Type": "multipart/form-data"}


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
        params = {"file_path": current_dag_location}
        rel_path = _hdfs_delete_path if self._is_hdfs else _file_delete_path
        uploader_url_with_path = urljoin(self.uploader_url, rel_path)
        try:
            requests.post(uploader_url_with_path, auth=self.uploader_auth, params=params, headers=_json_headers)
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(f"Failed to delete DAG file {current_dag_location}. {response_info}", e)
        except Exception as e:
            raise DagFileDeletionFailedException(f"Failed to delete DAG file {current_dag_location}", e)

    def pause_dag(self, dag_id: str) -> DAG:
        return self._set_pause_state(dag_id, True)

    def upload_dag(self, dag_id: str, file_path: str):
        current_dag_location = self._get_dag_location(dag_id)
        params = {"destination_dir": self.dag_home}
        rel_path = _hdfs_upload_path if self._is_hdfs else _file_upload_path
        uploader_url_with_path = urljoin(self.uploader_url, rel_path)
        try:
            with open(file_path, "rb") as f:
                files = {"file": (f"{dag_id}.zip", f, "application/x-gzip")}
                requests.post(
                    uploader_url_with_path,
                    auth=self.uploader_auth,
                    files=files,
                    params=params,
                    headers=_file_headers,
                )
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload DAG file: {current_dag_location}", e)
        finally:
            if Path(file_path).exists():
                os.remove(file_path)

    def unpause_dag(self, dag_id: str) -> DAG:
        return self._set_pause_state(dag_id, False)

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

    @retry(retry=retry_if_exception_type(HTTPError), stop=stop_after_attempt(20), wait=wait_fixed(15), reraise=True)
    def _set_pause_state(self, dag_id: str, is_paused: bool) -> DAG:
        response = requests.patch(
            url=f"{self.airflow_url}/api/v1/dags/{dag_id}",
            data=json.dumps({"is_paused": is_paused}),
            headers=_json_headers,
        )
        response.raise_for_status()
        response_data = response.json()
        return DAG.create(response_data)

    def put_object_from_file(self, upload_path: str, file_name: str, file_path: str):
        params = {"destination_dir": upload_path}
        rel_path = _hdfs_upload_path if self._is_hdfs else _file_upload_path
        uploader_url_with_path = urljoin(self.uploader_url, rel_path)
        try:
            with open(file_path, "rb") as f:
                files = {"file": (file_name, f, "application/x-gzip")}
                requests.post(
                    uploader_url_with_path,
                    auth=self.uploader_auth,
                    files=files,
                    params=params,
                    headers=_file_headers,
                )
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(
                f"Failed to upload file {file_path}/{file_name} to {upload_path}. {response_info}", e
            )
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload file {file_path}/{file_name} to {upload_path}", e)

    def put_object(self, upload_path: str, file_name: str, content: str):
        params = {"destination_dir": upload_path}
        rel_path = _hdfs_upload_path if self._is_hdfs else _file_upload_path
        uploader_url_with_path = urljoin(self.uploader_url, rel_path)
        files = {"file": (file_name, content.encode(), "application/x-gzip")}
        try:
            requests.post(
                uploader_url_with_path, auth=self.uploader_auth, files=files, params=params, headers=_file_headers
            )
        except requests.RequestException as e:
            response_info = (
                f"Status Code: {e.response.status_code}, Response Text: {e.response.text}"
                if e.response
                else "No response"
            )
            raise DagUploadFailedException(f"Failed to upload content to {upload_path}/{file_name}. {response_info}", e)
        except Exception as e:
            raise DagUploadFailedException(f"Failed to upload content to {upload_path}/{file_name} ", e)
