from abc import ABC, abstractmethod
from typing import Optional
from google.cloud.storage.blob import Blob
import re

from .AirflowClient import AirflowRestClient


class GCSPathInfo:
    def __init__(self, bucket: str, path: Optional[str]):
        self.bucket = bucket
        self.path = path




class ComposerRestClient(AirflowRestClient, ABC):
    def __init__(self, airflow_url: str, project_id: str, client_id: Optional[str], key_json: str, dag_location: str,
                 location: Optional[str]):
        self.airflow_url = airflow_url
        self.project_id = project_id
        self.client_id = client_id
        self.key_json = key_json
        self.dag_location = dag_location
        self.location = location

    def __get_gcs_path_info(self) -> GCSPathInfo:
        pattern = re.compile(r"^g[c]?s://([a-z0-9][-a-z0-9.]*[a-z0-9]):?/(.*)?$")
        match = pattern.match(self.dag_location)
        if match:
            bucket, path = match.groups()
            gcs_path_info = GCSPathInfo(bucket, "/".join(path.split("/")))
        else:
            gcs_path_info = None
        return gcs_path_info

    def delete_dag_file(self, dag_id: str) -> bool:
        gcs_path_info = self.__get_gcs_path_info()
        blob: Blob = Blob(gcs_path_info.bucket, get_dag_location(gcs_path_info, dag_id))
        print("DELETE DAG: ${blobId.getBucket}/${blobId.getName}")
        blob.delete()

    def pause_dag(self, dag_id: str) -> DAG:
        pass

    def unpause_dag(self, dag_id: str) -> DAG:
        pass

    def create_secret(self, key: str, value: str) -> bool:
        pass

    def upload_dag(self, dag_id: str, file_path: str):
        pass