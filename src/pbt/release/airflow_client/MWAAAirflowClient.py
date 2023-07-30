from abc import ABC, abstractmethod
from typing import Optional
from google.cloud.storage.blob import Blob
import re

from .AirflowClient import AirflowRestClient


class MWAAAirflowClient(AirflowRestClient, ABC):
    def __init__(self):


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