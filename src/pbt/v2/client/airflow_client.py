import os
from abc import ABC, abstractmethod
from typing import Optional, Union
from pathlib import Path
import boto3
import requests
from google.cloud import secretmanager, storage
import re
import json
import requests
from typing import Callable, TypeVar, Optional
import tempfile
from google.cloud.storage import Blob
from google.oauth2.gdch_credentials import ServiceAccountCredentials
from google.cloud.secretmanager_v1 import types

from src.pbt.release.airflow_client import AirflowRestClient
from src.pbt.v2.project_models import DAG


def create_client(content: dict):
    return MWAARestClient(**content)


class AirflowRestClient(ABC):

    @abstractmethod
    def delete_dag_file(self, dag_id: str) -> bool:
        pass

    @abstractmethod
    def pause_dag(self, dag_id: str) -> DAG:
        pass

    @abstractmethod
    def unpause_dag(self, dag_id: str) -> DAG:
        pass

    @abstractmethod
    def create_secret(self, key: str, value: str) -> bool:
        pass

    @abstractmethod
    def upload_dag(self, dag_id: str, file_path: str):
        pass

    @abstractmethod
    def get_dag(self, dag_id: str) -> DAG:
        pass

    @abstractmethod
    def delete_dag(self, dag_id: str) -> str:
        pass


class GCSPathInfo:
    def __init__(self, bucket: str, path: Optional[str]):
        self.bucket = bucket
        self.path = path


class ComposerRestClient(AirflowRestClient, ABC):
    _SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
    _CONNECTIONS_SECRET_PREFIX = "airflow-connections-"

    def __init__(self, airflow_url: str, project_id: str, client_id: Optional[str], key_json: str, dag_location: str,
                 location: Optional[str]):
        self.airflow_url = airflow_url
        self.project_id = project_id
        self.key_json_file = key_json
        self.dag_location = dag_location
        self.location = location
        self.client_id = client_id
        self.file_path = ComposerRestClient.__create_key_json_file(key_json)

        self.storage_handler = storage.Client.from_service_account_json(key_json, project=project_id)

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
        blob: Blob = Blob(gcs_path_info.bucket, self.__get_dag_location(gcs_path_info, dag_id))
        try:
            bucket.blob(blob_name=blob.name).upload_from_filename(file_path)
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
        response = requests.request('GET', f"{self.airflow_url}/api/v1/dags/{dag_id}")
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

    def __set_pause_state(self, dag_id: str, is_paused: bool) -> DAG:
        response = requests.request('PATCH', f"{self.airflow_url}/api/v1/dags/{dag_id}", json={"is_paused": is_paused})
        response.raise_for_status()
        response_data = response.json()

        # Parse JSON into Dag class
        dag = DAG(**response_data)
        return dag


class MWAARestClient(AirflowRestClient, ABC):

    def __init__(self, region: str, access_key: str, secret_key: str, environment_name: str):
        self.environment_name = environment_name
        self.s3_aws_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        self.mwaa_client = boto3.client("mwaa", region_name=region, aws_access_key_id=access_key,
                                        aws_secret_access_key=secret_key)

        self.environment = self.mwaa_client.get_environment(Name=environment_name)
        self.dag_s3_path = self.environment['Environment']['DagS3Path']

    def create_secret(self, key: str, value: str) -> bool:
        return True

    def delete_dag_file(self, dag_id: str) -> bool:
        relative_path = f"{self.dag_s3_path}/{dag_id}.zip"
        source_bucket = self.__extract_bucket_name_from_arn()
        try:
            self.s3_aws_client.delete_object(Bucket=source_bucket, Key=relative_path)
            response = self.__get_response(f"dags delete {dag_id} --yes")
            print(response)
        except Exception as e:
            print(f"Error deleting file {relative_path} from bucket {source_bucket}", e)
            return False
        return True

    # todo improve both pause and unpause.
    def pause_dag(self, dag_id: str) -> DAG:
        response = self.__get_response(f"dags pause {dag_id}")
        return response

    def unpause_dag(self, dag_id: str) -> DAG:
        response = self.__get_response(f"dags unpause {dag_id}")
        return response

    def upload_dag(self, dag_id: str, file_path: str):
        relative_path = f"{self.environment['Environment']['DagS3Path']}/{dag_id}.zip"
        source_bucket = self.__extract_bucket_name_from_arn()
        try:
            self.s3_aws_client.upload_file(file_path, source_bucket, relative_path)
        except Exception as e:
            print(f"Error uploading file {file_path} to bucket {source_bucket}", e)
            raise e

    # todo new Execution Date is not supported as of now.
    def get_dag(self, dag_id: str) -> DAG:
        response = self.__get_response(f"dags list -o json")
        dag_list = json.loads(response)
        dag = next((dag for dag in dag_list if dag['dag_id'] == dag_id and dag['paused'] is not None), None)
        if dag is None:
            raise Exception(f"Dag {dag_id} not found")
        else:
            return DAG(**dag)

    def delete_dag(self, dag_id: str) -> str:
        response = self.__get_response(f"dags delete {dag_id} --yes")
        response.raise_for_status()

        return response.text

    def __execute_airflow_command(self, command: str, headers: dict, hostname: str):
        url = f"https://{hostname}/aws_mwaa/cli"
        response = requests.post(url, headers=headers, data=command)
        response.raise_for_status()  # Raise an HTTPError if the status is 4xx, 5xx
        return response.text  # Get the response body as text

    def __get_response(self, command: str):
        response_body = self.__execute_airflow_command(command)

        decoded_response = json.loads(response_body)

        if not decoded_response['stdout']:
            raise Exception(decoded_response['stderr'])
        else:
            return decoded_response['stdout']

    def __extract_bucket_name_from_arn(self) -> str:
        s3_arn_regex = r"arn:aws:s3:::(.+)"

        match = re.match(s3_arn_regex, self.dag_s3_path)
        if match:
            return match.group(1)  # Return the captured group
        else:
            raise ValueError(f"No match found for {self.dag_s3_path}")
