import base64
import re
from abc import ABC
import datetime

import boto3
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed, retry

from . import AirflowRestClient
from ...exceptions import DagNotAvailableException, DagFileDeletionFailedException, DagUploadFailedException
from ...project_models import DAG
import requests
import json


class MWAARestClient(AirflowRestClient, ABC):

    def __init__(self, environment_name: str, region: str, access_key: str, secret_key: str):
        self.environment_name = environment_name
        self.aws_s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        self.mwaa_client = boto3.client("mwaa", region_name=region, aws_access_key_id=access_key,
                                        aws_secret_access_key=secret_key)

        self.environment = self.mwaa_client.get_environment(Name=environment_name)
        self.dag_s3_path = self.environment['Environment']['DagS3Path']
        self._source_bucket = self._extract_bucket_name_from_arn()

    def create_secret(self, key: str, value: str) -> bool:
        return True

    def delete_dag_file(self, dag_id: str) -> bool:
        relative_path = f"{self.dag_s3_path}/{dag_id}.zip"
        try:
            self.aws_s3_client.delete_object(Bucket=self._source_bucket, Key=relative_path)
            try:
                self._get_response("dags delete {dag_id} --yes")
                return True
            except Exception:
                return False
        except Exception as e:
            raise DagFileDeletionFailedException(
                f"Error deleting file {relative_path} from bucket {self._source_bucket}", e)

    # todo improve both pause and unpause.
    def pause_dag(self, dag_id: str) -> DAG:
        response_as_text = self._get_response(f"dags pause {dag_id}")
        response_as_json = self._clean_response(response_as_text)
        return DAG(**response_as_json)

    def unpause_dag(self, dag_id: str) -> DAG:
        self.get_dag(dag_id)
        response_as_text = self._get_response(f"dags unpause {dag_id}")
        response_as_json = self._clean_response(response_as_text)
        print(f"Response as json {response_as_json}")
        return DAG.create_from_mwaa(response_as_json)

    def upload_dag(self, dag_id: str, file_path: str):
        relative_path = f"{self.dag_s3_path}/{dag_id}.zip"
        try:
            self.aws_s3_client.upload_file(file_path, self._source_bucket, relative_path)
        except Exception as e:
            print(f"Error uploading file {file_path} to bucket {self._source_bucket}", e)
            raise DagUploadFailedException(f"Error uploading file {file_path} to bucket {self._source_bucket}", e)

    @retry(retry=retry_if_exception_type(DagNotAvailableException), stop=stop_after_attempt(5), wait=wait_fixed(10))
    def get_dag(self, dag_id: str) -> DAG:
        response = self._get_response("dags list -o json")
        dag_list = json.loads(response)
        dag = next((dag for dag in dag_list if dag['dag_id'] == dag_id and dag['paused'] is not None), None)

        if dag is not None:
            return DAG.create_from_mwaa(dag)
        else:
            raise DagNotAvailableException(f"Dag {dag_id} not found")

    def delete_dag(self, dag_id: str) -> str:
        response = self._get_response(f"dags delete {dag_id} --yes")
        return response

    def _create_cli_token(self):
        return self.mwaa_client.create_cli_token(Name=self.environment_name)

    def _expiry_cli_token(self):
        return ExpiringValue(lambda: self._create_cli_token(), 60 * 60 * 24)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._expiry_cli_token().get_value()['CliToken']}",
            "User-Agent": "Prophecy"
        }

    def _clean_response(self, text):
        try:
            return json.loads(text)  # Try to parse the text as JSON
        except json.JSONDecodeError:
            # If unsuccessful, perform string transformations and return the result
            cleaned_text = "{\"" + text.strip() \
                .replace(", ", "\", \"") \
                .replace(": ", "\":\"") \
                .replace(",\"", ", \"") + "\"}"
            return json.loads(cleaned_text)

    def _execute_airflow_command(self, command: str):
        print(f"Expiry cli token {self._expiry_cli_token().get_value()}")
        url = f"https://{self._expiry_cli_token().get_value()['WebServerHostname']}/aws_mwaa/cli"
        response = requests.post(url, headers=self._headers(), data=command)
        response.raise_for_status()  # Raise an HTTPError if the status is 4xx, 5xx
        return response.text  # Get the response body as text

    def _get_response(self, command: str):
        response_body = self._execute_airflow_command(command)

        decoded_response = json.loads(response_body)

        if decoded_response.get('stdout', None) is not None:
            return base64.b64decode(decoded_response['stdout']).decode('utf-8')
        else:
            raise Exception(base64.b64decode(decoded_response['stderr']).decode('utf-8'))

    def _extract_bucket_name_from_arn(self) -> str:
        s3_arn_regex = r"arn:aws:s3:::(.+)"

        match = re.match(s3_arn_regex, self.environment['Environment']['SourceBucketArn'])
        if match:
            return match.group(1)  # Return the captured group
        else:
            raise ValueError(f"No match found for {self.dag_s3_path}")


class ExpiringValue:
    def __init__(self, generate_value, duration):
        self.generate_value = generate_value
        self.duration = duration
        self.value = None
        self.expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=duration)

    def get_value(self):
        if self.value is None or datetime.datetime.now() > self.expiration_time:
            self.value = self.generate_value()
            self.expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=self.duration)
        if self.value is None:
            raise Exception("Failed to generate value")
        return self.value

    def is_valid(self):
        return datetime.datetime.now() < self.expiration_time
