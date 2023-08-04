import json
import os
import tempfile
from pathlib import Path

import requests
from urllib.parse import unquote

from requests.auth import HTTPBasicAuth

from src.pbt.v2.exceptions import ArtifactDownloadFailedException
from src.pbt.v2.utility import Either


class NexusClient:

    def __init__(self, base_url: str, username: str, password: str, repository: str):

        self.base_url = base_url  # base-url https://execution.dev.cloud.prophecy.io/artifactory/
        self.username = username
        self.password = password
        self.repository = repository

    def upload_file(self, file_path: str, project_id: str, pipeline_name: str, release_version: str):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'multipart/form-data',
        }
        url = f'{self.base_url}/service/rest/v1/components?repository={self.repository}'
        data = {
            'raw.directory': f'/prophecy/artifact/{self._create_file_path(project_id, release_version, pipeline_name)}',
            'raw.asset1': file_path,
            'raw.asset1.filename': os.path.basename(file_path),
        }

        response = requests.post(url, headers=headers, data=data, auth=HTTPBasicAuth(self.username, self.password))
        return response

    '''
            for now this we will create on our own, but ideally this should be created with the help of pom.xml and setup.py.
            jar - pipeline_name-1.0.jar
            wheel - pipeline_name-1.0-py3-none-any.whl 
    '''

    def download_file(self, file_name: str, project_id: str, release_version: str, pipeline_name: str):
        # https: // execution.dev.cloud.prophecy.io / artifactory / repository / raw_host / ultra / demo / gamma / livy_scala.jar
        url = f'{self.base_url}/repository/{self.repository}/{self._create_file_path(project_id, release_version, pipeline_name)}/{file_name}'
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as tmp_dir:
                file_path = os.path.join(tmp_dir, file_name)
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                print(f"File downloaded at: {file_path}")
                Either(right=file_path)
        else:
            print(f"Unable to download file. HTTP response code: {response.status_code}")

            Either(left=ArtifactDownloadFailedException(response))

    def _create_file_path(self, project_id: str, release_version: str, pipeline_name):
        # todo cleanup.
        return f'project_{project_id}/release_version_{release_version}/pipeline_{pipeline_name}'

    @classmethod
    def initialize_nexus_client(cls, state_config_and_db_tokens):
        pass
