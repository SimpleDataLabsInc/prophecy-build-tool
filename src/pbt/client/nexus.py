import os
import tempfile

import requests

from requests.auth import HTTPBasicAuth

from ..utils.exceptions import ArtifactDownloadFailedException


class NexusClient:
    def __init__(self, base_url: str, username: str, password: str, repository: str):
        self._base_url = base_url  # base-url https://execution.dev.cloud.prophecy.io/artifactory/
        self._username = username
        self._password = password
        self._repository = repository

    @classmethod
    def initialize_nexus_client(cls, project_config):
        nexus_config = project_config.system_config.nexus

        if nexus_config is not None:
            return NexusClient(nexus_config.url, nexus_config.username, nexus_config.password, nexus_config.repository)
        else:
            raise Exception("Nexus config not found in deployment config, fallbacking to default settings.")

    def upload_file(self, file_path: str, project_id: str, pipeline_id: str, release_version: str, file_name: str):
        headers = {"accept": "application/json"}
        url = f"{self._base_url}/service/rest/v1/components?repository={self._repository}"
        data = {
            "raw.directory": f"/prophecy/artifact/{self._create_file_path(project_id, release_version, pipeline_id)}",
            "raw.asset1.filename": file_name,
        }

        files = {"raw.asset1": (file_name, open(file_path, "rb"), "application/java-archive")}

        print("Uploading file to: ", data)
        response = requests.post(
            url, headers=headers, data=data, auth=HTTPBasicAuth(self._username, self._password), files=files
        )
        return response

    """
            for now this we will create on our own, but ideally this should be created with the help of pom.xml and setup.py.
            jar - pipeline_name-1.0.jar
            wheel - pipeline_name-1.0-py3-none-any.whl
    """

    def download_file(self, file_name: str, project_id: str, release_version: str, pipeline_name: str):
        # https: // execution.dev.cloud.prophecy.io / artifactory / repository / raw_host / ultra / demo / gamma / livy_scala.jar
        url = f"{self._base_url}/repository/{self._repository}/prophecy/artifact/{self._create_file_path(project_id, release_version, pipeline_name)}/{file_name}"
        print("Downloading file from: ", url)
        response = requests.get(url, stream=True)

        try:
            # Create a temporary directory
            tmp_dir = tempfile.mkdtemp()
            file_path = os.path.join(tmp_dir, file_name)
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            print(f"File downloaded at: {file_path}")
            return file_path
        except Exception as e:
            print("Unable to download file. HTTP response code: ", e)

            raise ArtifactDownloadFailedException(e)

    def _create_file_path(self, project_id: str, release_version: str, pipeline_name: str):
        # todo cleanup.
        return f"project_{project_id}/release_version_{release_version}/pipelines_{pipeline_name}".replace("\\W", "_")
