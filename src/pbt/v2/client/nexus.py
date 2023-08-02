import json
import os
from pathlib import Path

import requests
from urllib.parse import unquote


class NexusClient:

    def __init__(self, url: str, username: str, password: str):
        self.url = url
        self.username = username
        self.password = password

    @staticmethod
    def from_url(url: str):
        return NexusClient(url, os.getenv('NexusUsername'), os.getenv('NexusPassword'))

    def upload_file(self, file_path: str, project_id: str, pipeline_name: str, release_version: str):
        url = f"{self.url}/nexus/repository/{project_id}/{pipeline_name}/{release_version}/{file_path}"
        with open(file_path, 'rb') as file:
            requests.put(url, auth=(self.username, self.password), data=file.read())

    def download_file(self, file_path: str, project_id: str, pipeline_name: str, release_version: str):
        url = f"{self.url}/nexus/repository/{project_id}/{pipeline_name}/{release_version}/{file_path}"
        with open(file_path, 'wb') as file:
            response = requests.get(url, auth=(self.username, self.password))
            download_url = json.loads(response.content.decode('utf-8')).get('downloadUrl', None)

            # Send a HTTP request to the URL of the file
            response = requests.get(download_url, stream=True)

            # Check if the request was successful
            if response.status_code == 200:
                # Get the filename from the URL
                filename = unquote(Path(download_url).name)

                # Write the contents of the response to a file
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=128):
                        f.write(chunk)
            else:
                print(f'Request failed with status code {response.status_code}')
