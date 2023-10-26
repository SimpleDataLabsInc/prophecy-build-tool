from abc import ABC

from requests import HTTPError

from . import PipelineUploader
from ....client.rest_client_factory import RestClientFactory
from ....entities.project import Project
from ....utils.project_config import ProjectConfig
from ....utils.utility import custom_print as log, Either


class DatabricksPipelineUploader(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 to_path: str, file_path: str, file_name: str, fabric_id: str, fabric_name: str):
        self.project = project
        self.project_config = project_config
        self.file_name = file_name
        self.file_path = file_path
        self.pipeline_id = pipeline_id
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name

        self.base_path = project_config.system_config.get_dbfs_base_path()
        self.upload_path = f"{self.base_path}/{to_path}/pipeline/{file_name}"

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)

    def upload_pipeline(self) -> Either:
        client = self.rest_client_factory.databricks_client(self.fabric_id)
        client.upload_src_path(self.file_path, self.upload_path)
        log(f"Uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_name}",
            step_id=self.pipeline_id)
        return Either(right=True)


class DatabricksPipelineUploaderApp(DatabricksPipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str):

        super().__init__(project, project_config, pipeline_id, from_path, to_path, file_name, fabric_id,
                         fabric_name)

    def upload_pipeline(self):
        try:
            return super().upload_pipeline()
        except HTTPError as e:
            response = e.response.content.decode('utf-8')
            log(step_id=self.pipeline_id, message=response)

            if e.response.status_code == 401 or e.response.status_code == 403:
                log(f'Error on uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_name}, but ignoring',
                    exception=e, step_id=self.pipeline_id)
                return Either(right=True)
            else:
                log(f"HttpError on uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_name}",
                    exception=e, step_id=self.pipeline_id)
                return Either(left=e)


        except Exception as e:
            log(f"Unknown Exception while uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_name}, ignoring exception",
                exception=e, step_id=self.pipeline_id)
            return Either(right=True)


class DatabricksPipelineUploaderCli(DatabricksPipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str):
        super().__init__(self, project, project_config, pipeline_id, from_path, to_path, file_name, fabric_id,
                         fabric_name)

    def upload_pipeline(self):
        try:
            return super().upload_pipeline()
        except Exception as e:
            log(f"Exception on uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_name}",
                exception=e, step_id=self.pipeline_id)
            return Either(right=False)
