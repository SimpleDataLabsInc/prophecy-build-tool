from abc import ABC

from requests import HTTPError

from . import PipelineUploader
from ...client.rest_client_factory import RestClientFactory
from ...deployment.jobs.databricks import get_fabric_label
from ...entities.project import Project
from ...utility import Either, custom_print as log
from ...utils.project_config import ProjectConfig
from ...utils.project_models import Colors


class DatabricksPipelineUploader(PipelineUploader, ABC):
    def __init__(
        self,
        project: Project,
        project_config: ProjectConfig,
        pipeline_id: str,
        to_path: str,
        file_path: str,
        file_name: str,
        fabric_id: str,
        fabric_name: str,
    ):
        self.project = project
        self.project_config = project_config
        self.file_name = file_name
        self.file_path = file_path
        self.pipeline_id = pipeline_id
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name
        self.fabric_label = get_fabric_label(fabric_name, fabric_id)

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.base_path = self.project_config.get_db_base_path(None)
        self.is_volume_supported = self.project_config.is_volume_supported(fabric_id)
        self.upload_path = f"{self.base_path}/{self.to_path}/pipeline/{self.file_name}"
        if self.is_volume_supported:
            self.volume_based_path = (
                f"{self.project_config.get_db_base_path(fabric_id)}/{self.to_path}/pipeline/{self.file_name}"
            )

    def upload_pipeline(self, path: str) -> Either:
        try:
            client = self.rest_client_factory.databricks_client(self.fabric_id)
            client.upload_src_path(self.file_path, self.upload_path)
            log(
                f"Uploading pipeline to databricks from-path `{self.file_path}` to path `{self.upload_path}` for fabric `{self.fabric_label}`",
                step_id=self.pipeline_id,
                indent=2,
            )
            if self.is_volume_supported:
                # why this is important ?
                # use of volume is hinged on providing volume prefix and
                # on databricks runtime version
                # we need to upload to both these places.
                client.upload_src_path(self.file_path, self.volume_based_path)
                log(
                    f"Uploading pipeline to databricks from-path `{self.file_path}` to volume based path `{self.volume_based_path}` for fabric `{self.fabric_label}`",
                    step_id=self.pipeline_id,
                    indent=2,
                )

            return Either(right=True)

        except HTTPError as e:
            response = e.response.content.decode("utf-8")
            log(step_id=self.pipeline_id, message=response)

            if e.response.status_code == 401 or e.response.status_code == 403:
                log(
                    f"{Colors.WARNING}Error on uploading pipeline to databricks from path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}, but ignoring{Colors.ENDC}",
                    exception=e,
                    step_id=self.pipeline_id,
                    indent=2,
                )
                return Either(right=True)
            else:
                log(
                    f"{Colors.FAIL}HttpError on uploading pipeline to databricks from-path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}{Colors.ENDC}",
                    exception=e,
                    step_id=self.pipeline_id,
                    indent=2,
                )
                return Either(left=e)

        except Exception as e:
            log(
                f"{Colors.FAIL}Unknown Exception on uploading pipeline to databricks from-path {self.file_path} to path {self.upload_path} for fabric {self.fabric_label}, ignoring exception{Colors.ENDC}",
                exception=e,
                step_id=self.pipeline_id,
                indent=2,
            )
            return Either(right=True)

    def exists(self) -> bool:
        try:
            log(f"Checking if path {self.upload_path} already exists.", self.pipeline_id, indent=2)
            client = self.rest_client_factory.databricks_client(self.fabric_id)
            return client.path_exist(self.upload_path)
        except Exception as e:
            log(
                f"{Colors.WARNING} Failed checking path {self.upload_path}{Colors.ENDC}",
                step_id=self.pipeline_id,
                exception=e,
                indent=2,
            )
            return False
