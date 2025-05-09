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
        self.is_volume_supported = (
            self.project_config.is_volume_supported(fabric_id)
            or fabric_id in self.project.fabric_volumes_detected.keys()
        )
        self.upload_path = f"{self.base_path}/{self.to_path}/pipeline/{self.file_name}"
        if self.is_volume_supported:
            if self.project_config.is_volume_supported(fabric_id):
                # NOTE: if both project config (fabrics.yml) and job definitions contain volume paths, then by having
                # this case first, we prioritize the volume path defined in the fabrics.yml.
                self.volume_based_path = (
                    f"{self.project_config.get_db_base_path(fabric_id)}/{self.to_path}/pipeline/{self.file_name}"
                )
            elif fabric_id in self.project.fabric_volumes_detected.keys():
                volume_opt = self.project.fabric_volumes_detected[fabric_id]
                self.volume_based_path = f"{self.project_config.system_config.get_dbfs_base_path(volume_opt)}/{self.to_path}/pipeline/{self.file_name}"
            else:
                # This case should ideally not be reached if is_volume_supported is true due to the checks above,
                # but kept for robustness. It was previously a NotImplementedError.
                log(
                    f"{Colors.WARNING}Volume path determination failed for fabric {self.fabric_label} despite is_volume_supported being true. "
                    f"Volume must be defined in jobs or in project config (fabrics.yml). Skipping volume-based upload for this reason.{Colors.ENDC}",
                    step_id=self.pipeline_id,
                )
                # Effectively disable volume upload if path cannot be determined
                self.is_volume_supported = False
                # self.volume_based_path will not be used if is_volume_supported is False

    def _attempt_single_upload(
        self, client, local_path: str, remote_path: str, path_description: str
    ) -> tuple[bool, HTTPError]:
        try:
            client.upload_src_path(local_path, remote_path)
            log(
                f"Uploading pipeline to databricks from-path `{local_path}` to {path_description} `{remote_path}` for fabric `{self.fabric_label}`",
                step_id=self.pipeline_id,
                indent=2,
            )
            return False, None  # No hard error
        except HTTPError as e:
            response = e.response.content.decode("utf-8")
            log(step_id=self.pipeline_id, message=response)
            if e.response.status_code == 401 or e.response.status_code == 403:
                log(
                    f"{Colors.WARNING}Error on uploading pipeline to databricks from path {local_path} to {path_description} {remote_path} for fabric {self.fabric_label}, but ignoring{Colors.ENDC}",
                    exception=e,
                    step_id=self.pipeline_id,
                    indent=2,
                )
                return False, None  # Ignored HTTPError, not a hard error
            else:
                log(
                    f"{Colors.FAIL}HttpError on uploading pipeline to databricks from-path {local_path} to {path_description} {remote_path} for fabric {self.fabric_label}{Colors.ENDC}",
                    exception=e,
                    step_id=self.pipeline_id,
                    indent=2,
                )
                return True, e  # Hard error
        except Exception as e:
            # This matches the original behavior where general exceptions are logged with FAIL
            # but result in an overall Either(right=True), meaning they are "ignored" for failure status.
            log(
                f"{Colors.FAIL}Unknown Exception on uploading pipeline to databricks from-path {local_path} to {path_description} {remote_path} for fabric {self.fabric_label}, ignoring exception{Colors.ENDC}",
                exception=e,
                step_id=self.pipeline_id,
                indent=2,
            )
            return False, None  # Ignored Exception, not a hard error

    def upload_pipeline(self, path: str) -> Either:  # 'path' param is from PipelineUploader, unused here
        client = self.rest_client_factory.databricks_client(self.fabric_id)
        first_hard_error: HTTPError = None

        # Attempt 1: Standard Upload Path
        is_hard_error, error_obj = self._attempt_single_upload(client, self.file_path, self.upload_path, "path")
        if is_hard_error and first_hard_error is None:
            first_hard_error = error_obj

        # Attempt 2: Volume-Based Upload Path (if applicable and path was determined)
        # This will run regardless of the outcome of the first attempt.
        if self.is_volume_supported and hasattr(self, "volume_based_path"):
            is_hard_error_volume, error_obj_volume = self._attempt_single_upload(
                client, self.file_path, self.volume_based_path, "volume based path"
            )
            if is_hard_error_volume and first_hard_error is None:
                first_hard_error = error_obj_volume
        elif self.is_volume_supported and not hasattr(self, "volume_based_path"):
            # This case handles where is_volume_supported was true initially,
            # but volume_based_path couldn't be determined in __init__.
            # The log for this scenario is already in __init__.
            pass

        if first_hard_error:
            return Either(left=first_hard_error)
        else:
            # If no hard error occurred in any attempt, it's considered an overall success.
            return Either(right=True)

    def exists(self) -> bool:
        try:
            log(f"Checking if path {self.upload_path} already exists.", self.pipeline_id, indent=2)
            client = self.rest_client_factory.databricks_client(self.fabric_id)
            # Note: This only checks the primary upload_path.
            # If exists() behavior needs to consider volume_based_path, that logic would also need adjustment.
            return client.path_exist(self.upload_path)
        except Exception as e:
            log(
                f"{Colors.WARNING} Failed checking path {self.upload_path}{Colors.ENDC}",
                step_id=self.pipeline_id,
                exception=e,
                indent=2,
            )
            return False
