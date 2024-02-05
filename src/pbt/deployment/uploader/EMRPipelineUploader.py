from abc import ABC

from . import PipelineUploader
from ...client.rest_client_factory import RestClientFactory
from ...entities.project import Project
from ...utility import Either, custom_print as log
from ...utils.project_config import EMRInfo, ProjectConfig
from ...utils.project_models import Colors


class EMRPipelineUploader(PipelineUploader, ABC):
    def __init__(
        self,
        project: Project,
        project_config: ProjectConfig,
        pipeline_id: str,
        from_path: str,
        to_path: str,
        file_name: str,
        fabric_id: str,
        fabric_name: str,
        emr_info: EMRInfo,
    ):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id

        self.from_path = from_path
        self.to_path = to_path
        self.file_name = file_name
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name

        self.emr_info = emr_info
        self.file_name = file_name

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.base_path = self.project_config.system_config.get_s3_base_path()
        self.upload_path = (
            f"{self.emr_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{self.file_name}".lstrip("/")
        )

    def upload_pipeline(self, path: str):
        try:
            client = self.rest_client_factory.s3_client(self.fabric_id)
            client.upload_file(self.emr_info.bare_bucket(), self.upload_path, self.from_path)

            log(
                f"{Colors.OKGREEN}Uploaded pipeline to s3, from path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name}{Colors.ENDC}",
                step_id=self.pipeline_id,
                indent=2,
            )

            if self.project.project_language == "python":
                content = self.project.get_py_pipeline_main_file(self.pipeline_id)
                pipeline_name = self.pipeline_id.split("/")[1]
                launcher_path = f"{self.emr_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{pipeline_name}/launcher.py".lstrip(
                    "/"
                )
                client.upload_content(self.emr_info.bare_bucket(), launcher_path, content)

                log(
                    f"{Colors.OKGREEN}Uploading py pipeline launcher to to-path {launcher_path} for fabric {self.fabric_name}{Colors.ENDC}",
                    step_id=self.pipeline_id,
                    indent=2,
                )
            return Either(right=True)
        except Exception as e:
            log(
                f"{Colors.WARNING}Unknown Exception while uploading pipeline to emr, from-path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name}, Ignoring{Colors.ENDC}",
                exception=e,
                step_id=self.pipeline_id,
                indent=2,
            )
            return Either(right=True)

    def exists(self) -> bool:
        return False
