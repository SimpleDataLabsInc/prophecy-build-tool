from abc import ABC

from . import PipelineUploader
from ...client.rest_client_factory import RestClientFactory
from ...deployment.jobs.databricks import get_fabric_label
from ...entities.project import Project
from ...utility import Either, custom_print as log, python_pipeline_name
from ...utils.project_config import OpenSourceAirflowInfo, ProjectConfig
from ...utils.project_models import Colors


class HdfsPipelineUploader(PipelineUploader, ABC):
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
        oss_info: OpenSourceAirflowInfo,
    ):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id

        self.from_path = from_path
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name
        self.fabric_label = get_fabric_label(fabric_name, fabric_id)

        self.oss_info = oss_info
        self.file_name = file_name
        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)
        self.prophecy_base_path = self.project_config.system_config.get_hdfs_base_path()
        self.root_location = self.oss_info.location.rstrip("/")
        self.upload_directory = f"{self.root_location}/{self.prophecy_base_path}/{self.to_path}/pipeline"

    def upload_pipeline(self, path: str):
        try:
            client = self.rest_client_factory.open_source_hdfs_client(self.fabric_id)
            client.put_object_from_file(self.upload_directory, self.file_name, self.from_path)

            log(
                f"{Colors.OKGREEN}Uploaded pipeline to HDFS, from-path {self.from_path} to-path {self.upload_directory} for fabric {self.fabric_label}{Colors.ENDC}",
                step_id=self.pipeline_id,
                indent=2,
            )

            if self.project.project_language == "python":
                content = self.project.get_py_pipeline_main_file(self.pipeline_id)
                pipeline_name = python_pipeline_name(self.pipeline_id.split("/")[1])
                launcher_file_name = "launcher.py"
                launcher_directory = (
                    f"{self.root_location}/{self.prophecy_base_path}/{self.to_path}/pipeline/{pipeline_name}"
                )
                client.put_object(launcher_directory, launcher_file_name, content)

                log(
                    f"{Colors.OKGREEN}Uploading py pipeline launcher to to-path {launcher_directory} for fabric {self.fabric_label}{Colors.ENDC}",
                    step_id=self.pipeline_id,
                    indent=2,
                )
            return Either(right=True)

        except Exception as e:
            log(
                f"{Colors.WARNING}Unknown Exception while uploading pipeline to data-proc, from-path {self.from_path} to to-path {self.upload_directory} for fabric {self.fabric_label}, ignoring exception{Colors.ENDC}",
                exception=e,
                step_id=self.pipeline_id,
                indent=2,
            )
            return Either(right=True)

    def exists(self) -> bool:
        return True
