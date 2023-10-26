from abc import ABC

from . import PipelineUploader
from ....client.rest_client_factory import RestClientFactory
from ....entities.project import Project
from ....utils.project_config import ProjectConfig, DataprocInfo
from ....utils.utility import custom_print as log, Either


class DataprocPipelineUploader(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str,
                 dataproc_info: DataprocInfo, upload_path: str):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id

        self.upload_path = upload_path
        self.from_path = from_path
        self.to_path = to_path
        self.fabric_id = fabric_id
        self.fabric_name = fabric_name
        self.dataproc_info = dataproc_info
        self.file_name = file_name
        self.base_path = self.project_config.system_config.get_s3_base_path()

        self.upload_path = f"{dataproc_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{self.file_name}".lstrip(
            "/")

        self.rest_client_factory = RestClientFactory.get_instance(RestClientFactory, project_config.fabric_config)

    def upload_pipeline(self):
        client = self.rest_client_factory.dataproc_client(self.fabric_name)
        client.put_object_from_file(self.dataproc_info.bare_bucket(), self.upload_path, self.from_path)

        log(f"Uploaded pipeline to data-proc, from path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name}",
            step_id=self.pipeline_id)

        if self.project.project_language == "python":
            content = self.project.get_py_pipeline_main_file(self.pipeline_id)
            pipeline_name = self.pipeline_id.split("/")[1]
            launcher_path = f"{self.dataproc_info.bare_path_prefix()}/{self.base_path}/{self.to_path}/pipeline/{pipeline_name}/launcher.py".lstrip(
                '/')
            client.put_object(self.dataproc_info.bare_bucket(), launcher_path, content)

            log(f"Uploading py pipeline launcher to path {launcher_path} and bucket {self.dataproc_info.bare_bucket()} for fabric {self.fabric_name}",
                step_id=self.pipeline_id)
        return Either(right=True)


class DataprocPipelineUploaderApp(DataprocPipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str,
                 dataproc_info: DataprocInfo):

        super().__init__(project, project_config, pipeline_id, from_path, to_path, file_name, fabric_id,
                         fabric_name, dataproc_info)

    def upload_pipeline(self):
        try:
            return super().upload_pipeline()
        except Exception as e:
            log(f"Unknown Exception on uploading pipeline to data-proc, from path {self.from_path} to path {self.upload_path} for fabric {self.fabric_name} , ignoring exception",
                exception=e, step_id=self.pipeline_id)
            return Either(right=True)


class DataprocPipelineUploaderCli(DataprocPipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 from_path: str, to_path: str, file_name: str, fabric_id: str, fabric_name: str,
                 dataproc_info: DataprocInfo):

        super().__init__(project, project_config, pipeline_id, from_path, to_path, file_name, fabric_id,
                         fabric_name, dataproc_info)

    def upload_pipeline(self):
        try:
            return super().upload_pipeline()
        except Exception as e:
            log(f"Unknown Exception while uploading pipeline to data-proc, from-path {self.from_path} to to-path {self.upload_path} for fabric {self.fabric_name}, ignoring exception",
                exception=e, step_id=self.pipeline_id)
            return Either(right=False)
