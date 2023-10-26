import os
from abc import ABC, abstractmethod
from typing import List

from . import PipelineUploader
from .databricks import DatabricksPipelineUploaderApp, DatabricksPipelineUploaderCli
from .dataproc import DataprocPipelineUploaderApp, DataprocPipelineUploaderCli
from .dummy import DummyPipelineUploader
from .emr import EMRPipelineUploaderApp, EMRPipelineUploaderCli
from ....entities.project import Project
from ....utils.exceptions import PipelinePathNotFoundException
from ....utils.project_config import ProjectConfig, EMRInfo, DataprocInfo
from ....utils.project_models import LogLevel, Status
from ....utils.utility import custom_print as log, Either


class PipelineUploadManager(PipelineUploader, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 all_fabrics: List[str], from_path: str):
        self.project = project
        self.project_config = project_config
        self.from_path = from_path
        self.pipeline_id = pipeline_id
        self.all_fabrics = all_fabrics

    @abstractmethod
    def create_databricks_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                            fabric_id: str, fabric_name: str):
        pass

    @abstractmethod
    def create_emr_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                     fabric_id: str, fabric_name: str, emr_info: EMRInfo):
        pass

    @abstractmethod
    def create_dataproc_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                          fabric_id: str, fabric_name: str, dataproc_info: DataprocInfo):
        pass

    def upload_pipeline(self):
        try:
            if self.from_path is None:
                raise PipelinePathNotFoundException(f"Pipeline path not found {self.pipeline_id}")

            file_name_with_extension = os.path.basename(self.from_path)
            file_name = file_name_with_extension.replace("-1.0.jar", ".jar") if file_name_with_extension.endswith(
                "-1.0.jar") else file_name_with_extension

            subscribed_project_id, subscribed_project_release_version, path = Project.is_cross_project_pipeline(
                self.from_path)

            if subscribed_project_id is not None:
                to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
            else:
                to_path = f"{self.project.project_id}/{self.project.release_version}"

            responses = []

            for fabric_id in self.all_fabrics:
                try:
                    fabric_info = self.project_config.fabric_config.get_fabric(fabric_id)
                    fabric_name = fabric_info.name
                    db_info = fabric_info.databricks
                    emr_info = fabric_info.emr
                    dataproc_info = fabric_info.dataproc

                    if not db_info:
                        pipeline_uploader = self.create_databricks_pipeline_uploader(self.pipeline_id, to_path,
                                                                                     self.from_path,
                                                                                     file_name,
                                                                                     fabric_id, fabric_name)

                    elif not emr_info:
                        pipeline_uploader = self.create_emr_pipeline_uploader(self.pipeline_id, self.from_path, to_path,
                                                                              file_name, fabric_id, fabric_name,
                                                                              emr_info)

                    elif not dataproc_info:
                        pipeline_uploader = self.create_dataproc_pipeline_uploader(self.pipeline_id, self.from_path,
                                                                                   to_path,
                                                                                   file_name, fabric_id, fabric_name,
                                                                                   dataproc_info)

                    else:
                        log(f"Fabric {fabric_id} is not supported for pipeline upload", step_id=self.pipeline_id)
                        pipeline_uploader = DummyPipelineUploader()

                    responses.append(pipeline_uploader.upload_pipeline())

                except Exception as e:
                    log(f"Error while uploading pipeline {self.pipeline_id} for fabric {fabric_id}",
                        step_id=self.pipeline_id, exception=e, level=LogLevel.TRACE)
                    log(step_status=Status.FAILED, step_id=self.pipeline_id)
                    responses.append(Either(left=e))

            if all([response.is_right for response in responses]):
                log(step_status=Status.SUCCEEDED, step_id=self.pipeline_id)
                return Either(right=True)
            else:
                log(step_status=Status.FAILED, step_id=self.pipeline_id)
                return Either(left=responses)

        except Exception as e:
            log(f"Error while uploading pipeline {self.pipeline_id}", step_id=self.pipeline_id,
                exception=e)
            log(step_status=Status.FAILED, step_id=self.pipeline_id)
            return Either(left=e)


class PipelineUploadManagerApp(PipelineUploadManager, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 all_fabrics: List[str], from_path: str):
        super().__init__(project, project_config, pipeline_id, all_fabrics, from_path)

    def create_databricks_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                            fabric_id: str, fabric_name: str):
        return DatabricksPipelineUploaderApp(self.project, self.project_config,
                                             self.pipeline_id, to_path, self.from_path,
                                             file_name,
                                             fabric_id, fabric_name)

    def create_emr_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                     fabric_id: str, fabric_name: str, emr_info: EMRInfo):
        return EMRPipelineUploaderApp(self.project, self.project_config,
                                      self.pipeline_id, self.from_path, to_path,
                                      file_name, fabric_id, fabric_name, emr_info)

    def create_dataproc_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                          fabric_id: str, fabric_name: str, dataproc_info: DataprocInfo):
        return DataprocPipelineUploaderApp(self.project, self.project_config,
                                           self.pipeline_id, self.from_path, to_path,
                                           file_name, fabric_id, fabric_name,
                                           dataproc_info)


class PipelineUploadManagerCli(PipelineUploadManager, ABC):
    def __init__(self, project: Project, project_config: ProjectConfig, pipeline_id: str,
                 all_fabrics: List[str], from_path: str):
        super().__init__(project, project_config, pipeline_id, all_fabrics, from_path)

    def create_databricks_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                            fabric_id: str, fabric_name: str):
        return DatabricksPipelineUploaderCli(self.project, self.project_config,
                                             self.pipeline_id, to_path, self.from_path,
                                             file_name,
                                             fabric_id, fabric_name)

    def create_emr_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                     fabric_id: str, fabric_name: str, emr_info: EMRInfo):
        return EMRPipelineUploaderCli(self.project, self.project_config,
                                      self.pipeline_id, self.from_path, to_path,
                                      file_name, fabric_id, fabric_name, emr_info)

    def create_dataproc_pipeline_uploader(self, pipeline_id: str, to_path: str, from_path: str, file_name: str,
                                          fabric_id: str, fabric_name: str, dataproc_info: DataprocInfo):
        return DataprocPipelineUploaderCli(self.project, self.project_config,
                                           self.pipeline_id, self.from_path, to_path,
                                           file_name, fabric_id, fabric_name,
                                           dataproc_info)
