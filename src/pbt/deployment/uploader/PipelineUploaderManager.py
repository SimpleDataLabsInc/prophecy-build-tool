import os
from abc import ABC
from typing import List

from . import PipelineUploader
from .DatabricksPipelineUploader import DatabricksPipelineUploader
from .DataprocPipelineUploader import DataprocPipelineUploader
from .EMRPipelineUploader import EMRPipelineUploader
from .HdfsPipelineUploader import HdfsPipelineUploader
from ...entities.project import Project, is_cross_project_pipeline
from ...utility import Either, custom_print as log, get_package_name
from ...utils.project_config import ProjectConfig
from ...utils.project_models import Colors, LogLevel, Status


class PipelineUploadManager(PipelineUploader, ABC):
    def __init__(
        self,
        project: Project,
        project_config: ProjectConfig,
        pipeline_id: str,
        pipeline_name: str,
        all_fabrics: List[str],
    ):
        self.project = project
        self.project_config = project_config
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.all_fabrics = all_fabrics

    def upload_pipeline(self, from_path: str):
        try:
            if from_path is None:
                raise Exception(f"Pipeline build failed {self.pipeline_id}")

            file_name_with_extension = os.path.basename(from_path)

            if file_name_with_extension.endswith("-1.0.jar"):
                # scala based pipeline
                file_name = file_name_with_extension.replace("-1.0.jar", ".jar")
            else:
                # python based pipeline they are correctly generated.
                file_name = file_name_with_extension

            subscribed_project_id, subscribed_project_release_version, path = is_cross_project_pipeline(
                self.pipeline_id
            )

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
                    oss_info = fabric_info.airflow_oss

                    if db_info is not None:
                        pipeline_uploader = DatabricksPipelineUploader(
                            self.project,
                            self.project_config,
                            self.pipeline_id,
                            to_path,
                            from_path,
                            file_name,
                            fabric_id,
                            fabric_name,
                        )

                    elif emr_info is not None:
                        pipeline_uploader = EMRPipelineUploader(
                            self.project,
                            self.project_config,
                            self.pipeline_id,
                            from_path,
                            to_path,
                            file_name,
                            fabric_id,
                            fabric_name,
                            emr_info,
                        )

                    elif dataproc_info is not None:
                        pipeline_uploader = DataprocPipelineUploader(
                            self.project,
                            self.project_config,
                            self.pipeline_id,
                            from_path,
                            to_path,
                            file_name,
                            fabric_id,
                            fabric_name,
                            dataproc_info,
                            subscribed_project_id is not None,
                        )

                    elif oss_info is not None:
                        pipeline_uploader = HdfsPipelineUploader(
                            self.project,
                            self.project_config,
                            self.pipeline_id,
                            from_path,
                            to_path,
                            file_name,
                            fabric_id,
                            fabric_name,
                            oss_info,
                        )

                    else:
                        log(
                            f"{Colors.WARNING}Fabric id `{fabric_id}` with name `{fabric_name}` is not supported for pipeline upload{Colors.ENDC}",
                            step_id=self.pipeline_id,
                        )
                        pipeline_uploader = DummyPipelineUploader()

                    responses.append(pipeline_uploader.upload_pipeline(""))

                except Exception as e:
                    log(
                        f"{Colors.FAIL}Error while uploading pipeline {self.pipeline_id} for fabric {fabric_id}{Colors.ENDC}",
                        step_id=self.pipeline_id,
                        exception=e,
                        level=LogLevel.TRACE,
                        indent=2,
                    )
                    log(step_status=Status.FAILED, step_id=self.pipeline_id)
                    responses.append(Either(left=e))

            if all([response.is_right for response in responses]):
                log(step_status=Status.SUCCEEDED, step_id=self.pipeline_id)
                return Either(right=True)
            else:
                log(step_status=Status.FAILED, step_id=self.pipeline_id)
                return Either(left=responses)

        except Exception as e:
            log(
                f"{Colors.FAIL}Error while uploading pipeline {self.pipeline_id}{Colors.ENDC}",
                step_id=self.pipeline_id,
                exception=e,
                indent=2,
            )
            log(step_status=Status.FAILED, step_id=self.pipeline_id)
            return Either(left=e)

    def exists(self) -> bool:
        response = True
        file_name = get_package_name(self.project.project_language, self.pipeline_name)

        subscribed_project_id, subscribed_project_release_version, path = self.project.is_cross_project_pipeline(
            self.pipeline_id
        )

        if subscribed_project_id is not None:
            to_path = f"{subscribed_project_id}/{subscribed_project_release_version}"
        else:
            to_path = f"{self.project.project_id}/{self.project.release_version}"

        for fabric_id in self.all_fabrics:
            fabric_info = self.project_config.fabric_config.get_fabric(fabric_id)
            fabric_name = fabric_info.name
            db_info = fabric_info.databricks
            emr_info = fabric_info.emr
            dataproc_info = fabric_info.dataproc
            oss_info = fabric_info.airflow_oss

            if db_info is not None:
                pipeline_uploader = DatabricksPipelineUploader(
                    self.project, self.project_config, self.pipeline_id, to_path, "", file_name, fabric_id, fabric_name
                )

            elif emr_info is not None:
                pipeline_uploader = EMRPipelineUploader(
                    self.project,
                    self.project_config,
                    self.pipeline_id,
                    to_path,
                    "",
                    file_name,
                    fabric_id,
                    fabric_name,
                    emr_info,
                )

            elif dataproc_info is not None:
                pipeline_uploader = DataprocPipelineUploader(
                    self.project,
                    self.project_config,
                    self.pipeline_id,
                    to_path,
                    "",
                    file_name,
                    fabric_id,
                    fabric_name,
                    dataproc_info,
                    subscribed_project_id is not None,
                )

            elif oss_info is not None:
                pipeline_uploader = HdfsPipelineUploader(
                    self.project,
                    self.project_config,
                    self.pipeline_id,
                    to_path,
                    "",
                    file_name,
                    fabric_id,
                    fabric_name,
                    dataproc_info,
                )
            else:
                pipeline_uploader = DummyPipelineUploader()

            response = response and pipeline_uploader.exists()

        return response


class DummyPipelineUploader(PipelineUploader, ABC):
    def upload_pipeline(self, path: str):
        return Either(right=True)

    def exists(self) -> bool:
        return True
