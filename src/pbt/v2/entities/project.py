import os
from typing import Optional

import yaml

from ..constants import PBT_FILE_NAME, LANGUAGE, JOBS, PIPELINES, \
    PIPELINE_CONFIGURATIONS, CONFIGURATIONS, JSON_EXTENSION, BASE_PIPELINE, PROJECT_ID_PLACEHOLDER_REGEX, \
    PROJECT_RELEASE_VERSION_PLACEHOLDER_REGEX
import re

from ..exceptions import ProjectPathNotFoundException, ProjectFileNotFoundException


class Project:
    _DATABRICKS_JOB_JSON = "databricks-job.json"

    def __init__(self, project_path: str, project_id: Optional[str] = None, release_version: Optional[str] = None):

        self.project_id = project_id
        self.release_version = release_version
        self.project_path = project_path

        self.pbt_project_dict = {}
        self.project_language = None
        self.jobs = None
        self.pipelines = None
        self.pipeline_id_to_name = {}
        self.pipeline_configurations = {}

        self._verify_project()
        self._load_project_config()
        self._extract_project_info()
        self._load_pipeline_configurations()

    def load_databricks_job(self, job_id: str) -> Optional[str]:
        content = None
        try:
            with open(os.path.join(self.project_path, job_id, "code", self._DATABRICKS_JOB_JSON), "r") as file:
                content = self._replace_placeholders(self._DATABRICKS_JOB_JSON, file.read())
        except FileNotFoundError:
            content = None

        return content

    def load_airflow_base_folder_path(self, job_id):
        return os.path.join(self.project_path, job_id, "code")

    def load_airflow_folder(self, job_id):
        rdc = {}
        base_path = os.path.join(self.project_path, job_id, "code")
        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                full_path = os.path.join(dir_path, filename)
                with open(full_path, 'r') as file:
                    relative_path = os.path.relpath(full_path, base_path)
                    content = self._replace_placeholders(relative_path, file.read())
                    rdc[relative_path] = content
                    # Do something with the content
        return rdc

    def load_airflow_aspect(self, job_id: str) -> Optional[str]:
        content = None
        with open(os.path.join(self.project_path, job_id, "pbt_aspects.yml"), "r") as file:
            content = file.read()
        return content

    def load_pipeline_base_path(self, pipeline):
        return os.path.join(self.project_path, pipeline, "code")

    @staticmethod
    def is_cross_project_pipeline(pipeline):

        HTTPSURIAllRepoPatterns = re.compile(
            r"gitUri=(.*)&subPath=(.*)&tag=(.*)&projectSubscriptionProjectId=(.*)&path=(.*)")

        match = HTTPSURIAllRepoPatterns.search(pipeline)
        if match:
            git_uri, sub_path, tag, project_subscription_project_id, path = match.groups()
            return project_subscription_project_id, tag.split('/')[1], path
        else:
            return None, None, None

    def load_pipeline_folder(self, pipeline):
        rdc = {}
        (project_subscription_id, version, pipeline_path) = self.is_cross_project_pipeline(pipeline)
        if project_subscription_id is not None:
            sub_path = f".prophecy/{project_subscription_id}/{pipeline_path}"
        else:
            sub_path = pipeline

        base_path = os.path.join(self.project_path, sub_path, "code")
        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                if filename.endswith(('.py', '.json', '.conf', '.scala', 'pom.xml')):
                    full_path = os.path.join(dir_path, filename)
                    with open(full_path, 'r') as file:
                        relative_path = os.path.relpath(full_path, base_path)
                        try:
                            content = self._replace_placeholders(relative_path, file.read())
                        except Exception as e:
                            print(f"Failed to replace placeholders in {relative_path}")
                            raise e
                        rdc[relative_path] = content
                        # Do something with the content
        return rdc

    @staticmethod
    def get_pipeline_whl_or_jar(base_path: str):
        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                full_path = os.path.join(dir_path, filename)
                if full_path.endswith(".whl") or full_path.endswith(".jar"):
                    return full_path

    def _load_subscribed_project_yml(self, subscribed_project_id: str):
        with open(os.path.join(self.project_path, ".prophecy", subscribed_project_id, "pbt_project.yml"), "r") as file:
            return yaml.safe_load(file.read())

    def get_pipeline_name(self, pipeline_id):
        subscribed_project_id, release_tag, pipeline_path = self.is_cross_project_pipeline(pipeline_id)

        # pipeline belongs to same deployment.
        if subscribed_project_id is None:
            pipeline_name = self.pipelines.get(pipeline_id, {}).get('name', pipeline_id.split('/')[
                -1])  # if it doesn't exist then return only the last name.
        else:
            subscribed_project = self._load_subscribed_project_yml(subscribed_project_id)
            pipeline_name = subscribed_project.get('pipelines', {}).get(pipeline_path, {}).get('name',
                                                                                               pipeline_path.split('/')[
                                                                                                   -1])

        return pipeline_name

    def _verify_project(self):
        if not os.path.exists(self.project_path):
            raise ProjectPathNotFoundException(f"Project path does not exist {self.project_path}")

        if not os.path.exists(os.path.join(self.project_path, PBT_FILE_NAME)):
            raise ProjectFileNotFoundException(f"Project file does not exist at path {self.project_path}")

    def _load_project_config(self):
        pbt_project_path = os.path.join(self.project_path, PBT_FILE_NAME)
        with open(pbt_project_path, "r") as project_to_release:
            self.pbt_project_dict = yaml.safe_load(project_to_release)

    def _extract_project_info(self):
        self.project_language = self.pbt_project_dict.get(LANGUAGE, None)
        self.jobs = self.pbt_project_dict.get(JOBS, {})
        self.pipelines = self.pbt_project_dict.get(PIPELINES, {})

    def _load_pipeline_configurations(self):
        pipeline_conf = dict(self.pbt_project_dict.get(PIPELINE_CONFIGURATIONS, []))

        for pipeline_config_path, pipeline_config_object in pipeline_conf.items():
            configurations = {}

            for configurations_key in pipeline_config_object[CONFIGURATIONS]:
                config_name = pipeline_config_object[CONFIGURATIONS][configurations_key]["name"]
                file_path = os.path.join(self.project_path, configurations_key + JSON_EXTENSION)

                with open(file_path, 'r') as file:
                    data = file.read()

                configurations[config_name] = data

            self.pipeline_configurations[pipeline_config_object[BASE_PIPELINE]] = configurations

    def get_files(self, pipeline_path: str, aspect: str):
        return os.path.join(self.project_path, pipeline_path + '/' + aspect)

    def _replace_placeholders(self, path: str, content: str) -> str:
        if self.project_id is not None and len(self.project_id) > 0 and (
                path.endswith('.json') or path.endswith('.scala') or path.endswith('.py')):
            content = content.replace(PROJECT_ID_PLACEHOLDER_REGEX, self.project_id)

        if self.release_version is not None and len(self.release_version) > 0 and (
                path.endswith('.json') or path.endswith('.scala') or path.endswith('.py')):
            content = content.replace(PROJECT_RELEASE_VERSION_PLACEHOLDER_REGEX, self.release_version)

        return content
