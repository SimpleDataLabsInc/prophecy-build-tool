import os
import re
from typing import Optional

import yaml

from ..utils.constants import PBT_FILE_NAME, LANGUAGE, JOBS, PIPELINES, \
    PIPELINE_CONFIGURATIONS, CONFIGURATIONS, JSON_EXTENSION, BASE_PIPELINE, PROJECT_ID_PLACEHOLDER_REGEX, \
    PROJECT_RELEASE_VERSION_PLACEHOLDER_REGEX, PROJECT_RELEASE_TAG_PLACEHOLDER_REGEX, GEMS
from ..utils.exceptions import ProjectPathNotFoundException, ProjectFileNotFoundException

SUBSCRIBED_ENTITY_URI_REGEX = r"gitUri=(.*)&subPath=(.*)&tag=(.*)&projectSubscriptionProjectId=(.*)&path=(.*)"


def _read_file_content(file_path: str) -> Optional[str]:
    with open(file_path, 'r') as file:
        content = file.read()
    return content


class Project:
    _DATABRICKS_JOB_JSON = "databricks-job.json"
    _CODE_FOLDER = "code"

    def __init__(self, project_path: str, project_id: Optional[str] = None,
                 release_tag: Optional[str] = None, release_version: Optional[str] = None):

        self.project_id = project_id
        self.project_path = project_path

        self.release_tag = release_tag
        self.release_version = release_version

        self.pbt_project_dict = {}
        self.project_language = None
        self.jobs = None
        self.pipelines = {}
        self.pipeline_id_to_name = {}
        self.gems = {}

        self._verify_project()
        self._load_project_config()
        self._extract_project_info()
        self.pipeline_configurations = self._load_pipeline_configurations()

    @property
    def name(self) -> str:
        return self.pbt_project_dict.get('name')

    def load_databricks_job(self, job_id: str) -> Optional[str]:
        try:
            path = os.path.join(self.project_path, job_id, self._CODE_FOLDER, self._DATABRICKS_JOB_JSON)
            content = _read_file_content(path)

            if content is not None:
                return self._replace_placeholders(self._DATABRICKS_JOB_JSON, content)

            return content
        except Exception:
            return None

    def load_airflow_base_folder_path(self, job_id):
        return os.path.join(self.project_path, job_id, self._CODE_FOLDER)

    def load_airflow_folder(self, job_id):
        return {path: self._replace_placeholders(path, content)
                for path, content in
                self._read_directory(os.path.join(self.project_path, job_id, self._CODE_FOLDER)).items()
                }

    def load_airflow_folder_with_placeholder(self, job_id):
        return self._read_directory(os.path.join(self.project_path, job_id, self._CODE_FOLDER))

    def load_airflow_aspect(self, job_id: str) -> Optional[str]:
        path = os.path.join(self.project_path, job_id, "pbt_aspects.yml")
        return _read_file_content(path)

    def load_pipeline_base_path(self, pipeline):
        return os.path.join(self.project_path, pipeline, self._CODE_FOLDER)

    def load_pipeline_folder(self, pipeline):
        (project_subscription_id, version, pipeline_path) = self.is_cross_project_pipeline(pipeline)

        if project_subscription_id is not None:
            sub_path = f".prophecy/{project_subscription_id}/{pipeline_path}"
        else:
            sub_path = pipeline

        base_path = os.path.join(self.project_path, sub_path, self._CODE_FOLDER)

        return self._read_directory(base_path)

    @staticmethod
    def get_pipeline_whl_or_jar(base_path: str):
        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                full_path = os.path.join(dir_path, filename)
                if full_path.endswith(".whl") or full_path.endswith(".jar"):
                    return full_path

    def get_py_pipeline_main_file(self, pipeline_id):
        subscribed_project_id, release_tag, pipeline_path = self.is_cross_project_pipeline(pipeline_id)

        if subscribed_project_id is None:
            path = pipeline_id
        else:
            path = ".prophecy/{}/{}".format(subscribed_project_id, pipeline_path)

        main_file = os.path.join(self.project_path, path, self._CODE_FOLDER, "main.py")
        data = _read_file_content(main_file)

        return data

    def get_pipeline_absolute_path(self, pipeline_id):
        return os.path.join(os.path.join(self.project_path, pipeline_id), "code")

    def get_pipeline_name(self, pipeline_id):
        subscribed_project_id, release_tag, pipeline_path = self.is_cross_project_pipeline(pipeline_id)

        # pipeline belongs to same deployment.
        if subscribed_project_id is None:
            pipeline_path = pipeline_id
            pipelines = self.pipelines

        else:
            subscribed_project = self._load_subscribed_project_yml(subscribed_project_id)
            pipelines = subscribed_project.get('pipelines', {})

        return pipelines.get(pipeline_path, {}).get('name', pipeline_path.split('/')[-1])

    def _read_directory(self, base_path: str):
        rdc = {}

        for dir_path, dir_names, filenames in os.walk(base_path):
            for filename in filenames:
                if filename.endswith(('.py', '.json', '.conf', '.scala', 'pom.xml')):
                    full_path = os.path.join(dir_path, filename)
                    content = _read_file_content(full_path)
                    if content is not None:
                        relative_path = os.path.relpath(full_path, base_path)
                        rdc[relative_path] = content
        return rdc

    def _load_subscribed_project_yml(self, subscribed_project_id: str):
        path = os.path.join(self.project_path, ".prophecy", subscribed_project_id, "pbt_project.yml")
        content = _read_file_content(path)
        if content is not None:
            return yaml.safe_load(content)

    def _verify_project(self):
        if not os.path.exists(self.project_path):
            raise ProjectPathNotFoundException(f"Project path does not exist {self.project_path}")

        if not os.path.exists(os.path.join(self.project_path, PBT_FILE_NAME)):
            raise ProjectFileNotFoundException(f"Project file does not exist at path {self.project_path}")

    def _load_project_config(self):
        pbt_project_path = os.path.join(self.project_path, PBT_FILE_NAME)
        content = _read_file_content(pbt_project_path)
        if content is not None:
            self.pbt_project_dict = yaml.safe_load(content)

    def _extract_project_info(self):
        self.project_language = self.pbt_project_dict.get(LANGUAGE, None)
        self.jobs = self.pbt_project_dict.get(JOBS, {})
        self.pipelines = self.pbt_project_dict.get(PIPELINES, {})
        self.gems = self.pbt_project_dict.get(GEMS, {})

    def _load_pipeline_configurations(self):
        pipeline_configurations = {}

        pipeline_conf = dict(self.pbt_project_dict.get(PIPELINE_CONFIGURATIONS, []))

        for pipeline_config_path, pipeline_config_object in pipeline_conf.items():
            configurations = {}

            for configurations_key in pipeline_config_object[CONFIGURATIONS]:
                config_name = pipeline_config_object[CONFIGURATIONS][configurations_key]["name"]
                file_path = os.path.join(self.project_path, configurations_key + JSON_EXTENSION)

                configurations[config_name] = _read_file_content(file_path)
            pipeline_configurations[pipeline_config_object[BASE_PIPELINE]] = configurations

        return pipeline_configurations

    def _replace_placeholders(self, path: str, content: str) -> str:
        if path.endswith('.json') or path.endswith('.scala') or path.endswith('.py'):
            content = content.replace(PROJECT_ID_PLACEHOLDER_REGEX, self.project_id) \
                .replace(PROJECT_RELEASE_VERSION_PLACEHOLDER_REGEX, self.release_version) \
                .replace(PROJECT_RELEASE_TAG_PLACEHOLDER_REGEX, self.release_tag)

        return content

    # only check for non-empty gems directory
    def non_empty_gems_directory(self):
        return os.path.exists(os.path.join(self.project_path, "gems"))

    @staticmethod
    def is_cross_project_pipeline(pipeline):

        match = re.compile(SUBSCRIBED_ENTITY_URI_REGEX).search(pipeline)

        if match:
            git_uri, sub_path, tag, project_subscription_project_id, path = match.groups()
            return project_subscription_project_id, tag.split('/')[1], path
        else:
            return None, None, None

    def get_package_path(self, _pipeline_id) -> (str, str):
        pipeline_path = os.path.join(self.project_path, _pipeline_id, "code")
        for root, dirs, files in os.walk(pipeline_path):
            for file in files:
                if file.endswith(".jar") or file.endswith(".whl"):
                    return root, file
