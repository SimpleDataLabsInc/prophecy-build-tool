import hashlib
import json
import os
import zipfile
from typing import Dict, Optional, List

from src.pbt.v2.client.airflow_client import AirflowRestClient
from src.pbt.v2.project.project_parser import ProjectParser
from src.pbt.v2.state_config import JobsInfo


class AirflowJobJsons:

    def __init__(self, job_pbt: dict, prophecy_job_json: str, rdc: Dict[str, str], sha: Optional[str]):
        self.job_pbt = job_pbt
        self.prophecy_job_json = prophecy_job_json
        self.rdc = rdc
        self.sha = sha
        self.filters_job_files = self.__filter_job_files()

    def is_valid_job(self):
        prophecy_job_json_dict = json.loads(self.prophecy_job_json)
        return self.job_pbt is not None and self.prophecy_job_json is not None and self.rdc is not None and \
            prophecy_job_json_dict['fabricId'] is not None

    def validate_prophecy_managed_checksum(self):
        file_joiner: str = "$$$"
        content = file_joiner.join(file_content for file_content in self.__filter_job_files().values())
        return self.sha == content

    def __filter_job_files(self):
        filtered_files = {file_name: file_content for file_name, file_content in self.rdc.items()
                          if
                          file_name == 'dag.py' or '__init__.py' or file_name == 'prophecy-job.json' or 'tasks/' in file_name}

        return dict(sorted(filtered_files.items()))

    def prophecy_job_json_dict(self):
        try:
            return json.loads(self.prophecy_job_json)
        except ValueError:
            return {}

    def dag_name(self):
        return self.prophecy_job_json_dict()['metainfo']['dag_name']

    def is_disabled(self):
        return self.prophecy_job_json_dict()['enabled'] is False


class AirflowJobs:
    def __init__(self, project: ProjectParser):
        self.project = project
        self.airflow_jobs: Dict[str, AirflowJobJsons] = {}
        self.valid_airflow_jobs = {}
        self.airflow_jobs_without_code = {}
        self.prophecy_managed_dbt_jobs = {}

        self.__initialize_airflow_jobs()
        self.__initialize_valid_airflow_jobs()
        self.__prophecy_dbt_managed_jobs()

    def __initialize_airflow_jobs(self):
        jobs = {}
        for job_id, parsed_job in self.project.jobs.items():
            if 'Databricks' not in parsed_job['scheduler']:

                rdc = self.project.load_airflow_folder(job_id)
                prophecy_job_json = None

                if rdc is not None and 'prophecy-job.json' in rdc:
                    prophecy_job_json = rdc['prophecy-job.json']
                    rdc.pop('prophecy-job.json', None)

                aspects = self.project.load_airflow_aspect(job_id)
                sha = None
                if aspects is not None:
                    sha = json.loads(aspects)['sha']

                jobs[job_id] = AirflowJobJsons(parsed_job, prophecy_job_json, rdc, sha)

        self.airflow_jobs = jobs

    def __initialize_valid_airflow_jobs(self):
        for job_id, job_jsons in self.airflow_jobs.items():
            if job_jsons.is_valid_job() and self.__validate_airflow_job(job_id, job_jsons):
                self.valid_airflow_jobs[job_id] = job_jsons
            else:
                self.airflow_jobs_without_code[job_id] = job_jsons

    # top level file with dag.py name.
    def __dag_file_exists(self, rdc: Dict[str, str]) -> bool:
        return any(file_name == 'dag.py' for file_name, content in rdc)

    @staticmethod
    def calculate_checksum(input_str, salt=None):
        salt = salt or os.getenv('PROPHECY_EXECUTION_SALT') or "prophecy_execution"

        sha256 = hashlib.sha256()
        input_bytes = input_str.encode('utf-8')
        salt_bytes = salt.encode('utf-8')

        sha256.update(input_bytes)
        sha256.update(salt_bytes)

        digest_bytes = sha256.digest()

        return ''.join(f'{byte:02x}' for byte in digest_bytes)

    # def generate_secure_content(content: Optional[str], salt: Optional[str]) -> Optional[str]:
    #     iterations = 10000
    #     key_length = 128 // 8  # key_length is in bytes
    #
    #     password = content.encode('utf-8')
    #     salt_bytes = salt.encode('utf-8')
    #
    #     kdf = PBKDF2HMAC(
    #         algorithm=hashes.SHA256(),
    #         length=key_length,
    #         salt=salt_bytes,
    #         iterations=iterations,
    #         backend=default_backend()
    #     )
    #
    #     key = kdf.derive(password)
    #
    #     hashed = base64.b64encode(key).decode('utf-8')
    #     return re.sub(r'\W+', '_', hashed)

    # Example usage:
    # print(generate_secure_content('password', 'salt'))

    def __filter_job_files(self, rdc: Dict[str, str]):
        filtered_files = {file_name: file_content for file_name, file_content in rdc.items()
                          if
                          file_name == 'dag.py' or '__init__.py' or file_name == 'prophecy-job.json' or 'tasks/' in file_name}

        return dict(sorted(filtered_files.items()))

    def __validate_airflow_job(self, job_id: str, job_jsons: AirflowJobJsons):

        is_prophecy_managed_fabric = self.project.state_config.is_fabric_prophecy_managed(
            job_jsons.job_pbt['fabric_id'])
        rdc = job_jsons.rdc
        if self.__dag_file_exists(rdc) is False:
            raise Exception(f"Please open the Job `{job_id}` in editor, check diagnostic errors and release again.")
        elif is_prophecy_managed_fabric:
            if job_jsons.validate_prophecy_managed_checksum():
                return job_jsons.filters_job_files
            else:
                raise Exception(
                    f"Job `{job_id}` has been externally edited. Please open the Job in editor and release again.")
        else:
            return rdc

    # todo handle exceptions
    def __valid_airflow_jobs(self):
        return [(job_id, job_content for job_id, job_content in self.airflow_jobs.items()
                 if
                 self.__validate_airflow_job(job_id, job_content) is not None and job_content['fabric_id'] is not None)]

    # we won't be able to check the prophecy_job_json structure to prophecy_job.json verbatim.
    def __prophecy_dbt_managed_jobs(self):
        for job_id, job_jsons in self.valid_airflow_jobs.items():
            is_job_enabled = job_jsons.job_pbt['enabled'] is True
            is_prophecy_managed_fabric = self.project.state_config.is_fabric_prophecy_managed(
                job_jsons.job_pbt['fabric_id'])
            if is_job_enabled and is_prophecy_managed_fabric and len(job_jsons.prophecy_job_json_as_dict) > 0 and any(
                    value['component'] == 'dbt' for key, value in job_jsons.prophecy_job_json_as_dict['processes']):
                self.prophecy_managed_dbt_jobs[job_id] = job_jsons

    def __add_jobs(self):
        pass

    def headers(self):
        pass

    def deploy(self):
        self.__deploy_remove_jobs()
        self.__deploy_delete_jobs()
        self.__deploy_add_jobs()
        self.__deploy_refresh_jobs()
        self.__deploy_pause_jobs()
        self.__deploy_resume_jobs()

    def __deploy_remove_jobs(self):
        pass

    def __jobs_to_be_deleted(self) -> List[JobsInfo]:
        return [
            airflow_job for airflow_job in self.project.state_config.get_airflow_jobs
            if all(
                airflow_job.job_id == job_id
                for job_id, job_jsons in self.valid_airflow_jobs.items()
            )
        ]

    def __jobs_with_fabric_changed(self) -> List[JobsInfo]:
        return [
            airflow_job for airflow_job in self.project.state_config.get_airflow_jobs
            if any(
                airflow_job.job_id == job_id and airflow_job.fabric_id != job_jsons.job_pbt['fabricId']
                for job_id, job_jsons in self.valid_airflow_jobs.items()
            )
        ]

    def __renamed_jobs(self) -> List[JobsInfo]:
        return [
            airflow_job for airflow_job in self.project.state_config.get_airflow_jobs
            if any(
                airflow_job.id == job_id and airflow_job.fabric_id == job_jsons.job_pbt['fabricId'] and
                job_jsons.job_pbt['enabled'] is True and airflow_job.scheduler_job_id != job_jsons.dag_name()
                for job_id, job_jsons in self.valid_airflow_jobs.items()
            )
        ]

    def __airflow_jobs_to_be_added(self) -> List[JobsInfo]:
        return self.__jobs_to_be_deleted() + self.__jobs_with_fabric_changed()

    def __all_removed_airflow_jobs(self) -> List[JobsInfo]:
        return list(set(self.__renamed_jobs() + self.__jobs_with_fabric_changed() + self.__jobs_to_be_deleted()))

    def __airflow_jobs_to_be_paused(self):
        disabled_jobs = {job_id: job_jsons for job_id, job_jsons in self.valid_airflow_jobs.items() if job_jsons.is_disabled}
        disabled_jobs_not_in_removed_jobs = {
            job_id:job_jsons
            for job_id, job_jsons in disabled_jobs.items()
            if all(
                airflow_job.id == job_id
                for airflow_job in self.__all_removed_airflow_jobs()
            ) is False
        }
        disabled_jobs_where_old_was_enabled = {
            job_id:job_jsons
            for job_id, job_jsons in disabled_jobs_not_in_removed_jobs
            if any(
                airflow_job.id == job_id and airflow_job.is_paused is False
                for airflow_job  in self.project.state_config.get_airflow_jobs
            )
        }
        return disabled_jobs_where_old_was_enabled

    def __sanitize_job(self, job_id) -> str:
        return ('_' if job_id is None else job_id).split('/')[-1].replace('\\W', '_')

    def __get_zipped_dag_name(self, dag_name: str):
        return f"/tmp/${dag_name}.zip"

    def __zip_folder(self, folder_path, output_path):
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    abs_file_path = os.path.join(root, file)
                    zipf.write(abs_file_path, abs_file_path[len(folder_path):])

    def __deploy_add_jobs(self):
        for job_id, job_content in self.__valid_airflow_jobs():
            client = AirflowRestClient.create_client(job_content['fabric_id'])
            self.__validate_airflow_job(job_id, job_content)
            dag_name = self.__sanitize_job(job_id)
            self.__zip_folder(self.project.load_airflow_folder(job_id), self.__get_zipped_dag_name(dag_name))
            try:
                client.upload_dag(dag_name, self.__get_zipped_dag_name(dag_name))
                client.unpause_dag(dag_name)
                return dag_name
            except Exception as e:
                print("Failed to upload_dag for job_id: " + job_id)
                print(e)

    def __deploy_refresh_jobs(self):
        pass

    def __deploy_delete_jobs(self):
        for job_id, job_content in self.__jobs_to_be_deleted().items():
            client = AirflowRestClient.create_client(job_content['fabric_id'])
            sanitized_job_name = self.__sanitize_job(job_id)
            client.delete_dag_file(sanitized_job_name)

    def __deploy_pause_jobs(self):
        for job_id, job_content in self.__airflow_jobs_to_be_paused().items():
            client = AirflowRestClient.create_client(job_content['fabric_id'])
            sanitized_job_name = self.__sanitize_job(job_id)
            prophecy_job = next(
                (content for path, content in self.project.load_airflow_folder(job_id) if path == "prophecy-job.json"),
                None)
            if prophecy_job is not None:
                prophecy_job_json = json.loads(prophecy_job)
                if prophecy_job_json['metainfo']['dagName'] is not None:
                    job_name = prophecy_job_json['metainfo']['dagName']
                else:
                    job_name = f"Prophecy_Job_{sanitized_job_name}"
                client.pause_dag(job_name)

    def __deploy_skip_jobs(self):
        pass


class AirflowGitSecrets:

    def __init__(self, project: ProjectParser, airflow_jobs: AirflowJobs):
        self.project = project
        self.airflow_jobs = airflow_jobs
        self.__git_secrets = None
        self.__initiate_git_secrets()

    def __initiate_git_secrets(self):
        pass

    def headers(self):
        pass

    def deploy(self):
        pass
