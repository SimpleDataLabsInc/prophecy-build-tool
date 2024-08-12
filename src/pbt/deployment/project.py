import copy
import os
from typing import List

import yaml

from .gems import GemsDeployment
from ..deployment.jobs.airflow import (
    AirflowGitSecrets,
    AirflowJobDeployment,
    DataprocPipelineConfigurations,
    EMRPipelineConfigurations,
    SparkSubmitPipelineConfigurations,
)
from ..deployment.jobs.databricks import (
    DBTComponents,
    DatabricksJobsDeployment,
    PipelineConfigurations,
    ScriptComponents,
)
from ..deployment.pipeline import PipelineDeployment
from ..entities.project import Project
from ..utility import Either, custom_print as log, is_online_mode
from ..utility import remove_null_items_recursively
from ..utils.constants import NEW_JOB_STATE_FILE
from ..utils.project_config import ProjectConfig
from ..utils.project_models import Colors, Operation, Status, StepMetadata, StepType


class ProjectDeployment:
    def __init__(self, project: Project, project_config: ProjectConfig):
        self.project = project
        self.project_config = project_config

        self._databricks_jobs = DatabricksJobsDeployment(project, project_config)
        self._airflow_jobs = AirflowJobDeployment(project, project_config)

        self._script_component = ScriptComponents(project, self._databricks_jobs, project_config)
        self._pipeline_configurations = PipelineConfigurations(project, self._databricks_jobs, project_config)
        self._spark_submit_pipeline_configurations = SparkSubmitPipelineConfigurations(
            project, self._airflow_jobs, project_config
        )
        self._emr_pipeline_configurations = EMRPipelineConfigurations(project, self._airflow_jobs, project_config)

        self._dataproc_pipeline_configurations = DataprocPipelineConfigurations(
            project, self._airflow_jobs, project_config
        )

        self._dbt_component = DBTComponents(project, self._databricks_jobs, project_config)
        self._airflow_git_secrets = AirflowGitSecrets(project, self._airflow_jobs, project_config)

        self._pipelines = PipelineDeployment(project, self._databricks_jobs, self._airflow_jobs, project_config)

        # add gems Deployment.
        self._gems = GemsDeployment(project, project_config)

    def headers(self):
        summary = (
            self._gems.summary()
            + self._script_component.summary()
            + self._dbt_component.summary()
            + self._airflow_git_secrets.summary()
            + self._pipeline_configurations.summary()
            + self._spark_submit_pipeline_configurations.summary()
            + self._emr_pipeline_configurations.summary()
            + self._dataproc_pipeline_configurations.summary()
            + self._pipelines.summary()
            + self._databricks_jobs.summary()
            + self._airflow_jobs.summary()
        )

        if len(summary) == 0:
            summary = ["No Job and pipelines to build"]

        summary_header = [StepMetadata("Summary", "Summary", Operation.Build, StepType.Summary)]

        header_components = (
            summary_header,
            self._gems.headers(),
            self._script_component.headers(),
            self._dbt_component.headers(),
            self._airflow_git_secrets.headers(),
            self._pipeline_configurations.headers(),
            self._spark_submit_pipeline_configurations.headers(),
            self._emr_pipeline_configurations.headers(),
            self._dataproc_pipeline_configurations.headers(),
            self._pipelines.headers(),
            self._databricks_jobs.headers(),
            self._airflow_jobs.headers(),
        )

        headers = sum(header_components, [])

        # 1st steps have to be summary
        for header in headers:
            log(step_id=header.id, step_metadata=header)

        for step in summary:
            log(message=step, step_id="Summary")

        log(step_status=Status.SUCCEEDED, step_id="Summary")

        return headers

    def build(self, pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python):
        self._pipelines.build(pipelines, ignore_build_errors, ignore_parse_errors, add_pom_python)

    def validate(self, treat_warning_as_errors):
        self._pipelines.validate(treat_warning_as_errors)

    def test(
        self,
    ):
        self._pipelines.test()

    def _deploy_gems(self):
        gems_responses = self._gems.deploy()

        if gems_responses is not None and any(response.is_left for response in gems_responses):
            raise Exception("Gems deployment failed.")

    def _deploy_scripts(self):
        script_responses = self._script_component.deploy()

        if script_responses is not None and any(response.is_left for response in script_responses):
            raise Exception("Script deployment failed.")

    def _deploy_dbt_components(self):
        dbt_responses = self._dbt_component.deploy()

        if dbt_responses is not None and any(response.is_left for response in dbt_responses):
            raise Exception("DBT deployment failed.")

    def _deploy_airflow_git_secrets(self):
        airflow_git_secrets_responses = self._airflow_git_secrets.deploy()

        if airflow_git_secrets_responses is not None and any(
            response.is_left for response in airflow_git_secrets_responses
        ):
            raise Exception("Airflow git secrets deployment failed.")

    def _deploy_pipeline_configs(self):
        pipeline_config_responses = self._pipeline_configurations.deploy()

        if pipeline_config_responses is not None and any(response.is_left for response in pipeline_config_responses):
            raise Exception("Pipeline config deployment failed.")

    def _deploy_emr_pipeline_config(self):
        emr_pipeline_config_responses = self._emr_pipeline_configurations.deploy()

        if emr_pipeline_config_responses is not None and any(
            response.is_left for response in emr_pipeline_config_responses
        ):
            raise Exception("EMR pipeline config deployment failed.")

    def _deploy_dataproc_pipeline_config(self):
        dataproc_pipeline_config_responses = self._dataproc_pipeline_configurations.deploy()

        if dataproc_pipeline_config_responses is not None and any(
            response.is_left for response in dataproc_pipeline_config_responses
        ):
            raise Exception("Dataproc pipeline config deployment failed.")

    def _deploy_spark_submit_pipeline_config(self):
        spark_submit_pipeline_config_responses = self._spark_submit_pipeline_configurations.deploy()

        if spark_submit_pipeline_config_responses is not None and any(
            response.is_left for response in spark_submit_pipeline_config_responses
        ):
            raise Exception("Spark Submit pipeline config deployment failed.")

    def _deploy_pipelines(self):
        pipeline_responses = self._pipelines.deploy()

        if pipeline_responses is not None and any(response.is_left for response in pipeline_responses):
            raise Exception("Pipeline deployment failed.")

    def _deploy_databricks_jobs(self) -> List[Either]:
        databricks_jobs_responses = self._databricks_jobs.deploy()

        return databricks_jobs_responses

    def _deploy_airflow_jobs(self) -> List[Either]:
        airflow_jobs_responses = self._airflow_jobs.deploy()

        return airflow_jobs_responses

    def deploy(self, job_ids):
        if is_online_mode():
            self._deploy_gems()

        self._deploy_scripts()
        self._deploy_dbt_components()
        self._deploy_airflow_git_secrets()
        self._deploy_pipeline_configs()
        self._deploy_spark_submit_pipeline_config()
        self._deploy_emr_pipeline_config()
        self._deploy_dataproc_pipeline_config()

        if not self.project_config.skip_builds:
            self._deploy_pipelines()
        else:
            log("\nSkipping pipeline deployment as skip_builds is set to true.\n")

        databricks_responses = self._deploy_databricks_jobs()
        airflow_responses = self._deploy_airflow_jobs()

        new_state_config = copy.deepcopy(self.project_config.jobs_state)

        # only jobs changes state_config.

        new_state_config.update_state(databricks_responses + airflow_responses)

        base_path = os.path.join(os.getcwd(), ".pbt")

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        if self.project_config.based_on_file or self.project_config.migrate:
            path = os.path.join(base_path, NEW_JOB_STATE_FILE)
            yaml_str = yaml.dump(data=remove_null_items_recursively(new_state_config.dict()))

            with open(path, "w") as file:
                file.write(yaml_str)

        # Only fail when there is a failure in jobs deployment.
        if databricks_responses is not None and any(response.is_left for response in databricks_responses):
            for response in databricks_responses:
                if response.is_left:
                    print(response.left)
            raise Exception("Databricks jobs deployment failed.")

        if airflow_responses is not None and any(response.is_left for response in airflow_responses):
            raise Exception("Airflow jobs deployment failed.")

        log(f"\n\n{Colors.OKCYAN}Deployment completed successfully.\n{Colors.ENDC}")

        if self.project_config.migrate:
            log(f"\n\n{Colors.OKCYAN}Migrating project to new version. \n{Colors.ENDC}")
            try:
                fabric_config_str = yaml.dump(
                    data=remove_null_items_recursively(self.project_config.fabric_config_without_conf_replace.dict())
                )
                system_config_str = yaml.dump(
                    data=remove_null_items_recursively(self.project_config.system_config.dict())
                )
                config_override_str = yaml.dump(
                    data=remove_null_items_recursively(self.project_config.configs_override.dict())
                )

                with open(os.path.join(base_path, "fabrics.yml"), "w") as file:
                    file.write(fabric_config_str)

                with open(os.path.join(base_path, "system.yml"), "w") as file:
                    file.write(system_config_str)

                with open(os.path.join(base_path, "override.yml"), "w") as file:
                    file.write(config_override_str)

                log(f"\n\n{Colors.OKCYAN}Successfully migrated project to new version.\n{Colors.ENDC}")
            except Exception as e:
                log(f"\n\n{Colors.FAIL}Failed to migrate project to new version.\n{Colors.ENDC}", e)
                if os.path.isdir(base_path):
                    os.rmdir(base_path)

        return databricks_responses + airflow_responses
