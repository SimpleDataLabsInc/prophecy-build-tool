from typing import List

from src.pbt.v2.constants import JOBS
from src.pbt.v2.project.project_parser import ProjectParser, DatabricksJobs, AirflowJobs
from src.pbt.v2.project_models import StepMetadata


class Project:
    def __init__(self, project: ProjectParser):
        self.project = project
        self.state_config = project.state_config
        self.jobs = self.project.project[JOBS]
        self.__databricks_jobs = DatabricksJobs(project)
        self.__airflow_jobs = AirflowJobs(project)

    @property
    def pipeline_configurations(self):
        return self.__databricks_jobs.pipeline_configurations

    def scripts_component(self):
        return self.__databricks_jobs.__script_components_from_jobs()

    def dbt_component(self):
        return self.__databricks_jobs.dbt_component_from_jobs

    def all_jobs(self):
        return self.__databricks_jobs

    def pipelines(self):
        return self.__databricks_jobs.__pipeline_components_from_jobs

    def databricks_jobs(self):
        return self.__databricks_jobs.headers()

    def airflow_jobs(self):
        return self.__airflow_jobs.headers()

    def dbt_component_from_jobs(self):
        return self.__databricks_jobs.dbt_component_from_jobs

    def get_script_components_for_jobs(self):
        return self.__databricks_jobs.__script_components_from_jobs()

    def headers(self) -> List[StepMetadata]:
        # atm suits with the ordering of how the steps will look like.

        return self.__databricks_jobs.headers() + self.__airflow_jobs.headers()
        #
        # step_metadata_list = []
        #
        # for job in self.__add_jobs():
        #     header = f"Adding job {job}"
        #     step_metadata_list.append(StepMetadata(job, header, "add", "job"))
        #
        # for job in self.__refresh_jobs():
        #     header = f"Refreshing job {job}"
        #     step_metadata_list.append(StepMetadata(job, header, "refresh", "job"))
        #
        # for job in self.__delete_jobs():
        #     header = f"Deleting job {job}"
        #     step_metadata_list.append(StepMetadata(job, header, "delete", "job"))
        #
        # return step_metadata_list

    def to_refresh(self):
        pass
