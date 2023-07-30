from src.pbt.v2.project.project_parser import Project
from src.pbt.v2.project_models import StepMetadata


# todo various permutations and combinations of headers based on flags.

class Headers():

    def __init__(self, project: Project):
        self.project = project
        pass

    def summary(self):
        return self.pipeline_configurations_headers() + self.upload_scripts_headers() \
            + self.dbt_profiles_to_build_headers() + self.dbt_secrets_to_upload_header() \
            + self.jobs_headers() + self.airflow_git_secrets_header()

    def pipeline_configurations_headers(self):
        headers = []

        for pipeline_id in self.project.pipeline_configurations:
            header = f"Pipeline configuration for path {pipeline_id} will be added."
            headers.append(StepMetadata(pipeline_id, header, "Upload", "Pipeline_configurations"))

        return headers

    def upload_scripts_headers(self):
        headers = []
        for job_id, script_component in self.project.scripts_component().items():
            for component in script_component.scripts:
                header = f"Script {component['path']} will be added."
                headers.append(StepMetadata(component['path'], header, "Upload", "Script"))

        return headers

    def dbt_profiles_to_build_headers(self):
        total_dbt_profiles = 0
        for job_id, dbt_components in self.project.dbt_component_from_jobs().items():

            for components in dbt_components.components:
                if components["profilePath"] is not None and components["profileContent"] is not None:
                    total_dbt_profiles = total_dbt_profiles + 1

        if total_dbt_profiles > 0:
            header = f"Upload {total_dbt_profiles} dbt profiles"
            return [StepMetadata("DBTProfileComponents", header, "Upload", "dbt_profiles")]
        else:
            return []

    def dbt_secrets_to_upload_header(self):
        total_dbt_secrets = 0
        for job_id, dbt_components in self.project.dbt_component_from_jobs().items():

            for component in dbt_components.components:
                if component["sqlFabricId"] is not None and component["secretKey"] is not None:
                    total_dbt_secrets = total_dbt_secrets + 1

        if total_dbt_secrets > 0:
            header = f"Upload {total_dbt_secrets} dbt secrets"
            return [StepMetadata("DBTSecretComponents", header, "Upload", "dbt_secrets")]
        else:
            return []
