from src.pbt.metadata.my_enums import StepMetadata
from src.pbt.v2.project.project_parser import Project
from src.pbt.v2.client import databricks_client
from src.pbt.v2.client.databricks_client import DatabricksClient


# TODO rename this later
class DeployAndRelease:
    def __init__(self, project: Project, db_fabrics_to_token: dict = {}):
        self.project = project
        self.db_fabrics_to_token = db_fabrics_to_token

    def release_headers(self):
        return self.pipeline_configurations_headers() + self.upload_scripts_headers() \
            + self.dbt_profiles_to_build_headers() + self.dbt_secrets_to_upload_header() \
            + self.jobs_headers() + self.airflow_git_secrets_header()

    def airflow_git_secrets_header(self):
        return []

    def jobs_headers(self):
        return self.project.databricks_jobs() + self.project.airflow_jobs()

    def deploy(self):
        # download pipelines
        # upload pipelines
        # upload scripts
        # upload dbt profiles
        # upload secrets
        # upload configurations.
        self.upload_configurations()
        self.upload_scripts()
        self.upload_dbt_profiles()
        self.upload_dbt_secrets()
        pass

    def pipeline_configurations_headers(self):
        headers = []

        for pipeline_id in self.project.pipeline_configurations:
            header = f"Pipeline configuration for path {pipeline_id} will be added."
            headers.append(StepMetadata.create(pipeline_id, header, "Upload", "Pipeline_configurations"))

        return headers

    def upload_configurations(self):
        for pipeline_id, configurations in self.project.pipeline_configurations.items():
            path = f"dbfs:/FileStore/prophecy/artifacts/dev/execution/1/{pipeline_id}"
            for configuration_name, configuration_content in configurations.items():
                actual_path = path + "/" + configuration_name + ".json"
                for fabric_id, token in self.db_fabrics_to_token.items():
                    client = DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token, fabric_id)
                    client.upload_content(configuration_content, actual_path)

    # def jobs_headers(self):
    #     return self.project.all_jobs()

    def upload_scripts_headers(self):
        headers = []
        for job_id, script_component in self.project.scripts_component().items():
            for component in script_component.scripts:
                header = f"Script {component['path']} will be added."
                headers.append(StepMetadata.create(component['path'], header, "Upload", "Script"))

        return headers

    def upload_scripts(self):
        for job_key, script_component in self.project.get_script_components_for_jobs().items():
            for component in script_component.scripts:
                DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token,
                                                   script_component.fabric_id).upload_content(component['content'],
                                                                                              component['path'])
                print(f"Uploaded script {component['path']} to {script_component.fabric_id}")

    def upload_dbt_profiles(self):
        for job_id, dbt_components in self.project.dbt_component_from_jobs().items():

            for components in dbt_components.components:
                if components["profilePath"] is not None and components["profileContent"] is not None:
                    DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token,
                                                       dbt_components.fabric_id) \
                        .upload_content(components["profileContent"], components["profilePath"])
                    print(f"Uploaded dbt profile {components['profilePath']} to {dbt_components.fabric_id}")

    def upload_dbt_secrets(self):
        for job_id, dbt_components in self.project.dbt_component_from_jobs().items():

            for components in dbt_components.components:
                if components["sqlFabricId"] is not None and components["secretKey"] is not None:
                    DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token,
                                                       dbt_components.fabric_id) \
                        .create_scope(dbt_components.secret_scope)

                    url = DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token,
                                                             fabric_id=components['sqlFabricId']).host
                    sql_fabric_token = self.db_fabrics_to_token.get(components['sqlFabricId'], "")
                    git_token = self.project.state_config.git_token_for_project(components['projectId'])

                    master_token = f"{sql_fabric_token};{git_token}"

                    DatabricksClient.from_state_config(self.project.state_config, self.db_fabrics_to_token,
                                                       dbt_components.fabric_id) \
                        .create_secret(dbt_components.secret_scope, components['secretKey'], master_token)

                    print(f"Uploaded dbt secrets {components['secretKey']} to scope {dbt_components.secret_scope}")

    def dbt_profiles_to_build_headers(self):
        total_dbt_profiles = 0
        for job_id, dbt_components in self.project.dbt_component_from_jobs().items():

            for components in dbt_components.components:
                if components["profilePath"] is not None and components["profileContent"] is not None:
                    total_dbt_profiles = total_dbt_profiles + 1

        if total_dbt_profiles > 0:
            header = f"Upload {total_dbt_profiles} dbt profiles"
            return [StepMetadata.create("DBTProfileComponents", header, "Upload", "dbt_profiles")]
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
            return [StepMetadata.create("DBTSecretComponents", header, "Upload", "dbt_secrets")]
        else:
            return []
