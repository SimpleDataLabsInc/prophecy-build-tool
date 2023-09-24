from .composer import ComposerRestClient
from .mwaa import MWAARestClient
from ...exceptions import UnknownAirflowProviderException, FabricNotConfiguredException
from ...project_config import ProjectConfig, FabricType, FabricProviderType


def create_airflow_client(fabric_id: str, project_config: ProjectConfig):
    fabric_info = project_config.fabric_config.get_fabric(fabric_id)

    if fabric_info is not None and fabric_info.type == FabricType.Airflow:

        composer = fabric_info.composer
        mwaa = fabric_info.mwaa

        if composer is not None:

            return ComposerRestClient(composer.airflow_url, composer.project_id, composer.client_id,
                                      composer.key_json, composer.dag_location)

        elif mwaa is not None:
            return MWAARestClient(mwaa.environment_name, mwaa.region, mwaa.access_key,
                                  mwaa.secret_key)

        else:
            raise UnknownAirflowProviderException("unknown provider")

    else:
        raise FabricNotConfiguredException(f"Fabric {fabric_id} is not configured in state config")


def get_fabric_provider_type(fabric_id: str, project_config: ProjectConfig) -> str:
    fabric_info = project_config.fabric_config.get_fabric(fabric_id)
    if fabric_info is not None:
        return str(fabric_info.provider.value)
    else:
        return FabricProviderType.Databricks.value
