
from src.pbt.v2.client.airflow_client.composer_client import ComposerRestClient
from src.pbt.v2.client.airflow_client.mwaa_client import MWAARestClient
from src.pbt.v2.exceptions import UnknownAirflowProviderException, FabricNotConfiguredException
from src.pbt.v2.state_config import ProjectConfig


def create_airflow_client(fabric_id: str, project_config: ProjectConfig):
    fabric_info = project_config.state_config.get_fabric(fabric_id)

    if fabric_info is not None and fabric_info.content is not None:

        composer_info = fabric_info.content.composer_info
        mwaa_info = fabric_info.content.mwaa_info

        if composer_info is not None:

            return ComposerRestClient(composer_info.airflow_url, composer_info.project_id, composer_info.client_id,
                                      composer_info.key_json, composer_info.dag_location)

        elif mwaa_info is not None:
            return MWAARestClient(mwaa_info.environment_name, mwaa_info.region, mwaa_info.access_key,
                                  mwaa_info.secret_key)

        else:
            raise UnknownAirflowProviderException(f"unknown provider")

    else:
        raise FabricNotConfiguredException(f"Fabric {fabric_id} is not configured in state config")


