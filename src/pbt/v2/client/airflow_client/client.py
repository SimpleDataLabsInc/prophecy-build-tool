from src.pbt.v2.client.airflow_client.composer_client import ComposerRestClient
from src.pbt.v2.client.airflow_client.mwaa_client import MWAARestClient
from src.pbt.v2.state_config import StateConfigAndDBTokens


def create_airflow_client(fabric_id: str, state_config_and_db_tokens: StateConfigAndDBTokens):
    fabric_info = state_config_and_db_tokens.state_config.get_fabric(fabric_id)
    if fabric_info is not None and fabric_info.content is not None:
        if fabric_info.content.composer_info is not None:
            composer_info = fabric_info.content.composer_info
            return ComposerRestClient(composer_info.airflow_url, composer_info.project_id, composer_info.client_id,
                                      composer_info.key_json, composer_info.dag_location)
        elif fabric_info.content.mwaa_info is not None:
            mwaa_info = fabric_info.content.mwaa_info
            return MWAARestClient(mwaa_info.environment_name, mwaa_info.region, mwaa_info.access_key,
                                  mwaa_info.secret_key)

