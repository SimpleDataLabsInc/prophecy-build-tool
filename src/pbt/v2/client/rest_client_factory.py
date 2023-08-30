from typing import Optional

from .airflow.composer import ComposerRestClient
from .airflow.mwaa import MWAARestClient
from .databricks import DatabricksClient
from .s3 import S3Client
from ..project_config import StateConfig, FabricInfo


class RestClientFactory:
    def __init__(self, state_config: StateConfig):
        self.state_config = state_config
        self.fabric_id_to_rest_client = {}

    def s3_client(self, fabric_id: str) -> S3Client:
        if fabric_id in self.fabric_id_to_rest_client:
            return self.fabric_id_to_rest_client[fabric_id]

        elif fabric_id is None or fabric_id == "":
            raise ValueError("Fabric Id is not defined in the deployment state")
        else:
            fabric_info: Optional[FabricInfo] = self.state_config.get_fabric(str(fabric_id))
            if fabric_info is None:
                raise ValueError("Fabric Id is not defined in the deployment state")

            emr = fabric_info.emr

            if emr is not None:
                return S3Client(emr.region, emr.access_key_id, emr.secret_access_key, emr.session_token)

            else:
                raise ValueError("Fabric Id is not defined in the deployment state")

    def databricks_client(self, fabric_id: str):
        if fabric_id in self.fabric_id_to_rest_client:
            return self.fabric_id_to_rest_client[fabric_id]

        elif fabric_id is None or fabric_id == "":
            raise ValueError("Fabric Id is not defined in the deployment state")
        else:
            fabric_info: Optional[FabricInfo] = self.state_config.get_fabric(str(fabric_id))
            if fabric_info is None:
                raise ValueError("Fabric Id is not defined in the deployment state")

            databricks = fabric_info.databricks

            if databricks is not None:
                return DatabricksClient.from_host_and_token(databricks.url, databricks.token)

            else:
                raise ValueError("Fabric Id is not defined in the deployment state")

    def airflow_client(self, fabric_id: str):
        if fabric_id in self.fabric_id_to_rest_client:
            return self.fabric_id_to_rest_client[fabric_id]

        elif fabric_id is None or fabric_id == "":
            raise ValueError("Fabric Id is not defined in the deployment state")
        else:
            fabric_info: Optional[FabricInfo] = self.state_config.get_fabric(str(fabric_id))
            if fabric_info is None:
                raise ValueError("Fabric Id is not defined in the deployment state")

            composer = fabric_info.composer
            mwaa = fabric_info.mwaa

            if composer is not None:
                return ComposerRestClient(composer.airflow_url, composer.project_id, composer.client_id,
                                          composer.key_json, composer.dag_location)
            elif mwaa is not None:
                return MWAARestClient(mwaa.environment_name, mwaa.region, mwaa.access_key,
                                      mwaa.secret_key)

            else:
                raise ValueError("Fabric Id is not defined in the deployment state")
