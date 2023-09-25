from typing import Optional

from .airflow import AirflowRestClient
from .airflow.composer import ComposerRestClient
from .airflow.mwaa import MWAARestClient
from .databricks import DatabricksClient
from .s3 import S3Client
from ..project_config import FabricInfo, FabricConfig


class RestClientFactory:
    _instance_dict = {}

    def __init__(self, fabric_config: FabricConfig):

        self.fabric_config = fabric_config
        self.fabric_id_to_rest_client = {}

    def _get_fabric_info(self, fabric_id: str) -> FabricInfo:
        if fabric_id is None or fabric_id == "":
            raise ValueError("Fabric Id is not defined in the deployment state")
        else:
            fabric_info: Optional[FabricInfo] = self.fabric_config.get_fabric(str(fabric_id))
            if fabric_info is None:
                raise ValueError("Fabric Id is not defined in the deployment state")
            return fabric_info

    def _get_client(self, fabric_id: str):
        if fabric_id in self.fabric_id_to_rest_client:
            return self.fabric_id_to_rest_client[fabric_id]
        else:
            return None

    def s3_client(self, fabric_id: str) -> S3Client:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        emr = self._get_fabric_info(fabric_id).emr

        if emr is not None:
            client = S3Client(emr.region, emr.access_key_id, emr.secret_access_key, emr.session_token)
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

    def databricks_client(self, fabric_id: str) -> DatabricksClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        databricks = self._get_fabric_info(fabric_id).databricks

        if databricks is not None:
            client = DatabricksClient.from_host_and_token(databricks.url, databricks.token)
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

    def airflow_client(self, fabric_id: str) -> AirflowRestClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        fabric_info = self._get_fabric_info(fabric_id)

        composer = fabric_info.composer
        mwaa = fabric_info.mwaa

        if composer is not None:
            client = ComposerRestClient(composer.airflow_url, composer.project_id, composer.client_id,
                                        composer.key_json, composer.dag_location)
        elif mwaa is not None:
            client = MWAARestClient(mwaa.environment_name, mwaa.region, mwaa.access_key,
                                    mwaa.secret_key)

        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

        if client is not None:
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

    def dataproc_client(self, fabric_id: str) -> ComposerRestClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        fabric_info = self._get_fabric_info(fabric_id)

        dataproc = fabric_info.dataproc

        if dataproc is not None:
            client = ComposerRestClient("dummyAirflowUrl",
                                        dataproc.project_id,
                                        None,
                                        dataproc.key_json,
                                        "dummyDagLocation",
                                        dataproc.location)
        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

        if client is not None:
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

    @staticmethod
    def get_instance(cls, fabric_config: FabricConfig):
        if cls._instance_dict.get(fabric_config) is None:
            cls._instance_dict[fabric_config] = RestClientFactory(fabric_config)

        return cls._instance_dict[fabric_config]
