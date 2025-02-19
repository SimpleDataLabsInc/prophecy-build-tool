import threading
from typing import Optional

from .airflow import AirflowRestClient
from .airflow.composer import ComposerRestClient
from .airflow.mwaa import MWAARestClient
from .airflow.open_source import OpenSourceRestClient
from .databricks import DatabricksClient
from .s3 import S3Client
from ..utils.project_config import FabricConfig, FabricInfo


class RestClientFactory:
    _instance = None

    _s3_client_creation_lock = threading.Lock()

    def __init__(self, fabric_config: FabricConfig):
        self.fabric_config = fabric_config
        self.fabric_id_to_rest_client = {}

    def _get_fabric_info(self, fabric_id: str) -> FabricInfo:
        if fabric_id is None or fabric_id == "":
            raise ValueError(f"Fabric Id {fabric_id} is not defined in the deployment state")
        else:
            fabric_info: Optional[FabricInfo] = self.fabric_config.get_fabric(str(fabric_id))
            if fabric_info is None:
                raise ValueError(f"Fabric Id {fabric_id} is not found in the fabric configs {self.fabric_config}")
            return fabric_info

    def _get_client(self, fabric_id: str):
        if fabric_id in self.fabric_id_to_rest_client:
            return self.fabric_id_to_rest_client[fabric_id]
        else:
            return None

    def s3_client(self, fabric_id: str) -> S3Client:
        self._s3_client_creation_lock.acquire()
        try:
            if self._get_client(fabric_id) is not None:
                return self._get_client(fabric_id)

            emr = self._get_fabric_info(fabric_id).emr

            if emr is not None:
                client = S3Client(
                    emr.region, emr.access_key_id, emr.secret_access_key, emr.session_token, emr.assumed_role
                )
                self.fabric_id_to_rest_client[fabric_id] = client
                return client

            else:
                raise ValueError("Fabric Id is not defined in the deployment state")
        finally:
            self._s3_client_creation_lock.release()

    def databricks_client(self, fabric_id: str) -> DatabricksClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        databricks = self._get_fabric_info(fabric_id).databricks

        if databricks is not None:
            oauth_client_secret = (
                databricks.oauth_credentials.client_secret if databricks.oauth_credentials is not None else None
            )
            client = DatabricksClient.from_databricks_info(
                host=databricks.url,
                auth_type=databricks.auth_type,
                token=databricks.token,
                oauth_client_secret=oauth_client_secret,
                user_agent=databricks.user_agent,
            )
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

        else:
            raise ValueError(
                f"Fabric Id {fabric_id} is not defined and the databricks client {self._get_fabric_info(fabric_id)}."
            )

    def airflow_client(self, fabric_id: str) -> AirflowRestClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        fabric_info = self._get_fabric_info(fabric_id)

        composer = fabric_info.composer
        mwaa = fabric_info.mwaa
        oss = fabric_info.airflow_oss

        if composer is not None:
            client = ComposerRestClient(
                composer.airflow_url, composer.project_id, composer.client_id, composer.key_json, composer.dag_location
            )
        elif mwaa is not None:
            client = MWAARestClient(
                mwaa.environment_name,
                mwaa.region,
                mwaa.access_key,
                mwaa.secret_key,
                mwaa.assumed_role,
                mwaa.custom_host,
            )

        elif oss is not None:
            return OpenSourceRestClient(
                oss.airflow_url,
                oss.airflow_username,
                oss.airflow_password,
                oss.uploader_url,
                oss.uploader_username,
                oss.uploader_password,
                oss.dag_location,
                oss.location,
            )

        else:
            raise ValueError(f"Fabric Id {fabric_id} is not defined.")

        if client is not None:
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

    def dataproc_client(self, fabric_id: str) -> ComposerRestClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        fabric_info = self._get_fabric_info(fabric_id)

        dataproc = fabric_info.dataproc

        if dataproc is not None:
            client = ComposerRestClient(
                "dummyAirflowUrl", dataproc.project_id, None, dataproc.key_json, "dummyDagLocation", dataproc.location
            )
        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

        if client is not None:
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

    def open_source_hdfs_client(self, fabric_id: str) -> OpenSourceRestClient:
        if self._get_client(fabric_id) is not None:
            return self._get_client(fabric_id)

        fabric_info = self._get_fabric_info(fabric_id)

        oss = fabric_info.airflow_oss

        if oss is not None:
            client = OpenSourceRestClient(
                oss.airflow_url,
                oss.airflow_username,
                oss.airflow_password,
                oss.uploader_url,
                oss.uploader_username,
                oss.uploader_password,
                oss.dag_location,
                oss.location,
            )
        else:
            raise ValueError("Fabric Id is not defined in the deployment state")

        if client is not None:
            self.fabric_id_to_rest_client[fabric_id] = client
            return client

    @staticmethod
    def get_instance(cls, fabric_config):
        if cls._instance is None:
            cls._instance = RestClientFactory(fabric_config)

        return cls._instance
