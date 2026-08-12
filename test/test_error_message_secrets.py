import pytest

from src.pbt.client.rest_client_factory import RestClientFactory
from src.pbt.utils.project_config import (
    ComposerInfo,
    DatabricksInfo,
    DataprocInfo,
    EMRInfo,
    FabricConfig,
    FabricInfo,
    FabricProviderType,
    FabricType,
    MwaaInfo,
    OAuthCredentials,
    OpenSourceAirflowInfo,
    ProjectAndGitTokens,
)

SECRET_VALUES = [
    "dapi-secret-databricks-token",
    "secret-oauth-client-secret",
    "secret-emr-access-key-id",
    "secret-emr-secret-access-key",
    "secret-emr-session-token",
    "secret-mwaa-access-key",
    "secret-mwaa-secret-key",
    "secret-airflow-password",
    "secret-uploader-password",
    "secret-composer-key-json",
    "secret-dataproc-key-json",
    "secret-git-token",
]


def build_fabric_config():
    return FabricConfig(
        fabrics=[
            FabricInfo(
                id="12038",
                name="dev_databricks_unified_fabric",
                type=FabricType.Sql,
                provider=FabricProviderType.Databricks,
                databricks=DatabricksInfo(
                    url="https://test.cloud.databricks.com",
                    token="dapi-secret-databricks-token",
                    auth_type="oauth",
                    user_agent="Prophecy",
                    oauth_credentials=OAuthCredentials(
                        client_id="test-client-id", client_secret="secret-oauth-client-secret"
                    ),
                ),
            ),
            FabricInfo(
                id="555",
                name="emr_fabric",
                type=FabricType.Spark,
                provider=FabricProviderType.EMR,
                emr=EMRInfo(
                    region="us-east-1",
                    bucket="s3://test-bucket/prefix",
                    access_key_id="secret-emr-access-key-id",
                    secret_access_key="secret-emr-secret-access-key",
                    session_token="secret-emr-session-token",
                ),
            ),
            FabricInfo(
                id="666",
                name="mwaa_fabric",
                type=FabricType.Airflow,
                provider=FabricProviderType.MWAA,
                mwaa=MwaaInfo(
                    region="us-east-1",
                    version="2.5.1",
                    access_key="secret-mwaa-access-key",
                    secret_key="secret-mwaa-secret-key",
                    airflow_url="https://mwaa.test",
                    dag_location="s3://dags",
                    environment_name="test-env",
                    assumed_role=None,
                    custom_host=None,
                ),
            ),
            FabricInfo(
                id="777",
                name="airflow_oss_fabric",
                type=FabricType.Airflow,
                provider=FabricProviderType.OpenSource,
                airflow_oss=OpenSourceAirflowInfo(
                    airflow_url="https://airflow.test",
                    airflow_username="airflow-user",
                    airflow_password="secret-airflow-password",
                    uploader_url="https://uploader.test",
                    uploader_username="uploader-user",
                    uploader_password="secret-uploader-password",
                    dag_location="/dags",
                    location="us",
                ),
            ),
            FabricInfo(
                id="888",
                name="composer_fabric",
                type=FabricType.Airflow,
                provider=FabricProviderType.Composer,
                composer=ComposerInfo(
                    key_json="secret-composer-key-json",
                    version="2.5.1",
                    project_id="test-gcp-project",
                    airflow_url="https://composer.test",
                    dag_location="gs://dags",
                ),
            ),
            FabricInfo(
                id="999",
                name="dataproc_fabric",
                type=FabricType.Spark,
                provider=FabricProviderType.Dataproc,
                dataproc=DataprocInfo(
                    bucket="gs://test-bucket/prefix",
                    project_id="test-gcp-project",
                    key_json="secret-dataproc-key-json",
                    location="us",
                ),
            ),
        ],
        project_git_tokens=[ProjectAndGitTokens(project_id="72805", git_token="secret-git-token")],
    )


def leaked_secrets(text):
    return [secret for secret in SECRET_VALUES if secret in text]


def test_fabric_not_found_error_does_not_expose_secrets():
    factory = RestClientFactory(build_fabric_config())

    with pytest.raises(ValueError) as exc_info:
        factory._get_fabric_info("7")

    message = str(exc_info.value)
    assert leaked_secrets(message) == [], f"error message exposed secrets: {message}"
    assert "Fabric Id 7 is not found in the fabric configs" in message
    assert "['12038', '555', '666', '777', '888', '999']" in message


def test_missing_databricks_config_error_does_not_expose_secrets():
    fabric_config = build_fabric_config()
    factory = RestClientFactory(fabric_config)

    with pytest.raises(ValueError) as exc_info:
        factory.databricks_client("888")

    message = str(exc_info.value)
    assert leaked_secrets(message) == [], f"error message exposed secrets: {message}"
    assert "does not have a databricks configuration defined" in message
    assert "composer_fabric" in message
