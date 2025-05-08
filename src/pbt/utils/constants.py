# File defining all the constants being used.


SCALA_LANGUAGE = "scala"
PYTHON_LANGUAGE = "python"

DATABRICKS_HOST = "DATABRICKS_HOST"
DATABRICKS_TOKEN = "DATABRICKS_TOKEN"

PROJECT_ID_PLACEHOLDER_REGEX = "__PROJECT_ID_PLACEHOLDER__"
PROPHECY_URL_PLACEHOLDER_REGEX = "__PROPHECY_URL_PLACEHOLDER__"
PROJECT_RELEASE_VERSION_PLACEHOLDER_REGEX = "__PROJECT_RELEASE_VERSION_PLACEHOLDER__"
PROJECT_RELEASE_TAG_PLACEHOLDER_REGEX = "__PROJECT_FULL_RELEASE_TAG_PLACEHOLDER__"
PROJECT_URL_PLACEHOLDER_REGEX = "__PROPHECY_URL_PLACEHOLDER__"

PIPELINE_ID = "PipelineId"

PBT_FILE_NAME = "pbt_project.yml"
LANGUAGE = "language"
JOBS = "jobs"
PIPELINES = "pipelines"
GEM_CONTAINER = "gemContainer"
GEMS = "gems"
PIPELINE_CONFIGURATIONS = "pipelineConfigurations"
BASE_PIPELINE = "basePipeline"
CONFIGURATIONS = "configurations"

JSON_EXTENSION = ".json"

DBT_COMPONENT = "DBTComponent"
SCRIPT_COMPONENT = "ScriptComponent"
PIPELINE_COMPONENT = "PipelineComponent"
COMPONENTS_LITERAL = "components"

FABRIC_ID = "fabric_id"
FABRIC_UID = "fabricUID"
SECRET_SCOPE = "secret_scope"

DBFS_FILE_STORE = "dbfs:/FileStore"
S3_FILE_STORE = "s3://"
HDFS_FILE_STORE = "hdfs://"
LOCAL_FILE_STORE = "/"
PROPHECY_ARTIFACTS = "prophecy/artifacts"

NEW_JOB_STATE_FILE = "new_state.yml"

MAVEN_SYNC_CONTEXT_FACTORY_OPTIONS = [
    "-Daether.syncContext.named.factory=file-lock",
    "-Daether.syncContext.named.nameMapper=file-gav",
    "-Daether.syncContext.named.time=120",
    # aether.syncContext.named.time (optional, default is 30): the time value for being blocked by trying to acquire a lock.
    # https://maven.apache.org/resolver/maven-resolver-impl/synccontextfactory.html
    # NOTE: may want to give a higher time. depending on how long builds block for...
]

JDK_JAVA_OPTIONS_ADD_EXPORTS = [
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
]
