import enum
import json
import traceback
from typing import List, Optional

from pydantic import BaseModel


class Operation(enum.Enum):
    Build = "Build"
    Add = "Add"
    Delete = "Delete"
    Remove = "Remove"
    Refresh = "Refresh"
    Skipped = "Skipped"
    Pause = "Pause"
    Rename = "Rename"
    Upload = "Upload"


class StepType(enum.Enum):
    Pipeline = "Pipeline"
    PipelineConfiguration = "PipelineConfiguration"
    Job = "Job"
    Script = "Script"
    DbtProfile = "DbtProfile"
    DbtSecret = "DbtSecret"
    DbtContent = "DbtContent"
    AirflowGitSecrets = "AirflowGitSecrets"
    Summary = "Summary"
    Subgraph = "Subgraph"  # why this?
    Project = "Project"
    Gems = "Gems"


class LogLevel(enum.Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARN = "WARN"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"
    TRACE = "TRACE"


class Colors:
    HEADER = "\033[35m"  # Purple
    OKBLUE = "\033[34m"  # Blue
    MAGENTA = "\033[35m"
    OKCYAN = "\033[36m"  # Cyan
    OKGREEN = "\033[38;2;2;122;72m"  # Green
    WARNING = "\033[38;2;181;71;8m"  # Yellow
    FAIL = "\033[38;2;180;35;24m"  # Red
    ENDC = "\033[0m"  # Reset color


def to_dict_recursive(obj):
    if isinstance(obj, enum.Enum):
        return obj.name

    if isinstance(obj, (list, tuple)):
        return [to_dict_recursive(item) for item in obj]
    elif isinstance(obj, dict):
        # Skip keys with None values
        return {key: to_dict_recursive(value) for key, value in obj.items() if value is not None}
    elif hasattr(obj, "__dict__"):
        return to_dict_recursive(
            {key: value for key, value in obj.__dict__.items() if not isinstance(value, type) and value is not None}
        )
    elif hasattr(obj, "__slots__"):
        return to_dict_recursive({slot: getattr(obj, slot) for slot in obj.__slots__ if getattr(obj, slot) is not None})
    else:
        return obj


class StepMetadata:
    def __init__(self, id: str, heading: str, operation: Operation, type: StepType):
        self.id = id.replace("\\W", "_")
        self.heading = heading
        self.operation = operation
        self.type = type

    def to_json(self):
        return json.dumps(self, default=to_dict_recursive)


class LogType(enum.Enum):
    Header = "Header"
    LogLine = "LogLine"
    Status = "Status"


class Status(enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class StepStatus:
    def __init__(self, step_id: str, status: Status):
        self.step_id = step_id
        self.status = status

    def to_json(self):
        # we want json to be in single line
        return json.dumps(self, default=to_dict_recursive)


class LogLine:
    def __init__(self, uid: str, log: str, level: LogLevel.INFO):
        self.uid = uid
        self.log = log
        self.level = level

    def to_json(self):
        # we want json to be in single line
        return json.dumps(self, default=to_dict_recursive)


class LogEvent:
    def __init__(self, step_id: str, type: LogType, log: Optional[str] = None):
        self.step_id = step_id
        self.type = type
        self.log = log

    def to_json(self):
        # we want json to be in single line
        return json.dumps(self, default=to_dict_recursive)

    @staticmethod
    def from_step_metadata(step_metadata: StepMetadata):
        return LogEvent(step_metadata.id, LogType.Header, step_metadata.to_json())

    @staticmethod
    def from_log(step_id: str, log: str, ex: Optional[Exception] = None, level: Optional[LogLevel] = None):
        log_level = level if level is not None else LogLevel.INFO
        if ex is not None:
            captured_trace = traceback.format_exc(limit=200)
            log = f"{log} exception message: {str(ex)}: Stacktrace: {'  '.join(captured_trace.splitlines())}"

            if log_level is not None:
                log_line = LogLine(step_id, log, level=log_level).to_json()
            else:
                log_line = LogLine(step_id, log, level=LogLevel.ERROR).to_json()

        else:
            log_line = LogLine(step_id, log, level=log_level).to_json()

        return LogEvent(step_id, LogType.LogLine, log_line)

    @staticmethod
    def from_status(status: Status, step_id: str):
        return LogEvent(step_id, LogType.Status, StepStatus(step_id, status).to_json())


class Component(BaseModel):
    id: str
    node_name: str
    path: Optional[str]
    language: str


class JobContent(BaseModel):
    job_id: str
    components: List[Component]
    enabled: bool
    fabric_id: str
    secret_scope: Optional[str]


class DbtComponentsModel:
    def __init__(self, fabric_id: str, secret_scope: str, components: List):
        self.fabric_id = str(fabric_id)
        self.secret_scope = secret_scope
        self.components = components


class ScriptComponentsModel:
    def __init__(self, fabric_id: str, scripts: List):
        self.scripts = scripts
        self.fabric_id = fabric_id


class DAG:
    def __init__(
        self,
        dag_id: str,
        description: Optional[str] = None,
        file_token: Optional[str] = None,
        fileloc: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_paused: bool = True,
        is_subdag: Optional[bool] = None,
        owners: List[str] = None,
        root_dag_id: Optional[str] = None,
        schedule_interval: Optional[str] = None,
        next_dagrun: Optional[str] = None,
        tags: List[str] = [],
    ):
        self.dag_id = dag_id
        self.description = description
        self.file_token = file_token
        self.fileloc = fileloc
        self.is_active = is_active
        self.is_paused = is_paused
        self.is_subdag = is_subdag
        self.owners = owners
        self.root_dag_id = root_dag_id
        self.schedule_interval = schedule_interval
        self.next_dagrun = next_dagrun
        self.tags = tags

    @staticmethod
    def create(responses: dict):
        dag_id = responses.get("dag_id")
        description = responses.get("description", None)
        file_token = responses.get("file_token", None)
        fileloc = responses.get("fileloc", None)
        is_active = responses.get("is_active", None)
        is_paused = responses.get("is_paused", None)
        is_subdag = responses.get("is_subdag", None)
        owners = responses.get("owners", [])
        root_dag_id = responses.get("root_dag_id", None)

        try:
            schedule_interval = responses.get("schedule_interval", {}).get("value", None)
        except AttributeError:
            schedule_interval = None

        next_dagrun = responses.get("next_dagrun", None)
        tags = responses.get("tags", [])
        return DAG(
            dag_id,
            description,
            file_token,
            fileloc,
            is_active,
            is_paused,
            is_subdag,
            owners,
            root_dag_id,
            schedule_interval,
            next_dagrun,
            tags,
        )

    # different from the scala release.
    @staticmethod
    def create_from_mwaa(response: dict):
        dag_id = response.get("dag_id")
        fileloc = response.get("filepath", None) or response.get("fileloc", None)
        is_paused = response.get("paused", None) or response.get("is_paused", None)
        owner = response.get("owner", None) or response.get("owners", None)
        owners = [] if owner is None else ([owner] if isinstance(owner, str) else owner)
        return DAG(dag_id, fileloc=fileloc, is_paused=is_paused is None or bool(is_paused), owners=owners)
