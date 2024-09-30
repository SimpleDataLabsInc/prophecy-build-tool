import os
import re
from enum import Enum
from typing import Any, Optional

from .utils.constants import SCALA_LANGUAGE
from .utils.project_models import LogEvent, LogLevel, Status, StepMetadata


# class mimicking Either behaviour to capture both success and failure
class Either:
    def __init__(self, left=None, right=None):
        self.left = left
        self.right = right

    @property
    def is_left(self):
        return self.left is not None

    @property
    def is_right(self) -> bool:
        return self.right is not None


def custom_print(
    message: Optional[Any] = None,
    exception=None,
    step_id=None,
    step_metadata: Optional[StepMetadata] = None,
    step_status: Optional[Status] = None,
    level: LogLevel = LogLevel.INFO,
    indent: int = 0,
) -> None:
    if is_online_mode():
        # Custom print: Print all variables.
        if step_metadata is not None:
            log_event = LogEvent.from_step_metadata(step_metadata)
        elif step_status is not None:
            log_event = LogEvent.from_status(step_status, step_id)
        else:
            log_event = LogEvent.from_log(step_id, message, exception)

        print(log_event.to_json(), flush=True)
    else:
        prefix_space = ""
        if indent != 0:
            prefix_space = " " * indent
        # Regular print: Skip stepName.
        if exception is not None:
            print(f"{prefix_space}{message}", exception)
        else:
            if message is not None:
                print(f"{prefix_space}{message}")


def is_online_mode() -> bool:
    return os.environ.get("PRINT_MODE", "REGULAR") == "CUSTOM"


# If the item is a dictionary


def remove_null_items_recursively(item):
    # If the item is a dictionary
    if isinstance(item, dict):
        return {k: remove_null_items_recursively(v) for k, v in item.items() if v is not None}

    # If the item is a list
    elif isinstance(item, list):
        return [remove_null_items_recursively(v) for v in item if v is not None]

    # If the item is an enum
    elif isinstance(item, Enum):
        return item.value

    # If the item is neither a dictionary, a list, nor an enum
    else:
        return item


def python_pipeline_name(pipeline_name: str):
    # todo combine in a single regex
    regex_match = r"[^\w\d.]+"
    underscore_regex = r"(_)\1+"
    result = re.sub(regex_match, "_", pipeline_name)
    return re.sub(underscore_regex, "_", result)


def get_package_name(project_language: str, pipeline_name: str):
    if project_language == SCALA_LANGUAGE:
        return f"{pipeline_name}.jar"
    else:
        result = python_pipeline_name(pipeline_name)
        return f"{result}-1.0-py3-none-any.whl"


def isBlank(input):
    if isinstance(input, str) and input and input.strip():
        return False
    return True


def get_temp_aws_role_creds(role_arn: str, _access_key: str, _secret_key: str):
    import boto3

    if isBlank(_access_key) or isBlank(_secret_key):
        # Retrieve credentials from the instance profile or environment
        credentials = boto3.Session()
    else:
        # Use Static Credentials
        credentials = boto3.Session(aws_access_key_id=_access_key, aws_secret_access_key=_secret_key)

    sts_client = credentials.client("sts")
    sts_assumed_role = sts_client.assume_role(RoleArn=role_arn, RoleSessionName="ProphecyReleaseAssumedRoleSession")
    return sts_assumed_role["Credentials"]
