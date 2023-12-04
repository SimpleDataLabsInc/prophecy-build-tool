import os
from enum import Enum
from typing import Optional, Any

from .utils.project_models import LogEvent, StepMetadata, Status, LogLevel


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


def custom_print(message: Optional[Any] = None, exception=None,
                 step_id=None,
                 step_metadata: Optional[StepMetadata] = None,
                 step_status: Optional[Status] = None, level: LogLevel = LogLevel.INFO, indent: int = 0) -> None:
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
        prefix_space = ''
        if indent != 0:
            prefix_space = ' ' * indent
        # Regular print: Skip stepName.
        if exception is not None:
            print(f'{prefix_space}{message}', exception)
        else:
            if message is not None:
                print(f'{prefix_space}{message}')


def is_online_mode() -> bool:
    return os.environ.get("PRINT_MODE", "REGULAR") == "CUSTOM"


# If the item is a dictionary


def remove_null_items_recursively(item):
    # If the item is a dictionary
    if isinstance(item, dict):
        return {
            k: remove_null_items_recursively(v)
            for k, v in item.items() if v is not None
        }

    # If the item is a list
    elif isinstance(item, list):
        return [remove_null_items_recursively(v) for v in item if v is not None]

    # If the item is an enum
    elif isinstance(item, Enum):
        return item.value

    # If the item is neither a dictionary, a list, nor an enum
    else:
        return item
