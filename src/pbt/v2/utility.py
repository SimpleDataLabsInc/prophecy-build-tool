# back-off :- 0 for making this fixed delay based retry
import base64
import binascii
import hashlib
import os
from enum import Enum
from typing import Optional, Any

from .project_models import LogEvent, StepMetadata, Status


def calculate_checksum(input_str, salt=None):
    salt = salt or "better_salt_than_never"

    md = hashlib.sha256()
    input_bytes = input_str.encode('utf-8')
    salt_bytes = salt.encode('utf-8')

    md.update(input_bytes)
    md.update(salt_bytes)

    digest_bytes = md.digest()

    return ''.join(f'{byte:02x}' for byte in digest_bytes)


def generate_secure_content(content: str, salt: str) -> str:
    iterations = 10000
    key_length = 128

    password = content.encode('utf-8')
    salt_bytes = salt.encode('utf-8')

    derived_key = hashlib.pbkdf2_hmac('sha256', password, salt_bytes, iterations, dklen=key_length // 8)
    hash_bytes = binascii.hexlify(derived_key)

    return base64.b64encode(hash_bytes).decode('utf-8').replace('\\W+', '_')


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
                 step_status: Optional[Status] = None):
    if os.environ.get('PRINT_MODE', 'REGULAR') == 'CUSTOM':
        # Custom print: Print all variables.
        if step_metadata is not None:
            log_event = LogEvent.from_step_metadata(step_metadata)
        elif step_status is not None:
            log_event = LogEvent.from_status(step_status, step_id)
        else:
            log_event = LogEvent.from_log(step_id, message, exception)

        print(log_event.to_json(), flush=True)
    else:
        # Regular print: Skip stepName.
        print(message, exception)


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
