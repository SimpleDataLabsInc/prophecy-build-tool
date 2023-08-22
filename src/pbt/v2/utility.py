# back-off :- 0 for making this fixed delay based retry
import base64
import binascii
import hashlib
import os

from .project_models import LogEntry


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
    def is_right(self):
        return self.right is not None


def custom_print(message, exceptions=None, step_name=None):
    if os.environ.get('PRINT_MODE', 'REGULAR') == 'CUSTOM' and step_name is not None:
        # Custom print: Print all variables.
        print((LogEntry.from_log(step_name, message, exceptions).to_json()))
    else:
        # Regular print: Skip stepName.
        print(message, exceptions)
