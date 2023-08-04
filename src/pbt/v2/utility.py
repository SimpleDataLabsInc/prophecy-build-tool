# back-off :- 0 for making this fixed delay based retry
import base64
import binascii
import hashlib
import os


def retry(exceptions, total_tries: int = 3, delay_in_seconds: int = 1, backoff: int = 2):
    def decorator(func):
        import time
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = total_tries
            while retries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if retries < 1:
                        raise e
                    print(f"Retrying in {delay_in_seconds} seconds...", e)
                    time.sleep(delay_in_seconds)
                    retries -= 1
                    if backoff != 0:
                        delay_in_seconds = delay_in_seconds * backoff

            return func(*args, **kwargs)

        return wrapper

    return decorator


def calculate_checksum(input_str, salt=None):
    salt = salt or os.getenv('PROPHECY_EXECUTION_SALT') or "prophecy_execution"

    sha256 = hashlib.sha256()
    input_bytes = input_str.encode('utf-8')
    salt_bytes = salt.encode('utf-8')

    sha256.update(input_bytes)
    sha256.update(salt_bytes)

    digest_bytes = sha256.digest()

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
