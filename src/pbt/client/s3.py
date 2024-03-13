from typing import Optional

import boto3
from botocore.client import BaseClient
from ..utility import get_temp_aws_role_creds


class S3Client:
    def __init__(
        self,
        region: str,
        access_key: Optional[str],
        secret_key: Optional[str],
        aws_session_token: Optional[str] = None,
        assumed_role: Optional[str] = None,
    ):
        if assumed_role:
            temporary_credentials = get_temp_aws_role_creds(assumed_role, access_key, secret_key)
            self.s3: BaseClient = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=temporary_credentials["AccessKeyId"],
                aws_secret_access_key=temporary_credentials["SecretAccessKey"],
                aws_session_token=temporary_credentials["SessionToken"],
            )

        else:
            self.s3: BaseClient = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=aws_session_token,
            )

    def upload_file(self, bucket, key, file_path):
        self.s3.upload_file(file_path, bucket, key)

    def upload_content(self, bucket, path, content):
        self.s3.put_object(Bucket=bucket, Key=path, Body=content)
