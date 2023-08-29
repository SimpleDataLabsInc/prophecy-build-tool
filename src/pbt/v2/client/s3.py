import boto3


class S3Client:

    def __init__(self, region, access_key, secret_key, aws_session_token=None):
        self.s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=aws_session_token
        )

    def put_file(self, bucket, key, file_path):
        self.s3.upload_file(file_path=file_path, bucket=bucket, key=key)
