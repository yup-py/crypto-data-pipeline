import boto3
from src.config.minio_config import MINIO_ENDPOINT

def get_s3_client():
    """Initializes a unified boto3 S3 client targeted at MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        region_name='us-east-1'
    )