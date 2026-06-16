import logging
from botocore.exceptions import ClientError
from src.utils.minio_client import get_s3_client

def get_pipeline_logger(name=None):
    """Configures centralized streaming logging handlers for clean Airflow console tracking."""
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    return logger

def ensure_bucket_exists(s3_client, bucket_name):
    """Verifies existence of the target storage bucket or creates it if missing."""
    logger = logging.getLogger(__name__)
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Target validation passed: Bucket '{bucket_name}' is active.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.warning(f"Bucket '{bucket_name}' not detected. Initializing provisioning sequence...")
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Successfully provisioned infrastructure storage bucket: {bucket_name}")
        else:
            logger.error(f"Failed to validate bucket existence with MinIO server code: {error_code}")
            raise