import logging
from botocore.exceptions import ClientError
from src.utils.minio_client import get_s3_client
from src.config.minio_config import BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def init_storage():
    """Validates the target environment and dynamically builds missing Medallion buckets using boto3."""
    s3_client = get_s3_client()
    buckets = [BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET]
    
    for bucket_name in buckets:
        try:
            # Check if bucket exists using standard S3 API head requests
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Storage infrastructure verification passed for bucket: '{bucket_name}'.")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Bucket '{bucket_name}' not detected. Provisioning bucket storage workspace...")
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket '{bucket_name}' successfully provisioned.")
            else:
                logger.error(f"Infrastructural failure encountered while validating bucket '{bucket_name}': {e}")
                raise

if __name__ == "__main__":
    init_storage()