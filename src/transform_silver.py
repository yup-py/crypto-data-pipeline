import os
import io
import json
import logging
from datetime import datetime, timezone
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MinIO Connection Configuration (Using optimized implicit credentials)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
BRONZE_BUCKET = os.environ.get("MINIO_BRONZE_BUCKET", "crypto-bronze")
SILVER_BUCKET = os.environ.get("MINIO_SILVER_BUCKET", "crypto-silver")

# Setup Daily Logging Infrastructure
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_filename = f"logs/{current_date}_transform.log"
    
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# Runtime Guard Rails
if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
    logger.critical("Infrastructure automation credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) are missing from the environment.")
    raise ValueError("CRITICAL: Storage layer validation credentials missing.")

def get_s3_client():
    """Initializes the boto3 S3 client targeted at MinIO, leveraging native env credentials."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        region_name='us-east-1'
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Verifies existence of the target storage bucket or creates it if missing."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Target validation passed: Bucket '{bucket_name}' is active.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.warning(f"Bucket '{bucket_name}' does not exist. Initializing provisioning sequence...")
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Successfully provisioned infrastructure storage bucket: {bucket_name}")
        else:
            raise

def read_bronze_json(s3_client, date_prefix):
    """Reads raw JSON dataset directly from MinIO Bronze layer using strictly in-memory buffers."""
    object_key = f"{date_prefix}/raw.json"
    logger.info(f"Extracting source object metadata path: s3://{BRONZE_BUCKET}/{object_key}")
    
    try:
        response = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=object_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        return json_data
    except ClientError as e:
        logger.critical(f"Failed to extract Bronze object path for partition target {date_prefix}: {e}")
        raise

def clean_and_normalize(json_data):
    """Applies robust schema structural processing, field normalization, and casting rules."""
    logger.info("Initializing in-memory transformations and metric cleansing with Pandas...")
    
    # Convert array maps to working structures
    df = pd.DataFrame(json_data)
    
    # Normalize structural header naming schemas to snake_case rules
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_', regex=False)
    
    # Retain structured target analytic columns
    columns_to_keep = [
        'id', 'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
        'fully_diluted_valuation', 'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h',
        'market_cap_change_percentage_24h', 'circulating_supply', 'total_supply',
        'max_supply', 'ath', 'atl', 'last_updated'
    ]
    
    available_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[available_columns]
    
    # Safe date serialization conversions
    if 'last_updated' in df.columns:
        df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
    
    # Enforce numeric robustness against empty tracking values
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0.0)
    
    # Enrich record metadata tracking metrics
    df['ingested_at'] = datetime.now(timezone.utc)
    
    logger.info(f"Transformation engine finalized. Final workspace dimensions: {df.shape[0]} rows x {df.shape[1]} features.")
    return df

def upload_silver_parquet(s3_client, df, date_prefix):
    """Serializes transformed dataframes into columnar Parquet binaries directly streaming to MinIO."""
    object_key = f"{date_prefix}/clean.parquet"
    
    # Create in-memory stream wrapper to circumvent writing files locally to disk
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
    parquet_buffer.seek(0)
    
    logger.info(f"Preparing compressed object streaming transmission targeting: s3://{SILVER_BUCKET}/{object_key}")
    s3_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=object_key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/x-parquet'
    )
    logger.info("Silver target data object transport serialization successfully finalized.")

if __name__ == "__main__":
    logger.info("--- Beginning Automated Daily Silver Transformation Run ---")
    try:
        s3 = get_s3_client()
        ensure_bucket_exists(s3, SILVER_BUCKET)
        
        # Resolve today's working folder partition structure
        current_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        
        # Execute processing pipeline sequence steps
        raw_data = read_bronze_json(s3, current_partition)
        cleaned_df = clean_and_normalize(raw_data)
        upload_silver_parquet(s3, cleaned_df, current_partition)
        
        logger.info("🎉 Étape 2: Silver layer processing architecture finalized successfully.\n")
    except Exception as e:
        logger.critical(f"❌ Pipeline Stage 2 execution sequence aborted: {e}\n")
        exit(1)