import os
import io
import json
import logging
from datetime import datetime, timezone
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "crypto-silver")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_transform.log"
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s", "%Y-%m-%d %H:%M:%S")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    for handler in [logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger

logger = setup_logging()

if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
    logger.critical("AWS storage credentials missing from environment.")
    raise ValueError("CRITICAL: Storage validation credentials missing.")

def ensure_bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Provisioned bucket: {bucket_name}")
        else:
            raise

def read_bronze_json(s3_client, date_prefix):
    key = f"{date_prefix}/raw.json"
    try:
        res = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=key)
        return json.loads(res['Body'].read().decode('utf-8'))
    except ClientError as e:
        logger.critical(f"Failed to read Bronze object {key}: {e}")
        raise

def clean_and_normalize(json_data):
    logger.info("Running data cleaning and normalization...")
    df = pd.DataFrame(json_data)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_', regex=False)
    
    # White-list columns: 'roi' and 'image' are dropped implicitly by omission
    columns_to_keep = [
        'id', 'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
        'fully_diluted_valuation', 'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h',
        'market_cap_change_percentage_24h', 'circulating_supply', 'total_supply',
        'max_supply', 'ath', 'atl', 'last_updated'
    ]
    df = df[[col for col in columns_to_keep if col in df.columns]].copy()
    
    # Contextual Imputation: Handle new tokens/low liquidity without introducing fake 0.0 drops
    for col in ['high_24h', 'low_24h']:
        if col in df.columns and 'current_price' in df.columns:
            df[col] = df[col].fillna(df['current_price'])
            
    if 'last_updated' in df.columns:
        df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce')
        
    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(0.0)
    df['ingested_at'] = datetime.now(timezone.utc)
    
    logger.info(f"Transformation complete. Dimensions: {df.shape[0]} rows x {df.shape[1]} features.")
    return df

def upload_silver_parquet(s3_client, df, date_prefix):
    key = f"{date_prefix}/clean.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=SILVER_BUCKET, Key=key, Body=buffer.getvalue(), ContentType='application/x-parquet'
    )
    logger.info(f"Successfully uploaded Parquet to s3://{SILVER_BUCKET}/{key}")

if __name__ == "__main__":
    logger.info("--- Starting Daily Silver Transformation ---")
    try:
        s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
        ensure_bucket_exists(s3, SILVER_BUCKET)
        
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        raw_payload = read_bronze_json(s3, partition)
        cleaned_df = clean_and_normalize(raw_payload)
        upload_silver_parquet(s3, cleaned_df, partition)
        
        logger.info("🎉 Étape 2: Silver layer processing completed successfully.")
    except Exception as e:
        logger.critical(f"❌ Pipeline Stage 2 aborted: {e}")
        exit(1)