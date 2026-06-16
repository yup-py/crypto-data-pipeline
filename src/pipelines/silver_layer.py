import os
import io
import json
from datetime import datetime, timezone
import pandas as pd
from botocore.exceptions import ClientError
from src.config.minio_config import BRONZE_BUCKET, SILVER_BUCKET
from src.utils.minio_client import get_s3_client
from src.utils.pipeline_utils import get_pipeline_logger, ensure_bucket_exists

logger = get_pipeline_logger(__name__)

if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
    logger.critical("AWS storage credentials missing from environment.")
    raise ValueError("CRITICAL: Storage validation credentials missing.")

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
    
    columns_to_keep = [
        'id', 'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
        'fully_diluted_valuation', 'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h',
        'market_cap_change_percentage_24h', 'circulating_supply', 'total_supply',
        'max_supply', 'ath', 'atl', 'last_updated'
    ]
    df = df[[col for col in columns_to_keep if col in df.columns]].copy()
    
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

def run_silver_transformation(**kwargs):
    """Entry point for Airflow PythonOperator."""
    logger.info("--- Starting Daily Silver Transformation ---")
    s3 = get_s3_client()
    ensure_bucket_exists(s3, SILVER_BUCKET)
    
    partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    raw_payload = read_bronze_json(s3, partition)
    cleaned_df = clean_and_normalize(raw_payload)
    upload_silver_parquet(s3, cleaned_df, partition)
    logger.info("Silver layer processing completed.")

if __name__ == "__main__":
    logger.info("--- Starting Daily Silver Transformation ---")
    try:
        run_silver_transformation()
        logger.info("🎉 Étape 2: Silver layer processing completed successfully.")
    except Exception as e:
        logger.critical(f"❌ Pipeline Stage 2 aborted: {e}")
        exit(1)