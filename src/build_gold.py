import os
import io
import logging
from datetime import datetime, timezone
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load configuration values
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "crypto-silver")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_gold.log"
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s", "%Y-%m-%d %H:%M:%S")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    for handler in [logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger

logger = setup_logging()

def ensure_bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Provisioned bucket: {bucket_name}")
        else:
            raise

def read_silver_parquet(s3_client, date_prefix):
    """Streams the cleaned Parquet file from Silver Layer directly into memory."""
    key = f"{date_prefix}/clean.parquet"
    try:
        res = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(res['Body'].read()), engine='pyarrow')
    except ClientError as e:
        logger.critical(f"Failed to find or read Silver object {key}: {e}")
        raise

def generate_dimensional_model(silver_df):
    logger.info("Initializing dimensional model transformation sequence...")
    
    # 1. GENERATE DIM_CRYPTOCURRENCY
    # Drop duplicates to keep unique dimension records
    dim_crypto = silver_df[['id', 'symbol', 'name']].copy()
    dim_crypto = dim_crypto.rename(columns={'id': 'crypto_id'}).drop_duplicates(subset=['crypto_id'])
    
    # 2. GENERATE DIM_DATE
    # Use 'last_updated' as the reference point to capture dates cleanly
    reference_date = pd.to_datetime(silver_df['last_updated']).iloc[0]
    date_key = int(reference_date.strftime('%Y%m%d'))
    
    dim_date_row = {
        'date_key': date_key,
        'full_date': reference_date.date(),
        'calendar_year': int(reference_date.year),
        'month_number': int(reference_date.month),
        'month_name': reference_date.strftime('%B'),
        'week_number': int(reference_date.isocalendar()[1]),
        'day_name': reference_date.strftime('%A')
    }
    dim_date = pd.DataFrame([dim_date_row])

    # 3. GENERATE FACT_MARKET_DAILY
    fact_market = silver_df.copy()
    fact_market = fact_market.rename(columns={
        'id': 'crypto_id',
        'price_change_percentage_24h': 'price_change_pct',
        'ingested_at': 'collected_at'
    })
    
    # Assign our date foreign key explicitly
    fact_market['date_key'] = date_key
    
    # Retain only explicit metrics declared in the target star schema
    fact_columns = [
        'crypto_id', 'date_key', 'current_price', 'market_cap', 'market_cap_rank',
        'total_volume', 'high_24h', 'low_24h', 'price_change_24h', 'price_change_pct', 'collected_at'
    ]
    fact_market = fact_market[[col for col in fact_columns if col in fact_market.columns]].copy()
    
    # Assign an auto-incrementing surrogate Primary Key (fact_key) starting from 1
    fact_market.insert(0, 'fact_key', range(1, len(fact_market) + 1))
    
    # --- REFERENTIAL INTEGRITY CHECK ---
    logger.info("Validating dimensional model referential integrity constraints...")
    
    crypto_keys_valid = set(fact_market['crypto_id']).issubset(set(dim_crypto['crypto_id']))
    date_keys_valid = set(fact_market['date_key']).issubset(set(dim_date['date_key']))
    
    if not (crypto_keys_valid and date_keys_valid):
        error_msg = "Referential integrity constraint validation failed! Missing foreign key mappings."
        logger.critical(error_msg)
        raise ValueError(error_msg)
        
    logger.info("✅ Referential integrity validation verified successfully. All keys resolved.")
    return dim_crypto, dim_date, fact_market

def upload_gold_parquet(s3_client, df, table_name, date_prefix):
    """Uploads table as a distinct Parquet file inside its own object path directory."""
    key = f"{date_prefix}/{table_name}.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=GOLD_BUCKET, Key=key, Body=buffer.getvalue(), ContentType='application/x-parquet'
    )
    logger.info(f"Successfully uploaded Gold asset to s3://{GOLD_BUCKET}/{key}")

if __name__ == "__main__":
    logger.info("--- Starting Daily Gold Model Extraction ---")
    try:
        s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
        ensure_bucket_exists(s3, GOLD_BUCKET)
        
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        
        # Pull data from Silver Layer
        silver_data = read_silver_parquet(s3, partition)
        
        # Transform and isolate into Star Schema DataFrames
        dim_crypto, dim_date, fact_market = generate_dimensional_model(silver_data)
        
        # Save structural files separately into the Gold bucket
        upload_gold_parquet(s3, dim_crypto, "Dim_Cryptocurrency", partition)
        upload_gold_parquet(s3, dim_date, "Dim_Date", partition)
        upload_gold_parquet(s3, fact_market, "Fact_Market_Daily", partition)
        
        logger.info("🎉 Étape 3: Gold layer processing completed successfully with structured Parquet snapshots.")
    except Exception as e:
        logger.critical(f"❌ Pipeline Stage 3 aborted: {e}")
        exit(1)