import os
import io
import logging
from datetime import datetime, timezone
import pandas as pd
from botocore.exceptions import ClientError
from src.config.minio_config import SILVER_BUCKET, GOLD_BUCKET
from src.utils.minio_client import get_s3_client

def setup_logging():
    """Configures streaming handlers for clean Airflow console tracking."""
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s", "%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

def ensure_bucket_exists(s3_client, bucket_name):
    """Verifies existence of the target storage bucket or creates it if missing."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"Bucket '{bucket_name}' not detected. Provisioning bucket storage workspace...")
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' successfully provisioned.")
        else:
            raise

def read_silver_parquet(s3_client, date_prefix):
    """Streams the cleaned Parquet file from Silver Layer directly into memory."""
    key = f"{date_prefix}/clean.parquet"
    try:
        logger.info(f"Fetching clean Silver file from: s3://{SILVER_BUCKET}/{key}")
        res = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(res['Body'].read()), engine='pyarrow')
    except ClientError as e:
        logger.critical(f"Failed to find or read Silver object {key}: {e}")
        raise

def generate_dimensional_model(silver_df):
    """Transforms raw-conformed Silver data into an optimized Star Schema model."""
    logger.info("Initializing dimensional model transformation sequence...")
    
    # 1. GENERATE DIM_CRYPTOCURRENCY
    logger.info("Constructing cryptocurrency dimension table...")
    dim_crypto = silver_df[['id', 'symbol', 'name']].copy()
    dim_crypto = dim_crypto.rename(columns={'id': 'crypto_id'}).drop_duplicates(subset=['crypto_id'])
    dim_crypto.columns = dim_crypto.columns.str.upper()
    
    # 2. GENERATE DIM_DATE
    logger.info("Constructing date dimension table...")
    reference_date = pd.to_datetime(silver_df['last_updated']).iloc[0]
    date_key = int(reference_date.strftime('%Y%m%d'))
    
    dim_date_row = {
        'DATE_KEY': date_key,
        'FULL_DATE': reference_date.date(),
        'CALENDAR_YEAR': int(reference_date.year),
        'MONTH_NUMBER': int(reference_date.month),
        'MONTH_NAME': reference_date.strftime('%B'),
        'WEEK_NUMBER': int(reference_date.isocalendar()[1]),
        'DAY_NAME': reference_date.strftime('%A')
    }
    dim_date = pd.DataFrame([dim_date_row])

    # 3. GENERATE FACT_MARKET_DAILY
    logger.info("Constructing daily market fact table with foreign keys...")
    fact_market = silver_df.copy()
    fact_market = fact_market.rename(columns={
        'id': 'crypto_id',
        'price_change_percentage_24h': 'price_change_pct',
        'ingested_at': 'collected_at'
    })
    fact_market['date_key'] = date_key
    
    fact_columns = [
        'crypto_id', 'date_key', 'current_price', 'market_cap', 'market_cap_rank',
        'total_volume', 'high_24h', 'low_24h', 'price_change_24h', 'price_change_pct', 'collected_at'
    ]
    fact_market = fact_market[[col for col in fact_columns if col in fact_market.columns]].copy()
    fact_market.insert(0, 'fact_key', range(1, len(fact_market) + 1))
    fact_market.columns = fact_market.columns.str.upper()
    
    # 4. VERIFY REFERENTIAL INTEGRITY
    logger.info("Validating dimensional model referential integrity constraints...")
    crypto_keys_valid = set(fact_market['CRYPTO_ID']).issubset(set(dim_crypto['CRYPTO_ID']))
    date_keys_valid = set(fact_market['DATE_KEY']).issubset(set(dim_date['DATE_KEY']))
    
    if not (crypto_keys_valid and date_keys_valid):
        error_msg = "Referential integrity constraint validation failed! Missing foreign key mappings."
        logger.critical(error_msg)
        raise ValueError(error_msg)
        
    logger.info("Referential integrity validation verified successfully. All keys resolved.")
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
    logger.info("--- Starting Step 3: Gold Dimensional Modeling Pipeline ---")
    try:
        s3 = get_s3_client()
        ensure_bucket_exists(s3, GOLD_BUCKET)
        
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        silver_data = read_silver_parquet(s3, partition)
        
        dim_crypto, dim_date, fact_market = generate_dimensional_model(silver_data)
        
        # Save each table as a distinct Parquet file to the Gold bucket
        upload_gold_parquet(s3, dim_crypto, "Dim_Cryptocurrency", partition)
        upload_gold_parquet(s3, dim_date, "Dim_Date", partition)
        upload_gold_parquet(s3, fact_market, "Fact_Market_Daily", partition)
        
        logger.info("🎉 Step 3: Gold layer transformation and storage sequence finalized successfully.\n")
    except Exception as e:
        logger.critical(f"❌ Step 3 Pipeline execution aborted: {e}\n")
        exit(1)