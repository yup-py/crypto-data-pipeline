import io
from datetime import datetime, timezone
import pandas as pd
from botocore.exceptions import ClientError
from src.config.minio_config import SILVER_BUCKET, GOLD_BUCKET
from src.utils.minio_client import get_s3_client
from src.utils.pipeline_utils import get_pipeline_logger, ensure_bucket_exists

logger = get_pipeline_logger(__name__)

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
    dim_crypto = silver_df[['id', 'symbol', 'name']].copy()
    dim_crypto = dim_crypto.rename(columns={'id': 'crypto_id'}).drop_duplicates(subset=['crypto_id'])
    dim_crypto.columns = dim_crypto.columns.str.upper()
    
    # 2. GENERATE DIM_DATE
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

def run_gold_modeling(**kwargs):
    """Entry point for Airflow PythonOperator."""
    logger.info("--- Starting Step 3: Gold Dimensional Modeling Pipeline ---")
    s3 = get_s3_client()
    ensure_bucket_exists(s3, GOLD_BUCKET)
    
    partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    silver_data = read_silver_parquet(s3, partition)
    dim_crypto, dim_date, fact_market = generate_dimensional_model(silver_data)
    
    upload_gold_parquet(s3, dim_crypto, "Dim_Cryptocurrency", partition)
    upload_gold_parquet(s3, dim_date, "Dim_Date", partition)
    upload_gold_parquet(s3, fact_market, "Fact_Market_Daily", partition)
    logger.info("Gold layer transformation complete.")

if __name__ == "__main__":
    logger.info("--- Starting Step 3: Gold Dimensional Modeling Pipeline ---")
    try:
        run_gold_modeling()
        logger.info("🎉 Step 3: Gold layer transformation and storage sequence finalized successfully.\n")
    except Exception as e:
        logger.critical(f"❌ Step 3 Pipeline execution aborted: {e}\n")
        exit(1)