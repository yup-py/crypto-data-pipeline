import os
import io
import sys
import logging
from datetime import datetime, timezone
import boto3
import pandas as pd
from dotenv import load_dotenv

# Force Python to recognize the root directory and the src/ folder
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

# Clean package-level imports targeting your src directory
from src import transform_silver
from src import build_gold
from src import load_gold_to_snowflake

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s]: %(message)s")
logger = logging.getLogger("backfill_processor")

def get_all_bronze_dates():
    """Scans MinIO to find all historical date partitions that have a raw.json file."""
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
    detected_dates = set()
    
    logger.info(f"Scanning bucket '{BRONZE_BUCKET}' for historical JSON data partitions...")
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BRONZE_BUCKET):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('raw.json'):
                    date_path = "/".join(obj['Key'].split('/')[:3])
                    detected_dates.add(date_path)
                    
    return sorted(list(detected_dates))

def run_backfill():
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
    
    # 1. Establish connection to Snowflake
    try:
        snowflake_ctx = load_gold_to_snowflake.get_snowflake_connection()
        load_gold_to_snowflake.init_snowflake_schema(snowflake_ctx)
    except Exception as e:
        logger.critical(f"Failed to connect to Snowflake: {e}")
        return

    # 2. Scan MinIO for every single historical folder
    historical_partitions = get_all_bronze_dates()
    
    if not historical_partitions:
        logger.warning("No historical date partitions found in Bronze. Ensure MinIO is active.")
        snowflake_ctx.close()
        return
        
    logger.info(f"Found {len(historical_partitions)} partitions to process: {historical_partitions}")
    
    # 3. Process every day sequentially through Silver -> Gold -> Snowflake
    for partition in historical_partitions:
        logger.info(f"\n==================================================")
        logger.info(f"⏳ BACKFILLING PARTITION: {partition}")
        logger.info(f"==================================================")
        
        try:
            # --- STEP 2: SILVER LAYER ---
            logger.info(f"Running Step 2 (Silver) for {partition}...")
            raw_payload = transform_silver.read_bronze_json(s3, partition)
            cleaned_df = transform_silver.clean_and_normalize(raw_payload)
            transform_silver.upload_silver_parquet(s3, cleaned_df, partition)
            
            # --- STEP 3: GOLD LAYER ---
            logger.info(f"Running Step 3 (Gold) for {partition}...")
            # Automatically reads the newly generated silver file and creates/uploads the 3 star-schema parquets
            silver_data = build_gold.read_silver_parquet(s3, partition)
            dim_crypto, dim_date, fact_market = build_gold.generate_dimensional_model(silver_data)
            
            build_gold.upload_gold_parquet(s3, dim_crypto, "Dim_Cryptocurrency", partition)
            build_gold.upload_gold_parquet(s3, dim_date, "Dim_Date", partition)
            build_gold.upload_gold_parquet(s3, fact_market, "Fact_Market_Daily", partition)
            
            # --- STEP 4: SNOWFLAKE WAREHOUSE ---
            logger.info(f"Running Step 4 (Snowflake Ingestion) for {partition}...")
            df_crypto = load_gold_to_snowflake.read_gold_parquet(s3, "Dim_Cryptocurrency", partition)
            df_date = load_gold_to_snowflake.read_gold_parquet(s3, "Dim_Date", partition)
            df_fact = load_gold_to_snowflake.read_gold_parquet(s3, "Fact_Market_Daily", partition)
            
            load_gold_to_snowflake.load_dataframe_to_snowflake(snowflake_ctx, df_crypto, "DIM_CRYPTOCURRENCY")
            load_gold_to_snowflake.load_dataframe_to_snowflake(snowflake_ctx, df_date, "DIM_DATE")
            load_gold_to_snowflake.load_dataframe_to_snowflake(snowflake_ctx, df_fact, "FACT_MARKET_DAILY")
            
            logger.info(f"✅ Successfully backfilled partition: {partition}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process partition {partition}: {e}")
            continue
            
    snowflake_ctx.close()
    logger.info("\n🎉 All historical backfill processing complete!")

if __name__ == "__main__":
    run_backfill()