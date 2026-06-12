import os
import io
import logging
from datetime import datetime, timezone
import pandas as pd
import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load configuration values
load_dotenv()

# MinIO / Storage Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")

# Snowflake Target Warehouse Configuration
SNOW_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOW_USER = os.getenv("SNOWFLAKE_USER")
SNOW_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOW_WH = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOW_DB = os.getenv("SNOWFLAKE_DATABASE")
SNOW_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_snowflake.log"
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s", "%Y-%m-%d %H:%M:%S")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    for handler in [logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger

logger = setup_logging()

def get_snowflake_connection():
    """Establishes a native session connection to the Snowflake Cloud Data Warehouse."""
    logger.info("Initiating connection parameters to Snowflake cluster...")
    return snowflake.connector.connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        warehouse=SNOW_WH,
        database=SNOW_DB,
        schema=SNOW_SCHEMA
    )

def init_snowflake_schema(ctx):
    """Executes structural DDL layouts matching your Step 0 Star Schema design."""
    cursor = ctx.cursor()
    try:
        logger.info(f"Initializing Target Data Warehouse database: {SNOW_DB} and logical schema: {SNOW_SCHEMA}...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOW_DB};")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOW_DB}.{SNOW_SCHEMA};")
        cursor.execute(f"USE SCHEMA {SNOW_DB}.{SNOW_SCHEMA};")
        
        # 1. Structural Dimension: Dim_Cryptocurrency
        logger.info("Validating table constraints for DIM_CRYPTOCURRENCY...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_CRYPTOCURRENCY (
                CRYPTO_ID VARCHAR PRIMARY KEY,
                SYMBOL VARCHAR,
                NAME VARCHAR
            );
        """)
        
        # 2. Structural Dimension: Dim_Date
        logger.info("Validating table constraints for DIM_DATE...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_DATE (
                DATE_KEY INTEGER PRIMARY KEY,
                FULL_DATE DATE,
                CALENDAR_YEAR INTEGER,
                MONTH_NUMBER INTEGER,
                MONTH_NAME VARCHAR,
                WEEK_NUMBER INTEGER,
                DAY_NAME VARCHAR
            );
        """)
        
        # 3. Structural Fact Ledger: Fact_Market_Daily
        logger.info("Validating table constraints for FACT_MARKET_DAILY...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FACT_MARKET_DAILY (
                FACT_KEY BIGINT PRIMARY KEY,
                CRYPTO_ID VARCHAR FOREIGN KEY REFERENCES DIM_CRYPTOCURRENCY(CRYPTO_ID),
                DATE_KEY INTEGER FOREIGN KEY REFERENCES DIM_DATE(DATE_KEY),
                CURRENT_PRICE FLOAT,
                MARKET_CAP BIGINT,
                MARKET_CAP_RANK INTEGER,
                TOTAL_VOLUME FLOAT,
                HIGH_24H FLOAT,
                LOW_24H FLOAT,
                PRICE_CHANGE_24H FLOAT,
                PRICE_CHANGE_PCT FLOAT,
                COLLECTED_AT TIMESTAMP_TZ
            );
        """)
        logger.info("✅ DDL constraints successfully mapped and instantiated inside Snowflake engine.")
    except Exception as e:
        logger.critical(f"DDL structural statement execution error encountered: {e}")
        raise
    finally:
        cursor.close()

def read_gold_parquet(s3_client, table_name, date_prefix):
    """Streams a specified Gold target Parquet snapshot asset directly from MinIO."""
    key = f"{date_prefix}/{table_name}.parquet"
    try:
        res = s3_client.get_object(Bucket=GOLD_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(res['Body'].read()), engine='pyarrow')
    except Exception as e:
        logger.error(f"Failed to extract Gold data layer object at path {key}: {e}")
        raise

def load_dataframe_to_snowflake(ctx, df, target_table):
    """Loads a unified Pandas DataFrame directly into Snowflake and audits transaction records."""
    # Standardize column structures into upper case strings to conform to Snowflake defaults
    df.columns = [col.upper() for col in df.columns]
    
    logger.info(f"Staging batch transaction containing {len(df)} records targeting table {target_table}...")
    
    # write_pandas chunks data and runs optimized COPY INTO sequences via a temporary internal stage
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=df,
        table_name=target_table,
        database=SNOW_DB,
        schema=SNOW_SCHEMA,
        use_logical_type=True
    )
    
    if success:
        logger.info(f"✅ Data load acknowledged: {nrows} rows successfully committed to {target_table} ({nchunks} chunks).")
    else:
        raise RuntimeError(f"Snowflake transaction aborted for structural target: {target_table}")

if __name__ == "__main__":
    logger.info("--- Starting Snowflake Cloud Data Warehouse Loading Sequence (Stage 4) ---")
    
    try:
        # 1. Initialize MinIO Client and resolve date path targets
        s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        
        # 2. Extract Star Schema assets from storage
        logger.info(f"Accessing Parquet model components for target date partition: {partition}")
        df_crypto = read_gold_parquet(s3, "Dim_Cryptocurrency", partition)
        df_date = read_gold_parquet(s3, "Dim_Date", partition)
        df_fact = read_gold_parquet(s3, "Fact_Market_Daily", partition)
        
        # 3. Create a live database connection and instantiate tables
        ctx = get_snowflake_connection()
        init_snowflake_schema(ctx)
        
        # 4. Strict Loading Order: Load parent dimensions first to preserve referential integrity
        load_dataframe_to_snowflake(ctx, df_crypto, "DIM_CRYPTOCURRENCY")
        load_dataframe_to_snowflake(ctx, df_date, "DIM_DATE")
        load_dataframe_to_snowflake(ctx, df_fact, "FACT_MARKET_DAILY")
        
        logger.info("🎉 Step 4 Complete: Cloud Data Warehouse synchronized and verified successfully!")
        
    except Exception as e:
        logger.critical(f"❌ Critical Error: Snowflake data warehouse migration sequence aborted: {e}")
        exit(1)
    finally:
        if 'ctx' in locals():
            ctx.close()
            logger.info("Snowflake active network communication channel safely closed.")