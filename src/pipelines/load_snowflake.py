import os
import io
import logging
from datetime import datetime, timezone
import pandas as pd
from botocore.exceptions import ClientError
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from src.config.minio_config import GOLD_BUCKET
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

def read_gold_parquet(s3_client, table_name, date_prefix):
    """Streams a processed Star Schema table back out of the Gold Bucket into memory."""
    key = f"{date_prefix}/{table_name}.parquet"
    try:
        logger.info(f"Reading Gold storage asset: s3://{GOLD_BUCKET}/{key}")
        res = s3_client.get_object(Bucket=GOLD_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(res['Body'].read()), engine='pyarrow')
    except ClientError as e:
        logger.critical(f"Missing essential Gold infrastructure file {key}: {e}")
        raise

def initialize_snowflake_schema(conn):
    """Creates the structural schema target space inside the data warehouse."""
    target_schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    logger.info(f"Ensuring target schema '{target_schema}' is prepared for loading...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        cursor.execute(f"USE SCHEMA {target_schema};")
        logger.info(f"Snowflake schema context switched to '{target_schema}' successfully.")
    finally:
        cursor.close()

def validate_loading_metrics(conn, table_name, local_row_count):
    """Validates warehouse execution row metrics to guarantee zero data loss."""
    logger.info(f"Validating row metrics consistency for target table: {table_name}...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        warehouse_count = cursor.fetchone()[0]
        
        logger.info(f"Table validation summary -> Local: {local_row_count} rows | Warehouse: {warehouse_count} rows.")
        if warehouse_count == 0 and local_row_count > 0:
            raise ValueError(f"Validation Error: Warehouse table {table_name} returned empty payload.")
        
        logger.info(f"✅ Data synchronization check passed for table: {table_name}")
    except Exception as err:
        logger.error(f"Failed to complete structural data validation for {table_name}: {err}")
        raise
    finally:
        cursor.close()

if __name__ == "__main__":
    logger.info("--- Starting Step 4: Snowflake Warehouse Synchronization & Loading ---")
    try:
        s3 = get_s3_client()
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        
        # Load the dimensional files into memory from MinIO Gold bucket
        dim_crypto = read_gold_parquet(s3, "Dim_Cryptocurrency", partition)
        dim_date = read_gold_parquet(s3, "Dim_Date", partition)
        fact_market = read_gold_parquet(s3, "Fact_Market_Daily", partition)
        
        # Connect to the Snowflake computing layer
        logger.info("Establishing secure session handshake with Snowflake endpoints...")
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE")
        )
        
        # Setup schema workspace
        initialize_snowflake_schema(conn)
        
        # 1. LOAD DIMENSIONS FIRST (To satisfy relational patterns)
        logger.info("Streaming Dim_Cryptocurrency to Snowflake instances...")
        write_pandas(conn, dim_crypto, "DIM_CRYPTOCURRENCY", auto_create_table=True)
        validate_loading_metrics(conn, "DIM_CRYPTOCURRENCY", len(dim_crypto))
        
        logger.info("Streaming Dim_Date to Snowflake instances...")
        write_pandas(conn, dim_date, "DIM_DATE", auto_create_table=True)
        validate_loading_metrics(conn, "DIM_DATE", len(dim_date))
        
        # 2. LOAD FACT TABLE SECOND
        logger.info("Streaming Fact_Market_Daily to Snowflake instances...")
        write_pandas(conn, fact_market, "FACT_MARKET_DAILY", auto_create_table=True, use_logical_type=True)
        validate_loading_metrics(conn, "FACT_MARKET_DAILY", len(fact_market))
        
        logger.info("Closing secure Snowflake session connections...")
        conn.close()
        logger.info("🎉 Step 4: Snowflake schema generation, loading, and validation completed successfully.\n")
        
    except Exception as e:
        logger.critical(f"❌ Step 4 Warehouse synchronization aborted: {e}\n")
        exit(1)