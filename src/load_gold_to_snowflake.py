import os
import io
import logging
from datetime import datetime, timezone
import pandas as pd
import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s]: %(message)s")
logger = logging.getLogger("snowflake_loader")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")
SNOW_ACCOUNT, SNOW_USER, SNOW_PASSWORD = os.getenv("SNOWFLAKE_ACCOUNT"), os.getenv("SNOWFLAKE_USER"), os.getenv("SNOWFLAKE_PASSWORD")
SNOW_WH, SNOW_DB, SNOW_SCHEMA = os.getenv("SNOWFLAKE_WAREHOUSE"), os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_DW"), os.getenv("SNOWFLAKE_SCHEMA", "GOLD")

def get_snowflake_connection():
    # Force context session parameters down the connection handshake pipeline
    return snowflake.connector.connect(
        user=SNOW_USER, password=SNOW_PASSWORD, account=SNOW_ACCOUNT,
        warehouse=SNOW_WH, database=SNOW_DB, schema=SNOW_SCHEMA
    )

def init_snowflake_schema(ctx):
    cursor = ctx.cursor()
    # Explicitly ensure the database context exists for table creation queries
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOW_DB}.{SNOW_SCHEMA};")
    cursor.execute(f"USE SCHEMA {SNOW_DB}.{SNOW_SCHEMA};")
    
    cursor.execute("CREATE TABLE IF NOT EXISTS DIM_CRYPTOCURRENCY (CRYPTO_ID VARCHAR PRIMARY KEY, SYMBOL VARCHAR, NAME VARCHAR);")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS DIM_DATE (
        DATE_KEY INT PRIMARY KEY, FULL_DATE DATE, CALENDAR_YEAR INT, 
        MONTH_NUMBER INT, MONTH_NAME VARCHAR, WEEK_NUMBER INT, DAY_NAME VARCHAR
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS FACT_MARKET_DAILY (
        FACT_KEY INT IDENTITY(1,1) PRIMARY KEY,
        CRYPTO_ID VARCHAR FOREIGN KEY REFERENCES DIM_CRYPTOCURRENCY(CRYPTO_ID),
        DATE_KEY INT FOREIGN KEY REFERENCES DIM_DATE(DATE_KEY),
        CURRENT_PRICE FLOAT, MARKET_CAP FLOAT, MARKET_CAP_RANK INT, TOTAL_VOLUME FLOAT,
        HIGH_24H FLOAT, LOW_24H FLOAT, PRICE_CHANGE_24H FLOAT, PRICE_CHANGE_PCT FLOAT, COLLECTED_AT TIMESTAMP_TZ
    );""")
    cursor.close()

def read_gold_parquet(s3_client, table_name, date_prefix):
    res = s3_client.get_object(Bucket=GOLD_BUCKET, Key=f"{date_prefix}/{table_name}.parquet")
    return pd.read_parquet(io.BytesIO(res['Body'].read()))

def load_dataframe_to_snowflake(ctx, df: pd.DataFrame, target_table: str):
    if df.empty:
        return

    cursor = ctx.cursor()
    cursor.execute(f"USE SCHEMA {SNOW_DB}.{SNOW_SCHEMA};")
    
    # Normalize input column maps to completely avoid snake_case vs UPPER_CASE mismatches
    df.columns = df.columns.str.lower()
    
    # Idempotency Restrictions using case-safe lowercase checking
    if target_table == "DIM_DATE":
        cursor.execute(f"SELECT DATE_KEY FROM {SNOW_DB}.{SNOW_SCHEMA}.DIM_DATE")
        existing = {int(row[0]) for row in cursor.fetchall()}
        df = df[~df['date_key'].astype(int).isin(existing)]

    elif target_table == "DIM_CRYPTOCURRENCY":
        cursor.execute(f"SELECT CRYPTO_ID FROM {SNOW_DB}.{SNOW_SCHEMA}.DIM_CRYPTOCURRENCY")
        existing = {str(row[0]) for row in cursor.fetchall()}
        df = df[~df['crypto_id'].astype(str).isin(existing)]

    elif target_table == "FACT_MARKET_DAILY":
        dates = tuple(df['date_key'].astype(int).unique())
        cond = f"= {dates[0]}" if len(dates) == 1 else f"IN {dates}"
        cursor.execute(f"DELETE FROM {SNOW_DB}.{SNOW_SCHEMA}.FACT_MARKET_DAILY WHERE DATE_KEY {cond}")
        ctx.commit()

    if df.empty:
        logger.info(f"✅ {target_table} is up-to-date. Skipping.")
        cursor.close()
        return

    # Convert DataFrame back to UPPERCASE right before write_pandas maps to Snowflake tables
    df.columns = df.columns.str.upper()

    # Bulk Write (With index warning fix included)
    logger.info(f"Loading {len(df)} rows to {target_table}...")
    success, _, nrows, _ = write_pandas(
        conn=ctx, 
        df=df.reset_index(drop=True),  # <-- This is the change that cleans the warning!
        table_name=target_table, 
        database=SNOW_DB, 
        schema=SNOW_SCHEMA, 
        use_logical_type=True
    )
    cursor.close()
    if success:
        logger.info(f"✅ Committed {nrows} rows to {target_table}.")

if __name__ == "__main__":
    try:
        s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
        partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        
        df_crypto = read_gold_parquet(s3, "Dim_Cryptocurrency", partition)
        df_date = read_gold_parquet(s3, "Dim_Date", partition)
        df_fact = read_gold_parquet(s3, "Fact_Market_Daily", partition)
        
        ctx = get_snowflake_connection()
        init_snowflake_schema(ctx)
        
        load_dataframe_to_snowflake(ctx, df_crypto, "DIM_CRYPTOCURRENCY")
        load_dataframe_to_snowflake(ctx, df_date, "DIM_DATE")
        load_dataframe_to_snowflake(ctx, df_fact, "FACT_MARKET_DAILY")
        logger.info("🎉 Snowflake load completed successfully!")
    except Exception as e:
        logger.error(f"❌ Pipeline Failed: {e}")
        exit(1)
    finally:
        if 'ctx' in locals() and ctx:
            ctx.close()