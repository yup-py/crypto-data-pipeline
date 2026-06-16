import os
import json
import logging
from datetime import datetime, timezone
import requests
from botocore.exceptions import ClientError
from src.config.minio_config import BRONZE_BUCKET
from src.utils.minio_client import get_s3_client

# CoinGecko API Configuration
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.environ.get("COINGECKO_API_KEY")

def setup_logging():
    """Configures streaming handlers for native Airflow console tracking."""
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

# Runtime Guard Rings
if not API_KEY:
    logger.critical("COINGECKO_API_KEY is missing from environment variables or .env file.")
    raise ValueError("CRITICAL: COINGECKO_API_KEY is missing from environment configuration.")

if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
    logger.critical("Infrastructure automation credentials missing from the environment.")
    raise ValueError("CRITICAL: Storage layer validation credentials missing.")

def ensure_bucket_exists(s3_client):
    """Verifies existence of the target storage bucket or creates it if missing."""
    try:
        s3_client.head_bucket(Bucket=BRONZE_BUCKET)
        logger.info(f"Target validation passed: Bucket '{BRONZE_BUCKET}' is active.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.warning(f"Bucket '{BRONZE_BUCKET}' does not exist. Initializing provisioning sequence...")
            s3_client.create_bucket(Bucket=BRONZE_BUCKET)
            logger.info(f"Successfully provisioned infrastructure storage bucket: {BRONZE_BUCKET}")
        else:
            logger.error(f"Failed to validate bucket existence with MinIO server code: {error_code}")
            raise

def fetch_market_data():
    """Fetches real-time cryptocurrency market metrics from CoinGecko API."""
    params = {"vs_currency": "usd", "per_page": 250, "page": 1}
    headers = {"accept": "application/json", "x-cg-demo-api-key": API_KEY}
    
    try:
        logger.info("Initiating network request handshake with CoinGecko API endpoints...")
        response = requests.get(COINGECKO_API_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status() 
        logger.info("Payload response code 200 acknowledged. Successfully downloaded market metrics metadata.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            logger.critical("Outbound API connection blocked: Rate-limited by CoinGecko server nodes (HTTP 429).")
        else:
            logger.error(f"REST API layer validation exception encountered: {http_err}")
        raise
    except requests.exceptions.Timeout:
        logger.critical("Outbound network infrastructure failed: Request dropped at 10-second timeout.")
        raise
    except requests.exceptions.RequestException as err:
        logger.error(f"Low-level protocol interface connectivity failure: {err}")
        raise

def upload_to_minio(s3_client, json_data):
    """Streams the raw JSON data payload into the structured MinIO folder tree path."""
    now = datetime.now(timezone.utc)
    object_key = f"{now.strftime('%Y/%m/%d')}/raw.json"
    json_bytes = json.dumps(json_data, indent=4).encode('utf-8')
    
    logger.info(f"Preparing object streaming transmission payload targeting: s3://{BRONZE_BUCKET}/{object_key}")
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=object_key,
        Body=json_bytes,
        ContentType='application/json'
    )
    logger.info("Object transport serialization acknowledged. Storage transaction finalized.")

if __name__ == "__main__":
    logger.info("--- Beginning Automated Daily Bronze Data Ingestion Run ---")
    try:
        s3 = get_s3_client()
        ensure_bucket_exists(s3)
        raw_payload = fetch_market_data()
        upload_to_minio(s3, raw_payload)
        logger.info("🎉 Étape 1: Bronze ingestion execution runtime sequence finalized successfully.\n")
    except Exception as e:
        logger.critical(f"❌ Pipeline sequence terminated unexpectedly: {e}\n")
        exit(1)