import os
import json
from datetime import datetime, timezone
import requests
from src.config.minio_config import BRONZE_BUCKET
from src.utils.minio_client import get_s3_client
from src.utils.pipeline_utils import get_pipeline_logger, ensure_bucket_exists

# Configuration Endpoints
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Initialize logger safely at module level
logger = get_pipeline_logger(__name__)


def validate_environment():
    """Validates that all required environment variables are present at runtime."""
    api_key = os.environ.get("COINGECKO_API_KEY")
    if not api_key:
        logger.critical("COINGECKO_API_KEY is missing from environment variables or .env file.")
        raise ValueError("CRITICAL: COINGECKO_API_KEY is missing from environment configuration.")

    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        logger.critical("Infrastructure automation credentials missing from the environment.")
        raise ValueError("CRITICAL: Storage layer validation credentials missing.")
        
    return api_key


def fetch_market_data(api_key):
    """Fetches real-time cryptocurrency market metrics from CoinGecko API using a validated key."""
    params = {"vs_currency": "usd", "per_page": 250, "page": 1}
    headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}
    
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
        Bucket=BRONZE_BUCKET, Key=object_key, Body=json_bytes, ContentType='application/json'
    )
    logger.info("Object transport serialization acknowledged. Storage transaction finalized.")


def run_bronze_ingestion(**kwargs):
    """Entry point for Airflow PythonOperator."""
    logger.info("--- Starting Bronze Data Ingestion ---")
    
    # Run environment guard checks strictly inside the active execution context
    api_key = validate_environment()
    
    s3 = get_s3_client()
    ensure_bucket_exists(s3, BRONZE_BUCKET)
    
    raw_payload = fetch_market_data(api_key)
    upload_to_minio(s3, raw_payload)
    logger.info("Bronze ingestion finished.")


if __name__ == "__main__":
    logger.info("--- Beginning Automated Daily Bronze Data Ingestion Run ---")
    try:
        run_bronze_ingestion()
        logger.info("🎉 Étape 1: Bronze ingestion execution runtime sequence finalized successfully.\n")
    except Exception as e:
        logger.critical(f"❌ Pipeline sequence terminated unexpectedly: {e}\n")
        exit(1)