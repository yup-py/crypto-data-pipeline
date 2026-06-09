import os
import json
import logging
from datetime import datetime, timezone
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# CoinGecko API Configuration
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.environ.get("COINGECKO_API_KEY")

# MinIO Connection Configuration (Sourced via implicit AWS environment vars)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
BUCKET_NAME = os.environ.get("MINIO_BRONZE_BUCKET", "crypto-bronze")

# Setup Daily Logging Infrastructure
def setup_logging():
    """Creates a daily log file and configures handlers for file and console tracking."""
    os.makedirs("logs", exist_ok=True)
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_filename = f"logs/{current_date}_ingest.log"
    
    # Define a clean, standardized log formatting pattern
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Root Logger Setup
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if script re-runs in the same interactive session
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # Handler 1: Write entries to local log file
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    
    # Handler 2: Stream entries to PowerShell console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

# Runtime Guard Rings (Strict implicit validation check)
if not API_KEY:
    logger.critical("COINGECKO_API_KEY is missing from environment variables or .env file.")
    raise ValueError("CRITICAL: COINGECKO_API_KEY is missing from environment configuration.")

if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
    logger.critical("Infrastructure automation credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) are missing from the environment.")
    raise ValueError("CRITICAL: Storage layer validation credentials missing.")

def get_s3_client():
    """Initializes the boto3 S3 client targeted at MinIO, leveraging native env credentials."""
    logger.info(f"Initializing connection wrapper to MinIO endpoint: {MINIO_ENDPOINT}")
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        region_name='us-east-1'  # Standard region stub required by boto3
    )

def ensure_bucket_exists(s3_client):
    """Verifies existence of the target storage bucket or creates it if missing."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Target validation passed: Bucket '{BUCKET_NAME}' is active.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.warning(f"Bucket '{BUCKET_NAME}' does not exist. Initializing provisioning sequence...")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            logger.info(f"Successfully provisioned infrastructure storage bucket: {BUCKET_NAME}")
        else:
            logger.error(f"Failed to validate bucket existence with MinIO server code: {error_code}")
            raise

def fetch_market_data():
    """Fetches real-time cryptocurrency market metrics from CoinGecko API."""
    params = {
        "vs_currency": "usd",
        "per_page": 250,
        "page": 1
    }
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": API_KEY
    }
    
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
        logger.critical("Outbound network infrastructure failed: Request connection dropped at 10-second timeout.")
        raise
    except requests.exceptions.RequestException as err:
        logger.error(f"Low-level protocol interface connectivity failure: {err}")
        raise

def upload_to_minio(s3_client, json_data):
    """Streams the raw JSON data payload into the structured MinIO folder tree path."""
    now = datetime.now(timezone.utc)
    object_key = f"{now.strftime('%Y/%m/%d')}/raw.json"
    
    json_bytes = json.dumps(json_data, indent=4).encode('utf-8')
    
    logger.info(f"Preparing object streaming transmission payload targeting: s3://{BUCKET_NAME}/{object_key}")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
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