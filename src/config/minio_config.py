import os
from dotenv import load_dotenv

# Automatically lookup custom parameters from an optional local .env file
load_dotenv()

# Centralized Connection Endpoints
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

# Centralized Management Definitions of our Medallion Buckets
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "crypto-silver")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")