import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "crypto-silver")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")

def interactive_viewer():
    print("=== MinIO Medallion Architecture Interactive Viewer ===")
    print("1: Bronze Layer (Raw JSON)")
    print("2: Silver Layer (Clean Parquet)")
    print("3: Gold Layer   (Analytical Parquet)")
    
    choice = input("Select data layer [1/2/3]: ").strip()
    if choice == '1':
        bucket, file_name, file_type = BRONZE_BUCKET, "raw.json", "json"
    elif choice == '2':
        bucket, file_name, file_type = SILVER_BUCKET, "clean.parquet", "parquet"
    elif choice == '3':
        bucket, file_name, file_type = GOLD_BUCKET, "gold.parquet", "parquet"
    else:
        print("❌ Invalid selection. Exiting.")
        return

    raw_date = input("Enter partition date (e.g., 2026-06-09 or 2026/06/09): ").strip()
    partition = raw_date.replace("-", "/") 
    object_key = f"{partition}/{file_name}"

    try:
        s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
        print(f"\n🔄 Fetching: s3://{bucket}/{object_key} ...")
        
        response = s3.get_object(Bucket=bucket, Key=object_key)
        buffer = io.BytesIO(response['Body'].read())
        
        df = pd.read_json(buffer) if file_type == "json" else pd.read_parquet(buffer, engine='pyarrow')
        
        print(f"\n🎉 File loaded! Dimensions: {df.shape[0]} rows x {df.shape[1]} columns.\n")
        print("--- Schema Information ---")
        print(df.info())
        print("\n--- Top 5 Rows ---")
        print(df.head())

    except Exception as e:
        print(f"❌ Failed to locate or read target partition block: {e}")

if __name__ == "__main__":
    interactive_viewer()