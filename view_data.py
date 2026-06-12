import os
import io
import pandas as pd
import boto3
from dotenv import load_dotenv

# Locate and load the environment configurations
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(script_dir) if os.path.basename(script_dir) == "scripts" else script_dir
load_dotenv(os.path.join(root_dir, ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "crypto-silver")
GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "crypto-gold")

def display_dataframe_info(title, df):
    """Helper function to print a clean layout of loaded DataFrames."""
    print("\n" + "="*50)
    print(f"🌟 TABLE: {title}")
    print("="*50)
    print(f"Dimensions: {df.shape[0]} rows x {df.shape[1]} columns.\n")
    print("--- Top 5 Rows ---")
    print(df.head(5))
    print("\n--- Schema Summary ---")
    df.info()

def fetch_parquet_from_minio(s3_client, bucket, key):
    """Downloads a parquet file object directly from MinIO into a Pandas DataFrame."""
    res = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(res['Body'].read()), engine='pyarrow')

def main():
    print("=== MinIO Medallion Architecture Interactive Viewer ===")
    print("1: Bronze Layer (Raw JSON)")
    print("2: Silver Layer (Clean Parquet)")
    print("3: Gold Layer   (Star Schema Dimensions & Facts)")
    
    layer = input("Select data layer [1/2/3]: ").strip()
    raw_date = input("Enter partition date (e.g., 2026-06-12): ").strip()
    partition_date = raw_date.replace("-", "/")
    
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, region_name='us-east-1')
    
    try:
        if layer == "1":
            key = f"{partition_date}/raw.json"
            print(f"\n🔄 Fetching: s3://{BRONZE_BUCKET}/{key} ...")
            res = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
            print("🎉 Raw JSON downloaded successfully. Top characters:")
            print(res['Body'].read().decode('utf-8')[:500])
            
        elif layer == "2":
            key = f"{partition_date}/clean.parquet"
            print(f"\n🔄 Fetching: s3://{SILVER_BUCKET}/{key} ...")
            df = fetch_parquet_from_minio(s3, SILVER_BUCKET, key)
            display_dataframe_info("Silver Clean Data", df)
            
        elif layer == "3":
            print("\n--- Gold Layer Inspection Options ---")
            print("1: Dim_Cryptocurrency")
            print("2: Dim_Date")
            print("3: Fact_Market_Daily")
            print("4: View All Tables Simultaneously")
            
            gold_choice = input("Select Gold option [1/2/3/4]: ").strip()
            
            table_mapping = {
                "1": ["Dim_Cryptocurrency"],
                "2": ["Dim_Date"],
                "3": ["Fact_Market_Daily"],
                "4": ["Dim_Cryptocurrency", "Dim_Date", "Fact_Market_Daily"]
            }
            
            targets = table_mapping.get(gold_choice)
            if not targets:
                print("❌ Invalid table selection.")
                return
                
            # Iterate and display based on selection matrix
            for table_name in targets:
                key = f"{partition_date}/{table_name}.parquet"
                print(f"\n🔄 Fetching: s3://{GOLD_BUCKET}/{key} ...")
                df = fetch_parquet_from_minio(s3, GOLD_BUCKET, key)
                display_dataframe_info(table_name, df)
                
        else:
            print("❌ Invalid layer choice selection.")
            
    except s3.exceptions.NoSuchKey:
        print(f"\n❌ Storage Error: The requested target path key does not exist inside MinIO.")
        print(f"   Please double-check your partition date or run your build script first.")
    except Exception as e:
        print(f"❌ Execution failure: {e}")

if __name__ == "__main__":
    main()