import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Modular Processing Function Imports
from src.pipelines.bronze_layer import run_bronze_ingestion
from src.pipelines.silver_layer import run_silver_transformation
from src.pipelines.gold_layer import run_gold_modeling
from src.pipelines.load_snowflake import run_snowflake_load

def on_failure_alert(context):
    """Triggers automatically if a task completely exhausts its retry attempts."""
    task_id = context.get('task_instance').task_id
    logical_date = context.get('logical_date')
    error = context.get('exception')
    logging.error(f"❌ CRITICAL: Task '{task_id}' failed for logical date {logical_date}.")
    logging.error(f"Reason for failure: {error}")

default_args = {
    'owner': 'ayoub',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure_alert,
}

with DAG(
    dag_id='cryptopipelinedag',
    default_args=default_args,
    description='Automated daily Medallion pipeline (MinIO Data Lake to Snowflake Warehouse)',
    schedule='15 0 * * *', 
    catchup=False,
) as dag:

    task_ingest_bronze = PythonOperator(
        task_id='ingestbronze', 
        python_callable=run_bronze_ingestion
    )

    task_transform_silver = PythonOperator(
        task_id='transformsilver', 
        python_callable=run_silver_transformation
    )

    task_build_gold_model = PythonOperator(
        task_id='buildgoldmodel', 
        python_callable=run_gold_modeling
    )

    task_load_snowflake = PythonOperator(
        task_id='load_snowflake', 
        python_callable=run_snowflake_load
    )

    # Medallion Architecture Sequence Chain
    task_ingest_bronze >> task_transform_silver >> task_build_gold_model >> task_load_snowflake