from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import logging
import os
from dotenv import load_dotenv

# loading .env
dotenv_path = '/opt/airflow/.env'
load_dotenv(dotenv_path)

# Add etl .py
sys.path.append("/opt/airflow/billboard_spotify_etl")

# Importign ETL Functions
from transform import transform_data
from load import load_to_bigquery


# Args
default_args = {
    'owner': 'Jeremy',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 26)
}

# Creating DAG
dag = DAG(
    'billboard_spotify_etl',
    default_args=default_args,
    description='Billboard and Spotify ETL Pipeline',
    schedule_interval='0 0 * * 1',  # Run monday at midnight
    catchup=False
)

# Creating etl func
def etl_process():
    """Wrapper function for the ETL process"""
    logging.info("Starting ETL process...")
    
    # Transform data
    logging.info("Starting data transformation...")
    df = transform_data()

    if df is not None and not df.empty:
        logging.info(f"Transformation complete. DataFrame shape: {df.shape}")

        # Load to BigQuery
        logging.info("Starting BigQuery load...")
        if load_to_bigquery(df):
            logging.info("ETL process completed successfully")
            return True
        else:
            raise Exception("Failed to load data to BigQuery")
    else:
        raise Exception("Transform returned None or empty DataFrame")

# Create the task
etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=etl_process,
    dag=dag
)

# Set task dependencies (in this case, we only have one task)
etl_task
