from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta
import pandas as pd 
default_args = {
    'owner': 'tran_hien',
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}
# Define DAG details
with DAG(
    dag_id='a_test_dag',
    start_date=datetime(2024, 4, 19),  # Adjust as needed
    default_args = default_args
) as dag:
    clean_up_csv = BashOperator(
    task_id= 'clean_up_csv_files',
    bash_command = 'rm /opt/airflow/data/raw_data/*.csv; rm /opt/airflow/data/cleaned_data/*.csv',
    dag = dag
    )
    clean_up_csv

