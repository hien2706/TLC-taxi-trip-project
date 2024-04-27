from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta
import pandas as pd 
from python_file.extract import extract_zone_geometry
from python_file.transform import transform_location_dim,transform_datetime_dim, \
                                  transform_base_dim,transform_trip_fact

#url
zone_geometry_url = "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.json?accessType=DOWNLOAD"
taxi_zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
trip_data_url = "https://raw.githubusercontent.com/hien2706/TLC-data/main/trips_data_2019_5000.csv"
base_dim_raw_url = "https://raw.githubusercontent.com/hien2706/TLC-data/main/hvfhs_base.csv"

#raw data path
taxi_zone_lookup_path = '/opt/airflow/data/raw_data/taxi_zone_lookup.csv'
zone_geometry_path = '/opt/airflow/data/raw_data/zone_geometry.csv'
trip_data_path = '/opt/airflow/data/raw_data/trips_data_2019_5000.csv'
base_dim_raw_path = '/opt/airflow/data/raw_data/hvfhs_base.csv'

#cleaned data path
location_dim_path = '/opt/airflow/data/cleaned_data/location_dim.csv'
datetime_dim_path = '/opt/airflow/data/cleaned_data/datetime_dim.csv'
base_dim_cleaned_path = '/opt/airflow/data/cleaned_data/base_dim.csv'
trip_fact_path = '/opt/airflow/data/cleaned_data/trip_fact.csv'

default_args = {
    'owner': 'tran_hien',
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='abc_my_dag',
    start_date=datetime(2024, 4, 8),
    schedule_interval=None,
    default_args = default_args
) as dag:
    
    extract_taxi_zone_lookup_task = BashOperator(
        task_id = 'extract_taxi_zone_lookup',
        bash_command = f'curl -o {taxi_zone_lookup_path} {taxi_zone_lookup_url}',
        dag = dag
    )
    
    extract_zone_geometry_task  = PythonOperator(
        task_id = 'extract_zone_geometry',
        python_callable = extract_zone_geometry,
        op_kwargs = {'url': zone_geometry_url,'path_to_save':zone_geometry_path},
        dag = dag
    )
    
    extract_trip_data_task = BashOperator(
        task_id = 'extract_trip_data',
        bash_command = f'curl -o {trip_data_path} {trip_data_url}',
        dag = dag
    )
    
    extract_base_dim_task = BashOperator(
        task_id = 'extract_base_dim',
        bash_command = f'curl -o {base_dim_raw_path} {base_dim_raw_url}',
        dag = dag
    )
    

    check_taxi_zone_lookup = FileSensor(
        task_id='check_taxi_zone_lookup_existence',
        filepath=taxi_zone_lookup_path,
        fs_conn_id='my_conn',
        dag =dag
    )
    check_zone_geometry = FileSensor(
        task_id = 'check_zone_geometry_existence',
        filepath = zone_geometry_path,
        fs_conn_id='my_conn',
        dag = dag
    )
    check_trip_data = FileSensor(
        task_id = 'check_trip_data_existence',
        filepath = trip_data_path,
        fs_conn_id='my_conn',
        dag = dag
    )
    check_base_dim = FileSensor(
        task_id = 'check_base_dim_existence',
        filepath = base_dim_raw_path,
        fs_conn_id='my_conn',
        dag = dag
    )
    

    transform_location_dim = PythonOperator(
        task_id = 'transform_location_dim',
        python_callable = transform_location_dim,
        op_kwargs = {'taxi_zone_lookup_path':taxi_zone_lookup_path,
                    'zone_geometry_path':  zone_geometry_path,
                    'location_dim_path': location_dim_path},
        dag = dag
    )

    transform_datetime_dim = PythonOperator(
        task_id ='transform_datetime_dim',
        python_callable = transform_datetime_dim,
        op_kwargs = {'trip_data_path':trip_data_path,
                    'datetime_dim_path': datetime_dim_path},
        dag = dag
    )


    transform_base_dim = PythonOperator(
        task_id = 'transform_base_dim',
        python_callable = transform_base_dim,
        op_kwargs = {'base_dim_raw_path': base_dim_raw_path,
                    'base_dim_cleaned_path' : base_dim_cleaned_path},
        dag = dag
    )
    transform_trip_fact = PythonOperator(
        task_id = 'transform_trip_fact',
        python_callable = transform_trip_fact,
        op_kwargs = {'trip_data_path' : trip_data_path,
                    'trip_fact_path' : trip_fact_path,
                    'base_dim_cleaned_path' : base_dim_cleaned_path,
                    'datetime_dim_path' : datetime_dim_path},
        dag = dag
    )
    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_conn',
        sql = "sql_file/create_table.sql",
        dag = dag
    )
    
    load_into_db = PostgresOperator(
        task_id='load_into_postgresql',
        postgres_conn_id='postgres_conn',
        sql = f"""COPY location_dim FROM '/data/cleaned_data/location_dim.csv' DELIMITER ',' CSV HEADER; \n
                COPY datetime_dim FROM '/data/cleaned_data/datetime_dim.csv' DELIMITER ',' CSV HEADER; \n
                COPY base_dim FROM '/data/cleaned_data/base_dim.csv' DELIMITER ',' CSV HEADER; \n
                COPY trip_fact FROM '/data/cleaned_data/trip_fact.csv' DELIMITER ',' CSV HEADER;""",
        dag = dag
    )
    
    clean_up_csv = BashOperator(
        task_id= 'clean_up_csv_files',
        bash_command = 'rm /opt/airflow/data/raw_data/*.csv; rm /opt/airflow/data/cleaned_data/*.csv',
        dag = dag
    )
     

    extract_taxi_zone_lookup_task >>  check_taxi_zone_lookup
    extract_zone_geometry_task >> check_zone_geometry
    extract_trip_data_task >> check_trip_data
    extract_base_dim_task >> check_base_dim

    
    
    check_taxi_zone_lookup >> transform_location_dim
    check_zone_geometry >> transform_location_dim
    check_trip_data >> transform_datetime_dim
    check_base_dim >> transform_base_dim

    transform_location_dim >> transform_trip_fact 
    transform_datetime_dim >> transform_trip_fact
    transform_base_dim >> transform_trip_fact
    
    transform_trip_fact >> create_table >> load_into_db >> clean_up_csv