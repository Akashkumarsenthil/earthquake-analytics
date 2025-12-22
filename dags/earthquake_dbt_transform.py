"""
Earthquake dbt Transformation DAG
Runs dbt models after data ingestion to transform raw data into analytics-ready tables
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os

# Configuration - adjust path based on your Docker setup
DBT_PROJECT_DIR = '/opt/airflow/dbt/earthquake_analytics'  # Adjust this path
DBT_PROFILES_DIR = '/opt/airflow/dbt'  # Where profiles.yml is located

default_args = {
    'owner': 'akash',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='earthquake_dbt_transform',
    default_args=default_args,
    description='Run dbt transformations on earthquake data',
    schedule_interval='*/10 * * * *',  # Every 10 minutes (after ingestion)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['earthquake', 'dbt', 'transformation', 'project']
) as dag:
    
    # Wait for ingestion DAG to complete (optional - can remove if not needed)
    # wait_for_ingestion = ExternalTaskSensor(
    #     task_id='wait_for_ingestion',
    #     external_dag_id='earthquake_realtime_ingestion',
    #     external_task_id='load_to_snowflake',
    #     timeout=600,
    #     mode='poke',
    #     poke_interval=30
    # )
    
    # dbt debug - test connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    # dbt deps - install packages
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    # dbt run staging models
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    # dbt run mart models (incremental)
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    # dbt test - run tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    # dbt docs generate (optional)
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}'
    )
    
    dbt_debug >> dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs
