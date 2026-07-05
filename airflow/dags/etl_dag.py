from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='wizard_bronze_to_silver_v2',
    description='Simple Hello World test for DAG-script connection',
    start_date=datetime(2026, 7, 5),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:
    
    run_hello_world = BashOperator(
        task_id='execute_hello_world',
        bash_command='echo "================ HELLO WORLD ================"'
    )

    run_hello_world