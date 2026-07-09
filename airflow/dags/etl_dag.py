from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
import sys
from airflow.operators.python import PythonOperator


sys.path.append('/opt/airflow/dags')

from scripts.transformation.unify_datasets import unify_dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='unify_datasets',
    description='Unify all datasets in one',
    start_date=datetime(2026, 7, 5),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:
    
    unify_datasets = PythonOperator(
        task_id='execute_hello_world',
        python_callable=unify_dataset,
        op_kwargs={
            'azure_conn_id':'AzureDataLake',
            'source_container': 'bronze',
            'source_dirs': ['datasets/'],
            'target_container': 'bronze',
            'target_dir': 'all_in_one/'
        }
    )

    unify_datasets