from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
import sys
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow/dags')


from scripts.transformation.unify_datasets import unify_dataset
from scripts.transformation.clean_dataset import cleaning



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ETL_pipeline',
    description='ETL pipeline',
    start_date=datetime(2026, 7, 5),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:
    
    # Merge datasets in one
    unify_datasets = PythonOperator(
        task_id='merge_datasets',
        python_callable=unify_dataset,
        op_kwargs={
            'azure_conn_id':'AzureDataLake',
            'source_container': 'bronze',
            'source_dirs': ['datasets/'],
            'target_container': 'bronze',
            'target_dir': 'all_in_one/'
        }
    )

    # Cleaning 
    cleaning_dataset = PythonOperator(
        task_id="clean_dataset",
        python_callable=cleaning
    )

    unify_datasets >> cleaning_dataset