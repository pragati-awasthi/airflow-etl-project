from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import ETL functions
from scripts.etl_extract_sample import extract_data
from scripts.etl_transform_sample import transform_data
from scripts.etl_load_sample import load_data

with DAG(
    dag_id='etl_sample_dag',
    start_date=datetime(2025, 12, 22),
    schedule_interval=None,
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        provide_context=True
    )

    task_transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True
    )

    task_load = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        provide_context=True
    )

    # DAG flow
    task_extract >> task_transform >> task_load
