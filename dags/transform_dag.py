# dags/transform_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

from scripts.etl_transform_amazon import (
    transform_orders,
    transform_customer_metrics,
    partition_by_date,
    summarize_transform_stats
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1)
}

with DAG(
    dag_id="amazon_transform_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "transform", "amazon"]
) as dag:

    # Wait for ETL DAG to finish
    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_etl_pipeline",
        external_dag_id="etl_pipeline_dag",
        external_task_id="validation_task",
        poke_interval=60,
        timeout=600,
        mode="poke"
    )

    transform_orders_task = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders,
        provide_context=True
    )

    transform_customer_task = PythonOperator(
        task_id="transform_customer_metrics",
        python_callable=transform_customer_metrics,
        provide_context=True
    )

    partition_task = PythonOperator(
        task_id="partition_by_date",
        python_callable=partition_by_date,
        provide_context=True
    )

    summarize_task = PythonOperator(
        task_id="summarize_transform_stats",
        python_callable=summarize_transform_stats,
        provide_context=True
    )

    # DAG Flow
    wait_for_etl >> [transform_orders_task, transform_customer_task]
    [transform_orders_task, transform_customer_task] >> partition_task
    partition_task >> summarize_task
