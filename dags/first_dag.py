from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="first_airflow_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Airflow is working successfully'"
    )

    start_task
