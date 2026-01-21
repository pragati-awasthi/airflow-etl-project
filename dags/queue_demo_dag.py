from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="queue_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = BashOperator(    # defining a task using BashOperator 
        task_id="important_task",     #
        bash_command="echo 'Hello from High Priority Queue'",
        queue="high_priority"
    )
