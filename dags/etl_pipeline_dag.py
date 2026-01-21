from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd

# Import extract function
from scripts.extract_data import extract_data


# ---------------- FAILURE CALLBACK ----------------
def task_failure_alert(context):
    """
    Called automatically when any task fails.
    """
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    print(f"[FAILURE ALERT] Task '{task_id}' failed in DAG '{dag_id}'")


# ---------------- CLEANING TASK ----------------
def clean_data(ti):
    """
    Cleans CSV data by removing null values.
    Pushes cleaned record count to XCom.
    """
    file_path = "/opt/airflow/data/Amazon.csv"
    df = pd.read_csv(file_path)

    df = df.dropna()
    cleaned_count = len(df)

    ti.xcom_push(key="cleaned_count", value=cleaned_count)
    print(f"Cleaned records: {cleaned_count}")


# ---------------- BRANCH DECISION ----------------
def decide_branch(ti):
    """
    Branch based on cleaned record count.
    """
    cleaned_count = ti.xcom_pull(key="cleaned_count")
    return "validate_data" if cleaned_count > 1 else "skip_validation"


# ---------------- VALIDATION TASK ----------------
def validate_data(ti):
    """
    Validates cleaned data.
    """
    cleaned_count = ti.xcom_pull(key="cleaned_count")

    if cleaned_count == 0:
        raise AirflowSkipException("No data to validate")

    print("Validation successful!")


# ---------------- DAG DEFINITION ----------------
with DAG(
    dag_id="etl_csv_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,

    # ⭐ FAILURE CALLBACK ADDED HERE ⭐
    on_failure_callback=task_failure_alert,

    tags=["internship", "etl", "csv"]
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'ETL Pipeline Started'"
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    branch_task = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=decide_branch
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    skip_validation = BashOperator(
        task_id="skip_validation",
        bash_command="echo 'Validation skipped due to insufficient data'"
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'ETL Pipeline Completed'"
    )

    # DAG FLOW
    start >> extract_task >> clean_task >> branch_task
    branch_task >> [validate_task, skip_validation] >> end
