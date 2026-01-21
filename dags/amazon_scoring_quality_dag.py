from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.data_quality import check_data_quality
from scripts.scoring_metrics import calculate_scoring_metrics

with DAG(
    dag_id="amazon_scoring_quality_metrics",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality
    )

    scoring = PythonOperator(
        task_id="scoring_metrics",
        python_callable=calculate_scoring_metrics
    )

    quality >> scoring
