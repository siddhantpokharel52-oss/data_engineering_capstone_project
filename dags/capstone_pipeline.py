"""Master DAG orchestrating the full ETL pipeline.

Runs daily. Extracts CSVs, loads to Bronze, cleans to Silver,
then transforms to Gold (Star Schema + SCD Type 2).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


from tasks.extract import extract_data
from tasks.load_bronze import load_bronze
from tasks.transform_silver import transform_silver
from tasks.transform_gold import transform_gold
from tasks.success_report import success_report

default_args = {
    "owner": "siddhant",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["pkhrl.siddhant@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="capstone_etl_pipeline",
    default_args=default_args,
    description="End-to-End ETL: CSV → Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "medallion"],
) as dag:

    extract = PythonOperator(
        task_id="extract_csv_data",
        python_callable=extract_data,
    )
    bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_bronze,
    )
    silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_silver,
    )
    gold = PythonOperator(
        task_id="transform_to_gold",
        python_callable=transform_gold,
    )
    report = PythonOperator(
        task_id="send_daily_summary_report",
        python_callable=success_report,
    )

    extract >> bronze >> silver >> gold >> report
