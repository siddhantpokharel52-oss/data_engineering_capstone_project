# dags/test_snowflake.py
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2026, 3, 12),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_query = SnowflakeOperator(
        task_id="check_connection",
        sql="SELECT CURRENT_DATE;",
        snowflake_conn_id="snowflake_default",
    )
