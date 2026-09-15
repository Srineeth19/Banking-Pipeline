"""
Runs dbt snapshots to capture SCD Type-2 history for customers and accounts,
independent of the main ingestion DAG so history is tracked on a fixed cadence.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="scd_snapshots",
    description="Run dbt snapshots for SCD Type-2 history tracking",
    default_args=default_args,
    schedule_interval="0 * * * *",  # hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "dbt", "scd2"],
) as dag:

    run_snapshots = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir .",
    )
