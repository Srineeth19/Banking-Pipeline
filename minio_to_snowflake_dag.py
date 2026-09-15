"""
Loads newline-delimited JSON CDC files landed in MinIO (Bronze) into
Snowflake raw/bronze tables, then triggers dbt to build staging/mart models.
Uses parallel per-table loads and an extended timeout to comfortably handle
~100k+ record batches.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

TABLES = ["customers", "accounts", "transactions"]

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def load_table_to_snowflake(table: str, **context):
    """
    Copies today's partition of newline-delimited JSON for `table` from the
    MinIO bronze bucket into the matching Snowflake RAW table using an
    external stage + COPY INTO, then logs row counts.
    Snowflake connection is resolved from the `snowflake_default` Airflow
    connection; MinIO/S3 stage credentials come from `minio_default`.
    """
    import snowflake.connector
    from airflow.hooks.base import BaseHook

    conn_info = BaseHook.get_connection("snowflake_default")
    ctx = snowflake.connector.connect(
        user=conn_info.login,
        password=conn_info.password,
        account=conn_info.extra_dejson.get("account"),
        warehouse=conn_info.extra_dejson.get("warehouse", "BANKING_WH"),
        database=conn_info.extra_dejson.get("database", "BANKING_DB"),
        schema="BRONZE",
    )
    ds = context["ds"]
    try:
        with ctx.cursor() as cur:
            cur.execute(f"""
                COPY INTO bronze.{table}_raw
                FROM @banking_bronze_stage/{table}/dt={ds}/
                FILE_FORMAT = (TYPE = JSON)
                ON_ERROR = 'CONTINUE'
            """)
            result = cur.fetchall()
            print(f"COPY INTO bronze.{table}_raw result: {result}")
    finally:
        ctx.close()


with DAG(
    dag_id="minio_to_snowflake",
    description="Load Bronze CDC data from MinIO into Snowflake, then run dbt",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "cdc", "snowflake"],
) as dag:

    load_tasks = [
        PythonOperator(
            task_id=f"load_{table}_to_snowflake",
            python_callable=load_table_to_snowflake,
            op_kwargs={"table": table},
        )
        for table in TABLES
    ]

    run_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select staging --profiles-dir .",
    )

    run_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir .",
    )

    run_dbt_tests = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/banking_dbt && dbt test --profiles-dir .",
    )

    load_tasks >> run_dbt_staging >> run_dbt_marts >> run_dbt_tests
