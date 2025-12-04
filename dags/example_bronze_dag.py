"""Minimal example DAG skeleton that wires to function-only modules.

This file is example-only and keeps the orchestration separate from logic implemented in `src/`.
"""
from datetime import datetime

# Keep DAG small and readable in real Composer environments.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from src.extract.mssql_to_parquet import extract_table_to_parquet

    with DAG(dag_id="example_bronze_extract", start_date=datetime(2025, 1, 1), schedule_interval="@daily", catchup=False) as dag:
        def _extract_task():
            # orchestration-level wrapper; actual extraction logic is in src.extract
            extract_table_to_parquet("placeholder-conn", "customers", "/tmp/extract/example")

        extract_op = PythonOperator(task_id="extract_customers", python_callable=_extract_task)
        extract_op
except Exception:
    # Make sure import works in environments that do not have Airflow installed.
    dag = None
