"""
exchange_rates_dag.py

Daily DAG for currency updates (ECB + CBR).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_PATH = os.getenv("PROJECT_PATH", "/opt/airflow/project")
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
}


def task_update_rates(**context) -> None:
    from src.loaders.exchange_rates import run_update_rates

    date_str = context["logical_date"].strftime("%Y-%m-%d")
    result = run_update_rates(rate_date=date_str)
    context["ti"].xcom_push(key="exchange_rates_summary", value=result)


with DAG(
    dag_id="exchange_rates_daily",
    description="Daily exchange-rate update for salary normalization",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule="15 17 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["fx", "currency", "daily"],
) as dag:
    update_rates = PythonOperator(
        task_id="update_exchange_rates",
        python_callable=task_update_rates,
        execution_timeout=timedelta(minutes=35),
    )