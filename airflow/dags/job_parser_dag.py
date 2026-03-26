"""
job_parser_dag.py

Primary Airflow DAG for the jobs pipeline.

Active flow:
    parse -> clean -> load -> aggregate -> embed

Notes:
- Parsing is assumed to be the stable/approved implementation in pars.py
- Intermediate datasets are exchanged through S3/MinIO, not local files
- raw_s3_keys from parse are passed into load so file-level manifest and job audit
  can be linked to the correct raw files
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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def task_parse(**context) -> None:
    from src.parsers.pars import run_parse_step

    date_str = context["ds"]
    result = run_parse_step(date_str=date_str) or []
    context["ti"].xcom_push(key="date_str", value=date_str)
    context["ti"].xcom_push(key="raw_s3_keys", value=result)


def task_clean(**context) -> None:
    from src.cleaners.data_cleaner import run_clean_step

    ti = context["ti"]
    date_str = ti.xcom_pull(task_ids="parse", key="date_str")
    raw_s3_keys = ti.xcom_pull(task_ids="parse", key="raw_s3_keys") or []

    clean_s3_key = run_clean_step(
        date_str=date_str,
        raw_s3_keys=raw_s3_keys,
    )
    if not clean_s3_key:
        raise ValueError(f"Cleaning step produced no S3 key for {date_str}")
    ti.xcom_push(key="clean_s3_key", value=clean_s3_key)


def task_load(**context) -> None:
    from src.loaders.load_to_db import run_load_step

    ti = context["ti"]
    date_str = ti.xcom_pull(task_ids="parse", key="date_str")
    raw_s3_keys = ti.xcom_pull(task_ids="parse", key="raw_s3_keys") or []
    clean_s3_key = ti.xcom_pull(task_ids="clean", key="clean_s3_key")

    summary = run_load_step(
        date_str=date_str,
        clean_s3_key=clean_s3_key,
        raw_s3_keys=raw_s3_keys,
    )
    ti.xcom_push(key="load_summary", value=summary)



def task_aggregate(**context) -> None:
    from src.aggregators.aggregate import run_aggregate_step

    summary = run_aggregate_step()
    context["ti"].xcom_push(key="aggregate_summary", value=summary)



def task_embed(**context) -> None:
    from src.loaders.qdrant_service import run_embedding_step

    date_str = context["ti"].xcom_pull(task_ids="parse", key="date_str")
    summary = run_embedding_step(date_str=date_str)
    context["ti"].xcom_push(key="embed_summary", value=summary)


with DAG(
    dag_id="jobs_pipeline_weekly",
    description="Weekly jobs pipeline: parse -> clean -> load -> aggregate -> embed",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "etl", "s3", "postgres", "qdrant"],
) as dag:
    parse = PythonOperator(
        task_id="parse",
        python_callable=task_parse,
        execution_timeout=timedelta(hours=3),
    )

    clean = PythonOperator(
        task_id="clean",
        python_callable=task_clean,
        execution_timeout=timedelta(hours=1),
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        execution_timeout=timedelta(hours=1),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=task_aggregate,
        execution_timeout=timedelta(minutes=30),
    )

    embed = PythonOperator(
        task_id="embed",
        python_callable=task_embed,
        execution_timeout=timedelta(hours=2),
    )

    parse >> clean >> load >> aggregate >> embed