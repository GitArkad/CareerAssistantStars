"""
job_parser_dag.py

Primary Airflow DAG for the jobs pipeline.

Single DAG, two parser modes:

1. query-based sources
   Examples: HH, Adzuna, USAJobs, Arbeitnow, Ashby
   These parsers are expected to search/fetch using their own query-driven logic.

2. catalog-based sources
   Examples: Greenhouse, Lever, Himalayas
   These parsers are expected to load a source catalog / board dataset once per run
   and then filter internally, instead of re-crawling the source for every query.

Pipeline:
    [parse_* in parallel] -> collect_keys -> clean -> load -> aggregate -> embed

Important:
- collect_keys waits for ALL parse tasks to finish (success/failed/skipped)
- pipeline continues as long as at least one parser produced data
- parser-specific logic lives inside src.parsers.pars
"""

from __future__ import annotations

import importlib
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------
# Parser config
# mode:
#   - "query"   -> parser uses query-driven fetching internally
#   - "catalog" -> parser loads source catalog once per run internally
# ---------------------------------------------------------------------
PARSERS = [
    {"task_id": "parse_hh",         "cls": "HHParser",         "mode": "query",   "timeout_h": 1.0},
    {"task_id": "parse_adzuna",     "cls": "AdzunaParser",     "mode": "query",   "timeout_h": 1.5},
    {"task_id": "parse_usajobs",    "cls": "USAJobsParser",    "mode": "query",   "timeout_h": 0.5},
    {"task_id": "parse_arbeitnow",  "cls": "ArbeitnowParser",  "mode": "query",   "timeout_h": 0.5},
    {"task_id": "parse_ashby",      "cls": "AshbyParser",      "mode": "query",   "timeout_h": 0.5},

    {"task_id": "parse_greenhouse", "cls": "GreenhouseParser", "mode": "catalog", "timeout_h": 1.0},
    {"task_id": "parse_lever",      "cls": "LeverParser",      "mode": "catalog", "timeout_h": 2.5},
    {"task_id": "parse_himalayas",  "cls": "HimalayasParser",  "mode": "catalog", "timeout_h": 1.0},
]


def _build_parser(cls_name: str):
    """Instantiate parser class with required credentials if needed."""
    pars_module = importlib.import_module("src.parsers.pars")
    cls = getattr(pars_module, cls_name)

    if cls_name == "AdzunaParser":
        app_id = os.getenv("ADZUNA_APP_ID")
        app_key = os.getenv("ADZUNA_APP_KEY")
        if not app_id or not app_key:
            return None
        return cls(app_id, app_key)

    if cls_name == "USAJobsParser":
        api_key = os.getenv("USAJOBS_API_KEY")
        email = os.getenv("USAJOBS_EMAIL")
        if not api_key or not email:
            return None
        return cls(api_key, email)

    return cls()


def _run_single_parser(cls_name: str, mode: str, **context) -> None:
    """
    Run a single parser and upload raw CSV to S3.

    mode is informational for DAG readability and future extension.
    Actual parser behavior is implemented in src.parsers.pars:
      - query parsers should run query-first logic internally
      - catalog parsers should run catalog-first logic internally
    """
    from src.loaders.s3_storage import ensure_bucket, raw_key, upload_df

    ti = context["ti"]
    date_str = context["logical_date"].strftime("%Y-%m-%d")
    run_id = context["run_id"]

    ensure_bucket()

    parser = _build_parser(cls_name)
    if parser is None:
        ti.xcom_push(key="raw_s3_key", value=None)
        return

    # The parser itself decides how to execute:
    # - query-based parsers use query-driven run()
    # - catalog-based parsers load and filter catalog internally
    parser.run()
    df = parser.to_df()

    if df.empty:
        ti.xcom_push(key="raw_s3_key", value=None)
        return

    key = raw_key(parser.source_name, date_str=date_str, run_id=run_id)
    upload_df(df, key)
    ti.xcom_push(key="raw_s3_key", value=key)


def task_collect_keys(**context) -> None:
    """
    Wait for all parser tasks to finish and gather available raw S3 keys.

    Continues the pipeline if at least one parser produced data.
    """
    ti = context["ti"]
    date_str = context["logical_date"].strftime("%Y-%m-%d")

    raw_keys: list[str] = []
    for parser_cfg in PARSERS:
        key = ti.xcom_pull(task_ids=parser_cfg["task_id"], key="raw_s3_key")
        if key:
            raw_keys.append(key)

    if not raw_keys:
        raise ValueError(f"No parsers produced data for {date_str}")

    ti.xcom_push(key="date_str", value=date_str)
    ti.xcom_push(key="raw_s3_keys", value=raw_keys)


def task_clean(**context) -> None:
    from src.cleaners.data_cleaner import run_clean_step

    ti = context["ti"]
    date_str = ti.xcom_pull(task_ids="collect_keys", key="date_str")
    raw_s3_keys = ti.xcom_pull(task_ids="collect_keys", key="raw_s3_keys") or []

    clean_s3_key = run_clean_step(date_str=date_str, raw_s3_keys=raw_s3_keys)
    if not clean_s3_key:
        raise ValueError(f"Cleaning step produced no S3 key for {date_str}")

    ti.xcom_push(key="clean_s3_key", value=clean_s3_key)


def task_load(**context) -> None:
    from src.loaders.load_to_db import run_load_step

    ti = context["ti"]
    date_str = ti.xcom_pull(task_ids="collect_keys", key="date_str")
    raw_s3_keys = ti.xcom_pull(task_ids="collect_keys", key="raw_s3_keys") or []
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

    date_str = context["ti"].xcom_pull(task_ids="collect_keys", key="date_str")
    summary = run_embedding_step(date_str=date_str)
    context["ti"].xcom_push(key="embed_summary", value=summary)


with DAG(
    dag_id="jobs_pipeline_weekly",
    description="Weekly jobs pipeline: parallel parse -> collect -> clean -> load -> aggregate -> embed",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "etl", "s3", "postgres", "qdrant"],
) as dag:

    parse_tasks = []
    for parser_cfg in PARSERS:
        task = PythonOperator(
            task_id=parser_cfg["task_id"],
            python_callable=_run_single_parser,
            op_kwargs={
                "cls_name": parser_cfg["cls"],
                "mode": parser_cfg["mode"],
            },
            execution_timeout=timedelta(hours=parser_cfg["timeout_h"]),
            retries=1,
            retry_delay=timedelta(minutes=3),
        )
        parse_tasks.append(task)
  
    collect = PythonOperator(
        task_id="collect_keys",
        python_callable=task_collect_keys,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
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

    parse_tasks >> collect >> clean >> load >> aggregate >> embed