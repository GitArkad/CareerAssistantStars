from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Добавляем путь к проекту
PROJECT_PATH = os.getenv("PROJECT_PATH", "/opt/airflow/project")
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

# Общие параметры DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Конфиг парсеров
PARSERS = [
    {"task_id": "parse_hh",         "cls": "HHParser",         "mode": "query",   "timeout_h": 1.0},
    {"task_id": "parse_adzuna",     "cls": "AdzunaParser",     "mode": "query",   "timeout_h": 1.5},
    {"task_id": "parse_usajobs",    "cls": "USAJobsParser",    "mode": "query",   "timeout_h": 1.5},
    {"task_id": "parse_arbeitnow",  "cls": "ArbeitnowParser",  "mode": "catalog",   "timeout_h": 0.5},
    {"task_id": "parse_ashby",      "cls": "AshbyParser",      "mode": "query",   "timeout_h": 0.5},

    {"task_id": "parse_greenhouse", "cls": "GreenhouseParser", "mode": "catalog", "timeout_h": 1.0},
    {"task_id": "parse_lever",      "cls": "LeverParser",      "mode": "catalog", "timeout_h": 2.5},
    {"task_id": "parse_himalayas",  "cls": "HimalayasParser",  "mode": "catalog", "timeout_h": 1.0},
]

# Создаёт экземпляр парсера
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

# Возвращает дату запуска в формате YYYY-MM-DD
def _resolve_date_str(context) -> str:
    logical_date = context.get("logical_date")

    if logical_date is None:
        dag_run = context.get("dag_run")
        logical_date = getattr(dag_run, "logical_date", None)

    if logical_date is None:
        ti = context.get("ti")
        logical_date = getattr(ti, "logical_date", None)

    if logical_date is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return logical_date.strftime("%Y-%m-%d")

# Запускает один парсер и сохраняет raw CSV в S3
def _run_single_parser(cls_name: str, mode: str, **context) -> None:
    from src.loaders.s3_storage import ensure_bucket, raw_key, upload_df

    ti = context["ti"]
    date_str = _resolve_date_str(context)
    run_id = context.get("run_id") or getattr(ti, "run_id", "manual")

    ensure_bucket()

    parser = _build_parser(cls_name)
    if parser is None:
        ti.xcom_push(key="raw_s3_key", value=None)
        return

    parser.run()
    df = parser.to_df()

    if df.empty:
        ti.xcom_push(key="raw_s3_key", value=None)
        return

    key = raw_key(parser.source_name, date_str=date_str, run_id=run_id)
    upload_df(df, key)
    ti.xcom_push(key="raw_s3_key", value=key)

# Собирает ключи raw-файлов из всех parser tasks
def task_collect_keys(**context) -> None:
    ti = context["ti"]
    date_str = _resolve_date_str(context)

    raw_keys: list[str] = []
    for parser_cfg in PARSERS:
        key = ti.xcom_pull(task_ids=parser_cfg["task_id"], key="raw_s3_key")
        if key:
            raw_keys.append(key)

    if not raw_keys:
        raise ValueError(f"No parsers produced data for {date_str}")

    ti.xcom_push(key="date_str", value=date_str)
    ti.xcom_push(key="raw_s3_keys", value=raw_keys)

# Запускает этап очистки данных
def task_clean(**context) -> None:
    from src.cleaners.data_cleaner import run_clean_step

    ti = context["ti"]
    date_str = ti.xcom_pull(task_ids="collect_keys", key="date_str")
    raw_s3_keys = ti.xcom_pull(task_ids="collect_keys", key="raw_s3_keys") or []

    clean_s3_key = run_clean_step(date_str=date_str, raw_s3_keys=raw_s3_keys)
    if not clean_s3_key:
        raise ValueError(f"Cleaning step produced no S3 key for {date_str}")

    ti.xcom_push(key="clean_s3_key", value=clean_s3_key)

# Загружает очищенные данные в БД
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

# Обновляет слой job_skills для аналитики
def task_refresh_job_skills(**context) -> None:
    from src.aggregators.aggregate import run_refresh_job_skills_step

    summary = run_refresh_job_skills_step()
    context["ti"].xcom_push(key="job_skills_summary", value=summary)

# Пересчитывает v2-аналитику для Streamlit
def task_aggregate_v2(**context) -> None:
    from src.aggregators.aggregate import run_aggregate_v2_step

    summary = run_aggregate_v2_step()
    context["ti"].xcom_push(key="aggregate_v2_summary", value=summary)

# Пересчитывает старые агрегаты
def task_aggregate(**context) -> None:
    from src.aggregators.aggregate import run_aggregate_step

    summary = run_aggregate_step()
    context["ti"].xcom_push(key="aggregate_summary", value=summary)

# Отправляет pending вакансии в embeddings
def task_embed(**context) -> None:
    from src.loaders.qdrant_service import run_embedding_step
    summary = run_embedding_step(date_str=None)
    context["ti"].xcom_push(key="embed_summary", value=summary)

with DAG(
    dag_id="jobs_pipeline_weekly",
    description="Jobs pipeline every 2 days: parallel parse -> collect -> clean -> load -> job_skills -> aggregate_v2 -> aggregate -> embed",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule=timedelta(days=2),
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

    refresh_job_skills = PythonOperator(
        task_id="refresh_job_skills",
        python_callable=task_refresh_job_skills,
        execution_timeout=timedelta(minutes=20),
    )

    aggregate_v2 = PythonOperator(
        task_id="aggregate_v2",
        python_callable=task_aggregate_v2,
        execution_timeout=timedelta(minutes=30),
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

    parse_tasks >> collect >> clean >> load >> refresh_job_skills >> aggregate_v2 >> aggregate >> embed