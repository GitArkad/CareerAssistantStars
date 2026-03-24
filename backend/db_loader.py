"""
db_loader.py

Loader for PostgreSQL that works with the current schema:
- jobs_raw
- jobs_curated
- etl_runs

Main use cases:
1) load_raw_jobs(...)        -> inserts raw parsed payloads into jobs_raw
2) upsert_curated_jobs(...)  -> inserts/updates normalized jobs into jobs_curated
3) start_etl_run(...) / finish_etl_run(...) -> logs pipeline runs in etl_runs

Environment variables:
- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_HOST
- POSTGRES_PORT
- POSTGRES_DB
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Optional, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values


# =========================================================
# Public result models
# =========================================================

@dataclass
class RawLoadResult:
    total_received: int
    inserted: int
    duplicates_or_updated: int
    failed: int


@dataclass
class CuratedLoadResult:
    total_received: int
    inserted: int
    updated: int
    failed: int


# =========================================================
# Column definitions
# =========================================================

RAW_COLUMNS: list[str] = [
    "source",
    "source_job_id",
    "url",
    "search_query",
    "payload",
    "fetched_at",
    "parsed_at",
    "processing_status",
    "processing_error",
    "content_hash",
]

CURATED_COLUMNS: list[str] = [
    "job_id",
    "raw_job_id",
    "source",
    "source_job_id",
    "url",
    "title",
    "title_normalized",
    "description",
    "requirements",
    "responsibilities",
    "nice_to_have",
    "salary_from",
    "salary_to",
    "currency",
    "salary_period",
    "experience_level",
    "seniority_normalized",
    "years_experience_min",
    "years_experience_max",
    "company_name",
    "industry",
    "company_size",
    "key_skills",
    "skills_extracted",
    "skills_normalized",
    "tech_stack_tags",
    "tools",
    "methodologies",
    "location",
    "country",
    "region",
    "city",
    "remote",
    "remote_type",
    "employment_type",
    "search_query",
    "published_at",
    "parsed_at",
    "embedding_status",
]


# =========================================================
# Connection
# =========================================================

def get_connection():
    """Create a PostgreSQL connection using environment variables."""
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")

    missing = [
        name for name, value in {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": port,
            "POSTGRES_DB": db,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(
            f"Missing required database environment variables: {', '.join(missing)}"
        )

    return psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=db,
    )


# =========================================================
# ETL run logging
# =========================================================

def start_etl_run(
    pipeline_name: str = "jobs_pipeline",
    dag_id: Optional[str] = None,
    source: Optional[str] = None,
    run_date: Optional[date] = None,
) -> int:
    """
    Insert a new ETL run into etl_runs and return its ID.
    """
    sql = """
        INSERT INTO etl_runs (
            pipeline_name,
            dag_id,
            run_date,
            source,
            status,
            started_at
        )
        VALUES (%s, %s, %s, %s, 'running', NOW())
        RETURNING id;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                pipeline_name,
                dag_id,
                run_date or date.today(),
                source,
            ))
            run_id = cur.fetchone()[0]
        conn.commit()

    return run_id


def finish_etl_run(
    run_id: int,
    *,
    status: str,
    jobs_extracted: int = 0,
    jobs_new_raw: int = 0,
    jobs_processed_raw: int = 0,
    jobs_curated_inserted: int = 0,
    jobs_curated_updated: int = 0,
    jobs_duplicates: int = 0,
    embeddings_created: int = 0,
    aggregates_updated: bool = False,
    error_message: Optional[str] = None,
) -> None:
    """
    Finish an ETL run and write summary counters.
    """
    if status not in {"success", "failed"}:
        raise ValueError("status must be 'success' or 'failed'")

    sql = """
        UPDATE etl_runs
        SET status = %s,
            jobs_extracted = %s,
            jobs_new_raw = %s,
            jobs_processed_raw = %s,
            jobs_curated_inserted = %s,
            jobs_curated_updated = %s,
            jobs_duplicates = %s,
            embeddings_created = %s,
            aggregates_updated = %s,
            error_message = %s,
            finished_at = NOW(),
            duration_sec = EXTRACT(EPOCH FROM (NOW() - started_at))::INT
        WHERE id = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    status,
                    jobs_extracted,
                    jobs_new_raw,
                    jobs_processed_raw,
                    jobs_curated_inserted,
                    jobs_curated_updated,
                    jobs_duplicates,
                    embeddings_created,
                    aggregates_updated,
                    error_message,
                    run_id,
                ),
            )
        conn.commit()


# =========================================================
# Normalization helpers
# =========================================================

def _to_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "None"):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        # Accept ISO formats, including trailing Z
        value = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "remote"}:
            return True
        if normalized in {"false", "0", "no", "n", "onsite", "on-site"}:
            return False
    return default


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _to_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        # If string looks like JSON array, try to parse it first
        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [
                        str(item).strip()
                        for item in parsed
                        if str(item).strip()
                    ]
            except json.JSONDecodeError:
                pass
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _normalize_raw_record(record: dict[str, Any]) -> dict[str, Any]:
    source = _to_text(record.get("source"))
    if not source:
        raise ValueError("raw record is missing required field: source")

    payload = record.get("payload")
    if payload is None:
        # If payload is not explicitly passed, keep the original record as payload.
        payload = dict(record)

    normalized = {
        "source": source,
        "source_job_id": _to_text(record.get("source_job_id")),
        "url": _to_text(record.get("url")),
        "search_query": _to_text(record.get("search_query")),
        "payload": Json(payload),
        "fetched_at": _to_datetime(record.get("fetched_at")) or datetime.utcnow(),
        "parsed_at": _to_datetime(record.get("parsed_at")),
        "processing_status": _to_text(record.get("processing_status")) or "new",
        "processing_error": _to_text(record.get("processing_error")),
        "content_hash": _to_text(record.get("content_hash")),
    }
    return normalized


def _normalize_curated_record(record: dict[str, Any]) -> dict[str, Any]:
    job_id = _to_text(record.get("job_id"))
    source = _to_text(record.get("source"))

    if not job_id:
        raise ValueError("curated record is missing required field: job_id")
    if not source:
        raise ValueError("curated record is missing required field: source")

    remote_type = _to_text(record.get("remote_type"))
    remote_value = record.get("remote")
    if remote_value is None and remote_type:
        remote = remote_type.lower() in {"remote", "hybrid"}
    else:
        remote = _to_bool(remote_value, default=False)

    normalized = {
        "job_id": job_id,
        "raw_job_id": _to_int(record.get("raw_job_id")),
        "source": source,
        "source_job_id": _to_text(record.get("source_job_id")),
        "url": _to_text(record.get("url")),
        "title": _to_text(record.get("title")),
        "title_normalized": _to_text(record.get("title_normalized")),
        "description": _to_text(record.get("description")),
        "requirements": _to_text(record.get("requirements")),
        "responsibilities": _to_text(record.get("responsibilities")),
        "nice_to_have": _to_text(record.get("nice_to_have")),
        "salary_from": _to_int(record.get("salary_from")),
        "salary_to": _to_int(record.get("salary_to")),
        "currency": _to_text(record.get("currency")),
        "salary_period": _to_text(record.get("salary_period")),
        "experience_level": _to_text(record.get("experience_level")),
        "seniority_normalized": _to_text(record.get("seniority_normalized")),
        "years_experience_min": _to_int(record.get("years_experience_min")),
        "years_experience_max": _to_int(record.get("years_experience_max")),
        "company_name": _to_text(record.get("company_name")),
        "industry": _to_text(record.get("industry")),
        "company_size": _to_text(record.get("company_size")),
        "key_skills": _to_text_list(record.get("key_skills")),
        "skills_extracted": _to_text_list(record.get("skills_extracted")),
        "skills_normalized": _to_text_list(record.get("skills_normalized")),
        "tech_stack_tags": _to_text_list(record.get("tech_stack_tags")),
        "tools": _to_text_list(record.get("tools")),
        "methodologies": _to_text_list(record.get("methodologies")),
        "location": _to_text(record.get("location")),
        "country": _to_text(record.get("country")),
        "region": _to_text(record.get("region")),
        "city": _to_text(record.get("city")),
        "remote": remote,
        "remote_type": remote_type,
        "employment_type": _to_text(record.get("employment_type")),
        "search_query": _to_text(record.get("search_query")),
        "published_at": _to_datetime(record.get("published_at")),
        "parsed_at": _to_datetime(record.get("parsed_at")),
        "embedding_status": _to_text(record.get("embedding_status")) or "pending",
    }

    return normalized


def _values_from_record(
    record: dict[str, Any],
    columns: Sequence[str],
) -> tuple[Any, ...]:
    return tuple(record.get(col) for col in columns)


# =========================================================
# Helpers for counting inserted vs updated
# =========================================================

def _get_existing_job_ids(job_ids: Sequence[str]) -> set[str]:
    if not job_ids:
        return set()

    sql = "SELECT job_id FROM jobs_curated WHERE job_id = ANY(%s);"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (list(job_ids),))
            return {row[0] for row in cur.fetchall()}


def _get_existing_raw_keys(
    keys: Sequence[tuple[str, Optional[str], Optional[str]]]
) -> set[tuple[str, Optional[str], Optional[str]]]:
    """
    Check which raw records already exist by:
    - (source, source_job_id) when source_job_id is present
    - (source, url) when url is present

    This is only approximate counting for summary stats.
    Actual conflict handling is done by PostgreSQL.
    """
    if not keys:
        return set()

    existing: set[tuple[str, Optional[str], Optional[str]]] = set()

    source_job_pairs = [(s, sj) for s, sj, _ in keys if sj]
    source_url_pairs = [(s, u) for s, _, u in keys if u]

    with get_connection() as conn:
        with conn.cursor() as cur:
            if source_job_pairs:
                cur.execute(
                    """
                    SELECT source, source_job_id
                    FROM jobs_raw
                    WHERE (source, source_job_id) IN %s
                    """,
                    (tuple(source_job_pairs),),
                )
                for source, source_job_id in cur.fetchall():
                    existing.add((source, source_job_id, None))

            if source_url_pairs:
                cur.execute(
                    """
                    SELECT source, url
                    FROM jobs_raw
                    WHERE (source, url) IN %s
                    """,
                    (tuple(source_url_pairs),),
                )
                for source, url in cur.fetchall():
                    existing.add((source, None, url))

    return existing


# =========================================================
# Raw loader
# =========================================================

def load_raw_jobs(
    records: Iterable[dict[str, Any]],
    *,
    update_existing: bool = True,
) -> RawLoadResult:
    """
    Insert raw jobs into jobs_raw.

    Conflict strategy:
    - if source_job_id exists -> conflict on (source, source_job_id)
    - else if url exists      -> fallback insert row-by-row with ON CONFLICT (source, url)
    - else                    -> plain insert (no dedupe key)

    Because PostgreSQL cannot use two different partial unique indexes
    in one single ON CONFLICT clause, this loader splits records.
    """
    normalized_records: list[dict[str, Any]] = []
    failed = 0

    for record in records:
        try:
            normalized_records.append(_normalize_raw_record(record))
        except Exception:
            failed += 1

    if not normalized_records:
        return RawLoadResult(
            total_received=0,
            inserted=0,
            duplicates_or_updated=0,
            failed=failed,
        )

    with_source_job_id = [r for r in normalized_records if r["source_job_id"]]
    with_url_only = [
        r for r in normalized_records if not r["source_job_id"] and r["url"]
    ]
    without_keys = [
        r for r in normalized_records if not r["source_job_id"] and not r["url"]
    ]

    duplicates_or_updated = 0
    inserted = 0

    # Approximate pre-count for reporting
    existing_source_job = _get_existing_raw_keys(
        [(r["source"], r["source_job_id"], None) for r in with_source_job_id]
    )
    existing_url = _get_existing_raw_keys(
        [(r["source"], None, r["url"]) for r in with_url_only]
    )
    duplicates_or_updated += len(existing_source_job) + len(existing_url)

    with get_connection() as conn:
        with conn.cursor() as cur:
            if with_source_job_id:
                values = [
                    _values_from_record(r, RAW_COLUMNS)
                    for r in with_source_job_id
                ]
                sql = f"""
                    INSERT INTO jobs_raw ({", ".join(RAW_COLUMNS)})
                    VALUES %s
                    ON CONFLICT (source, source_job_id)
                    DO UPDATE SET
                        url = EXCLUDED.url,
                        search_query = EXCLUDED.search_query,
                        payload = EXCLUDED.payload,
                        fetched_at = EXCLUDED.fetched_at,
                        parsed_at = EXCLUDED.parsed_at,
                        processing_status = EXCLUDED.processing_status,
                        processing_error = EXCLUDED.processing_error,
                        content_hash = EXCLUDED.content_hash
                """
                if not update_existing:
                    sql = f"""
                        INSERT INTO jobs_raw ({", ".join(RAW_COLUMNS)})
                        VALUES %s
                        ON CONFLICT (source, source_job_id) DO NOTHING
                    """
                execute_values(cur, sql, values, page_size=500)
                inserted += len(with_source_job_id)

            if with_url_only:
                values = [
                    _values_from_record(r, RAW_COLUMNS)
                    for r in with_url_only
                ]
                sql = f"""
                    INSERT INTO jobs_raw ({", ".join(RAW_COLUMNS)})
                    VALUES %s
                    ON CONFLICT (source, url)
                    DO UPDATE SET
                        search_query = EXCLUDED.search_query,
                        payload = EXCLUDED.payload,
                        fetched_at = EXCLUDED.fetched_at,
                        parsed_at = EXCLUDED.parsed_at,
                        processing_status = EXCLUDED.processing_status,
                        processing_error = EXCLUDED.processing_error,
                        content_hash = EXCLUDED.content_hash
                """
                if not update_existing:
                    sql = f"""
                        INSERT INTO jobs_raw ({", ".join(RAW_COLUMNS)})
                        VALUES %s
                        ON CONFLICT (source, url) DO NOTHING
                    """
                execute_values(cur, sql, values, page_size=500)
                inserted += len(with_url_only)

            if without_keys:
                values = [
                    _values_from_record(r, RAW_COLUMNS)
                    for r in without_keys
                ]
                sql = f"""
                    INSERT INTO jobs_raw ({", ".join(RAW_COLUMNS)})
                    VALUES %s
                """
                execute_values(cur, sql, values, page_size=500)
                inserted += len(without_keys)

        conn.commit()

    # These counts are approximate for records that may already exist,
    # but they are good enough for ETL summaries.
    inserted = max(0, len(normalized_records) - duplicates_or_updated)
    return RawLoadResult(
        total_received=len(normalized_records),
        inserted=inserted,
        duplicates_or_updated=duplicates_or_updated,
        failed=failed,
    )


# =========================================================
# Curated loader
# =========================================================

def upsert_curated_jobs(
    records: Iterable[dict[str, Any]],
) -> CuratedLoadResult:
    """
    Upsert normalized jobs into jobs_curated using job_id as the primary key.
    """
    normalized_records: list[dict[str, Any]] = []
    failed = 0

    for record in records:
        try:
            normalized_records.append(_normalize_curated_record(record))
        except Exception:
            failed += 1

    if not normalized_records:
        return CuratedLoadResult(
            total_received=0,
            inserted=0,
            updated=0,
            failed=failed,
        )

    job_ids = [r["job_id"] for r in normalized_records]
    existing_job_ids = _get_existing_job_ids(job_ids)

    inserted = sum(1 for job_id in job_ids if job_id not in existing_job_ids)
    updated = sum(1 for job_id in job_ids if job_id in existing_job_ids)

    values = [_values_from_record(r, CURATED_COLUMNS) for r in normalized_records]

    update_columns = [col for col in CURATED_COLUMNS if col != "job_id"]
    update_clause = ",\n                        ".join(
        f"{col} = EXCLUDED.{col}" for col in update_columns
    )

    sql = f"""
        INSERT INTO jobs_curated ({", ".join(CURATED_COLUMNS)})
        VALUES %s
        ON CONFLICT (job_id)
        DO UPDATE SET
                        {update_clause},
                        updated_at = NOW()
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
        conn.commit()

    return CuratedLoadResult(
        total_received=len(normalized_records),
        inserted=inserted,
        updated=updated,
        failed=failed,
    )


# =========================================================
# Convenience wrapper
# =========================================================

def run_db_load(
    *,
    raw_records: Optional[Iterable[dict[str, Any]]] = None,
    curated_records: Optional[Iterable[dict[str, Any]]] = None,
    pipeline_name: str = "jobs_pipeline",
    dag_id: Optional[str] = None,
    source: Optional[str] = None,
    aggregates_updated: bool = False,
    embeddings_created: int = 0,
    manage_etl_run: bool = True,
) -> dict[str, Any]:
    """
    Convenience wrapper to load raw and/or curated data and update etl_runs.

    Returns a summary dictionary.
    """
    run_id: Optional[int] = None
    raw_result = RawLoadResult(0, 0, 0, 0)
    curated_result = CuratedLoadResult(0, 0, 0, 0)

    try:
        if manage_etl_run:
            run_id = start_etl_run(
                pipeline_name=pipeline_name,
                dag_id=dag_id,
                source=source,
            )

        if raw_records is not None:
            raw_result = load_raw_jobs(raw_records)

        if curated_records is not None:
            curated_result = upsert_curated_jobs(curated_records)

        summary = {
            "run_id": run_id,
            "raw_total_received": raw_result.total_received,
            "raw_inserted": raw_result.inserted,
            "raw_duplicates_or_updated": raw_result.duplicates_or_updated,
            "raw_failed": raw_result.failed,
            "curated_total_received": curated_result.total_received,
            "curated_inserted": curated_result.inserted,
            "curated_updated": curated_result.updated,
            "curated_failed": curated_result.failed,
            "status": "success",
        }

        if manage_etl_run and run_id is not None:
            finish_etl_run(
                run_id,
                status="success",
                jobs_extracted=raw_result.total_received,
                jobs_new_raw=raw_result.inserted,
                jobs_processed_raw=raw_result.total_received - raw_result.failed,
                jobs_curated_inserted=curated_result.inserted,
                jobs_curated_updated=curated_result.updated,
                jobs_duplicates=raw_result.duplicates_or_updated,
                embeddings_created=embeddings_created,
                aggregates_updated=aggregates_updated,
            )

        return summary

    except Exception as exc:
        if manage_etl_run and run_id is not None:
            finish_etl_run(
                run_id,
                status="failed",
                jobs_extracted=raw_result.total_received,
                jobs_new_raw=raw_result.inserted,
                jobs_processed_raw=raw_result.total_received - raw_result.failed,
                jobs_curated_inserted=curated_result.inserted,
                jobs_curated_updated=curated_result.updated,
                jobs_duplicates=raw_result.duplicates_or_updated,
                embeddings_created=embeddings_created,
                aggregates_updated=aggregates_updated,
                error_message=str(exc)[:5000],
            )
        raise


__all__ = [
    "RawLoadResult",
    "CuratedLoadResult",
    "get_connection",
    "start_etl_run",
    "finish_etl_run",
    "load_raw_jobs",
    "upsert_curated_jobs",
    "run_db_load",
]