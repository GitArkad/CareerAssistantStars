"""
db_loader.py

Загрузчик данных текущего jobs pipeline в PostgreSQL.

Архитектура:
- ingestion_manifest : реестр обработанных файлов (одна строка на один raw-файл)
- job_registry       : текущее состояние / идентичность каждой вакансии
- job_audit          : аудит изменений вакансий по каждому запуску
- jobs_curated       : нормализованные вакансии для приложения, поиска и аналитики
- etl_runs           : журнал запусков пайплайна

Важно:
- сырые payload вакансий хранятся в S3/MinIO, а не в PostgreSQL
- run_db_load() использует ОДНО соединение с БД на весь цикл загрузки
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

# -------------------------------------------------------------------
# Dataclass-структуры для аккуратного возврата итогов загрузки.
# Нужны, чтобы стандартизировать ответ по manifest / curated / audit.
# -------------------------------------------------------------------

@dataclass
class ManifestLoadResult:
    total_received: int
    inserted: int
    updated: int
    failed: int


@dataclass
class CuratedLoadResult:
    total_received: int
    inserted: int
    updated: int
    failed: int


@dataclass
class AuditLoadResult:
    total_received: int
    inserted: int
    failed: int

# -------------------------------------------------------------------
# Явный список колонок для ingestion_manifest.
# Используется для построения INSERT ... VALUES через execute_values.
# Такой подход уменьшает риск рассинхронизации порядка полей.
# ------------------------------------------------------------------

MANIFEST_COLUMNS: list[str] = [
    "run_id",
    "source",
    "raw_s3_key",
    "clean_s3_key",
    "raw_file_hash",
    "raw_row_count",
    "clean_row_count",
    "loaded_row_count",
    "fetched_at",
    "parsed_at",
    "cleaned_at",
    "loaded_at",
    "status",
    "error_message",
    "metadata",
]

# -------------------------------------------------------------------
# Явный список колонок для jobs_curated.
# Порядок должен соответствовать порядку значений в rows при upsert.
# -------------------------------------------------------------------

CURATED_COLUMNS: list[str] = [
    "job_id",
    "run_id",
    "raw_s3_key",
    "clean_s3_key",
    "content_hash",
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
    "department",
    "key_skills",
    "skills_extracted",
    "skills_normalized",
    "tech_stack_tags",
    "tools",
    "methodologies",
    "visa_sponsorship",
    "relocation",
    "benefits",
    "education",
    "certifications",
    "spoken_languages",
    "equity_bonus",
    "security_clearance",
    "role_family",
    "location",
    "country",
    "country_normalized",
    "region",
    "city",
    "remote",
    "remote_type",
    "employment_type",
    "is_data_role",
    "is_ml_role",
    "is_python_role",
    "is_analyst_role",
    "search_query",
    "published_at",
    "parsed_at",
    "embedding_status",
]


def get_connection():
    """
    Создаёт соединение с PostgreSQL из переменных окружения.

    Обязательные env-переменные:
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    - POSTGRES_HOST
    - POSTGRES_PORT
    - POSTGRES_DB

    Если чего-то не хватает, выбрасываем ValueError сразу,
    чтобы ошибка была явной и не маскировалась под generic DB error.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")

    missing = [
        name
        for name, value in {
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


def start_etl_run(
    conn,
    pipeline_name: str = "jobs_pipeline",
    dag_id: Optional[str] = None,
    source: Optional[str] = None,
    run_date: Optional[date] = None,
) -> int:
    """
    Создаёт запись о старте ETL-запуска в etl_runs.

    Возвращает id созданного запуска.
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
    with conn.cursor() as cur:
        cur.execute(sql, (pipeline_name, dag_id, run_date or date.today(), source))
        return cur.fetchone()[0]


def finish_etl_run(
    conn,
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
    Завершает ETL-run и записывает его итоговые метрики.

    status должен быть:
    - success
    - failed
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


def _to_datetime(value: Any) -> Optional[datetime]:
    """
    Приводит значение к datetime, если это возможно.

    Поддерживает:
    - datetime
    - date
    - ISO-строки
    - строки с Z -> заменяются на +00:00

    Если значение пустое или не парсится — возвращает None.
    """
    if value in (None, "", "None"):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        text = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> Optional[int]:
    """
    Приводит значение к int.

    Поддерживает:
    - int
    - float
    - числовые строки

    Для bool вернёт 0/1.
    Для пустых / некорректных значений вернёт None.
    """
    if value in (None, "", "None"):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any, default: bool = False) -> bool:
    """
    Приводит значение к bool.

    Поддерживаются:
    - bool
    - int / float
    - строки yes/no, true/false, 1/0
    - remote/hybrid -> True
    - onsite/on-site -> False

    Если распознать не удалось — возвращаем default.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "remote", "hybrid"}:
            return True
        if normalized in {"false", "0", "no", "n", "onsite", "on-site"}:
            return False
    return default


def _to_text(value: Any) -> Optional[str]:
    """
    Приводит значение к обрезанной строке.
    Пустые строки превращает в None.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float):
        import math
        if math.isnan(value):
            return []
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"nan", "none", "null", "{}"}:
            return []
        if text.startswith("{") and text.endswith("}"):
            inner = text[1:-1].strip()
            if not inner:
                return []
            return [s.strip().strip('"').strip("'") for s in inner.split(",") if s.strip()]
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                pass
        return [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _first_non_null(record: dict[str, Any], *keys: str) -> Any:
    """
    Возвращает первое непустое значение из record по списку ключей.
    """
    for key in keys:
        value = record.get(key)
        if value not in (None, "", "None"):
            return value
    return None


def _normalize_manifest_record(record: dict[str, Any]) -> dict[str, Any]:
    run_id = _to_text(record.get("run_id"))
    source = _to_text(record.get("source"))
    raw_s3_key = _to_text(record.get("raw_s3_key"))

    if not run_id:
        raise ValueError("manifest record is missing required field: run_id")
    if not source:
        raise ValueError("manifest record is missing required field: source")
    if not raw_s3_key:
        raise ValueError("manifest record is missing required field: raw_s3_key")

    metadata = record.get("metadata")
    if metadata is None:
        metadata = {}
    elif not isinstance(metadata, dict):
        metadata = {"value": metadata}

    return {
        "run_id": run_id,
        "source": source,
        "raw_s3_key": raw_s3_key,
        "clean_s3_key": _to_text(record.get("clean_s3_key")),
        "raw_file_hash": _to_text(record.get("raw_file_hash")),
        "raw_row_count": _to_int(record.get("raw_row_count")),
        "clean_row_count": _to_int(record.get("clean_row_count")),
        "loaded_row_count": _to_int(record.get("loaded_row_count")),
        "fetched_at": _to_datetime(record.get("fetched_at")),
        "parsed_at": _to_datetime(record.get("parsed_at")),
        "cleaned_at": _to_datetime(record.get("cleaned_at")),
        "loaded_at": _to_datetime(record.get("loaded_at")),
        "status": _to_text(record.get("status")) or "loaded",
        "error_message": _to_text(record.get("error_message")),
        "metadata": metadata,
    }


def _normalize_curated_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Нормализует одну вакансию для jobs_curated.

    Обязательные поля:
    - job_id
    - source

    Особенности:
    - salary_from может прийти как salary_from или salary_min
    - salary_to   может прийти как salary_to   или salary_max
    - company_name может прийти как company_name или company
    - remote может быть вычислен из remote_type, если remote явно не передан
    - seniority_normalized по умолчанию 'unknown'
    - role_family по умолчанию 'other'
    - embedding_status по умолчанию 'pending'
    """
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

    return {
        "job_id": job_id,
        "run_id": _to_text(record.get("run_id")),
        "raw_s3_key": _to_text(record.get("raw_s3_key")),
        "clean_s3_key": _to_text(record.get("clean_s3_key")),
        "content_hash": _to_text(record.get("content_hash")),
        "source": source,
        "source_job_id": _to_text(record.get("source_job_id")),
        "url": _to_text(record.get("url")),
        "title": _to_text(record.get("title")),
        "title_normalized": _to_text(record.get("title_normalized")),
        "description": _to_text(record.get("description")),
        "requirements": _to_text(record.get("requirements")),
        "responsibilities": _to_text(record.get("responsibilities")),
        "nice_to_have": _to_text(record.get("nice_to_have")),
        "salary_from": _to_int(_first_non_null(record, "salary_from", "salary_min")),
        "salary_to": _to_int(_first_non_null(record, "salary_to", "salary_max")),
        "currency": _to_text(record.get("currency")),
        "salary_period": _to_text(record.get("salary_period")),
        "experience_level": _to_text(record.get("experience_level")),
        "seniority_normalized": _to_text(record.get("seniority_normalized")) or "unknown",
        "years_experience_min": _to_int(record.get("years_experience_min")),
        "years_experience_max": _to_int(record.get("years_experience_max")),
        "company_name": _to_text(_first_non_null(record, "company_name", "company")),
        "industry": _to_text(record.get("industry")),
        "company_size": _to_text(record.get("company_size")),
        "department": _to_text(record.get("department")),
        "key_skills": _to_text_list(record.get("key_skills")),
        "skills_extracted": _to_text_list(record.get("skills_extracted")),
        "skills_normalized": _to_text_list(record.get("skills_normalized")),
        "tech_stack_tags": _to_text_list(record.get("tech_stack_tags")),
        "tools": _to_text_list(record.get("tools")),
        "methodologies": _to_text_list(record.get("methodologies")),
        "visa_sponsorship": _to_bool(record.get("visa_sponsorship"), default=False),
        "relocation": _to_bool(record.get("relocation"), default=False),
        "benefits": _to_text(record.get("benefits")),
        "education": _to_text(record.get("education")),
        "certifications": _to_text(record.get("certifications")),
        "spoken_languages": _to_text_list(record.get("spoken_languages")),
        "equity_bonus": _to_text(record.get("equity_bonus")),
        "security_clearance": _to_text(record.get("security_clearance")),
        "role_family": _to_text(record.get("role_family")) or "other",
        "location": _to_text(record.get("location")),
        "country": _to_text(record.get("country")),
        "country_normalized": _to_text(record.get("country_normalized")),
        "region": _to_text(record.get("region")),
        "city": _to_text(record.get("city")),
        "remote": remote,
        "remote_type": remote_type,
        "employment_type": _to_text(record.get("employment_type")),
        "is_data_role": _to_bool(record.get("is_data_role"), default=False),
        "is_ml_role": _to_bool(record.get("is_ml_role"), default=False),
        "is_python_role": _to_bool(record.get("is_python_role"), default=False),
        "is_analyst_role": _to_bool(record.get("is_analyst_role"), default=False),
        "search_query": _to_text(record.get("search_query")),
        "published_at": _to_datetime(record.get("published_at")),
        "parsed_at": _to_datetime(record.get("parsed_at")),
        "embedding_status": _to_text(record.get("embedding_status")) or "pending",
    }


def _get_existing_manifest_raw_keys(conn, raw_s3_keys: Sequence[str]) -> set[str]:
    if not raw_s3_keys:
        return set()
    sql = "SELECT raw_s3_key FROM ingestion_manifest WHERE raw_s3_key = ANY(%s);"
    with conn.cursor() as cur:
        cur.execute(sql, (list(raw_s3_keys),))
        return {row[0] for row in cur.fetchall()}


def _get_existing_curated_state(conn, job_ids: Sequence[str]) -> dict[str, Optional[str]]:
    if not job_ids:
        return {}
    sql = "SELECT job_id, content_hash FROM jobs_curated WHERE job_id = ANY(%s);"
    with conn.cursor() as cur:
        cur.execute(sql, (list(job_ids),))
        return {row[0]: row[1] for row in cur.fetchall()}


def upsert_manifest_records(conn, manifest_records: Sequence[dict[str, Any]]) -> ManifestLoadResult:
    """
    Возвращает множество raw_s3_key, которые уже есть в ingestion_manifest.

    Нужна для подсчёта inserted vs updated после upsert.
    """
    normalized: list[dict[str, Any]] = []
    failed = 0

    for record in manifest_records:
        try:
            normalized.append(_normalize_manifest_record(record))
        except Exception:
            failed += 1

    if not normalized:
        return ManifestLoadResult(
            total_received=len(manifest_records), inserted=0, updated=0, failed=failed
        )

    existing_keys = _get_existing_manifest_raw_keys(
        conn,
        [record["raw_s3_key"] for record in normalized],
    )

    rows = []
    for record in normalized:
        row = []
        for column in MANIFEST_COLUMNS:
            value = record[column]
            if column == "metadata":
                value = Json(value)
            row.append(value)
        rows.append(tuple(row))

    sql = f"""
        INSERT INTO ingestion_manifest ({', '.join(MANIFEST_COLUMNS)})
        VALUES %s
        ON CONFLICT (raw_s3_key)
        DO UPDATE SET
            run_id = EXCLUDED.run_id,
            source = EXCLUDED.source,
            clean_s3_key = COALESCE(EXCLUDED.clean_s3_key, ingestion_manifest.clean_s3_key),
            raw_file_hash = COALESCE(EXCLUDED.raw_file_hash, ingestion_manifest.raw_file_hash),
            raw_row_count = COALESCE(EXCLUDED.raw_row_count, ingestion_manifest.raw_row_count),
            clean_row_count = COALESCE(EXCLUDED.clean_row_count, ingestion_manifest.clean_row_count),
            loaded_row_count = COALESCE(EXCLUDED.loaded_row_count, ingestion_manifest.loaded_row_count),
            fetched_at = COALESCE(EXCLUDED.fetched_at, ingestion_manifest.fetched_at),
            parsed_at = COALESCE(EXCLUDED.parsed_at, ingestion_manifest.parsed_at),
            cleaned_at = COALESCE(EXCLUDED.cleaned_at, ingestion_manifest.cleaned_at),
            loaded_at = COALESCE(EXCLUDED.loaded_at, ingestion_manifest.loaded_at),
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            metadata = COALESCE(EXCLUDED.metadata, ingestion_manifest.metadata),
            updated_at = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    inserted = sum(1 for record in normalized if record["raw_s3_key"] not in existing_keys)
    updated = len(normalized) - inserted
    return ManifestLoadResult(
        total_received=len(manifest_records), inserted=inserted, updated=updated, failed=failed
    )


def upsert_job_registry(conn, curated_records: Sequence[dict[str, Any]]) -> None:
    if not curated_records:
        return

    rows = []
    for record in curated_records:
        seen_at = record.get("parsed_at") or datetime.utcnow()
        rows.append(
            (
                record["job_id"],
                record["source"],
                record.get("source_job_id"),
                record.get("url"),
                record.get("run_id") or "unknown",
                record.get("run_id") or "unknown",
                seen_at,
                seen_at,
                record.get("raw_s3_key"),
                record.get("clean_s3_key"),
                record.get("content_hash"),
                True,
            )
        )

    sql = """
        INSERT INTO job_registry (
            job_id,
            source,
            source_job_id,
            url,
            first_seen_run_id,
            last_seen_run_id,
            first_seen_at,
            last_seen_at,
            last_raw_s3_key,
            last_clean_s3_key,
            content_hash,
            is_active
        )
        VALUES %s
        ON CONFLICT (job_id)
        DO UPDATE SET
            source = EXCLUDED.source,
            source_job_id = COALESCE(EXCLUDED.source_job_id, job_registry.source_job_id),
            url = COALESCE(EXCLUDED.url, job_registry.url),
            last_seen_run_id = EXCLUDED.last_seen_run_id,
            last_seen_at = EXCLUDED.last_seen_at,
            last_raw_s3_key = COALESCE(EXCLUDED.last_raw_s3_key, job_registry.last_raw_s3_key),
            last_clean_s3_key = COALESCE(EXCLUDED.last_clean_s3_key, job_registry.last_clean_s3_key),
            content_hash = COALESCE(EXCLUDED.content_hash, job_registry.content_hash),
            is_active = TRUE,
            updated_at = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)


def upsert_curated_jobs(conn, curated_records: Sequence[dict[str, Any]]) -> CuratedLoadResult:
    normalized: list[dict[str, Any]] = []
    failed = 0

    for record in curated_records:
        try:
            normalized.append(_normalize_curated_record(record))
        except Exception:
            failed += 1

    if not normalized:
        return CuratedLoadResult(
            total_received=len(curated_records), inserted=0, updated=0, failed=failed
        )

    existing_state = _get_existing_curated_state(conn, [record["job_id"] for record in normalized])

    rows = [tuple(record[column] for column in CURATED_COLUMNS) for record in normalized]
    update_assignments = [
        f"{column} = EXCLUDED.{column}"
        for column in CURATED_COLUMNS
        if column != "job_id"
    ] + ["updated_at = NOW()"]

    sql = f"""
        INSERT INTO jobs_curated ({', '.join(CURATED_COLUMNS)})
        VALUES %s
        ON CONFLICT (job_id)
        DO UPDATE SET
            {', '.join(update_assignments)};
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    inserted = sum(1 for record in normalized if record["job_id"] not in existing_state)
    updated = len(normalized) - inserted
    return CuratedLoadResult(
        total_received=len(curated_records), inserted=inserted, updated=updated, failed=failed
    )


def insert_job_audit_rows(
    conn,
    curated_records: Sequence[dict[str, Any]],
    existing_curated_state: dict[str, Optional[str]],
) -> AuditLoadResult:
    """
    Пишет аудит по вакансиям в job_audit.

    Для каждой записи определяется action:
    - inserted  : вакансии раньше не было
    - unchanged : content_hash не изменился
    - updated   : вакансия была, но content_hash изменился

    ON CONFLICT (run_id, job_id) нужен на случай повторного запуска
    или переигрывания того же run_id.
    """
    if not curated_records:
        return AuditLoadResult(total_received=0, inserted=0, failed=0)

    rows = []
    failed = 0

    for record in curated_records:
        try:
            job_id = record["job_id"]
            old_hash = existing_curated_state.get(job_id)
            new_hash = record.get("content_hash")

            if old_hash is None:
                action = "inserted"
            elif old_hash == new_hash:
                action = "unchanged"
            else:
                action = "updated"

            seen_at = record.get("parsed_at") or datetime.utcnow()
            rows.append(
                (
                    record.get("run_id") or "unknown",
                    job_id,
                    record.get("source"),
                    record.get("source_job_id"),
                    record.get("url"),
                    record.get("title"),
                    record.get("company_name"),
                    record.get("raw_s3_key"),
                    record.get("clean_s3_key"),
                    new_hash,
                    action,
                    "ok",
                    None,
                    seen_at,
                )
            )
        except Exception:
            failed += 1

    if not rows:
        return AuditLoadResult(total_received=len(curated_records), inserted=0, failed=failed)

    sql = """
        INSERT INTO job_audit (
            run_id,
            job_id,
            source,
            source_job_id,
            url,
            title,
            company_name,
            raw_s3_key,
            clean_s3_key,
            content_hash,
            action,
            status,
            message,
            seen_at
        )
        VALUES %s
        ON CONFLICT (run_id, job_id)
        DO UPDATE SET
            source = EXCLUDED.source,
            source_job_id = EXCLUDED.source_job_id,
            url = EXCLUDED.url,
            title = EXCLUDED.title,
            company_name = EXCLUDED.company_name,
            raw_s3_key = EXCLUDED.raw_s3_key,
            clean_s3_key = EXCLUDED.clean_s3_key,
            content_hash = EXCLUDED.content_hash,
            action = EXCLUDED.action,
            status = EXCLUDED.status,
            message = EXCLUDED.message,
            seen_at = EXCLUDED.seen_at;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    return AuditLoadResult(total_received=len(curated_records), inserted=len(rows), failed=failed)


def run_db_load(
    *,
    manifest_records: Sequence[dict[str, Any]],
    curated_records: Sequence[dict[str, Any]],
    pipeline_name: str = "jobs_pipeline",
    dag_id: Optional[str] = None,
    source: Optional[str] = None,
    manage_etl_run: bool = True,
) -> dict[str, Any]:
    """
    Orchestrator-функция файла.

    Полный порядок работы:
    1. Открыть одно соединение с PostgreSQL.
    2. При необходимости создать etl_run.
    3. Загрузить ingestion_manifest.
    4. Нормализовать curated_records.
    5. Получить текущее состояние jobs_curated по job_id.
    6. Обновить job_registry.
    7. Обновить jobs_curated.
    8. Записать аудит в job_audit.
    9. Посчитать unchanged / duplicates.
    10. Завершить etl_run как success.
    11. Сделать commit.

    Если возникает любая ошибка:
    - текущая транзакция откатывается
    - etl_run помечается как failed через отдельное соединение
    - ошибка пробрасывается выше
    """
    with get_connection() as conn:
        conn.autocommit = False
        etl_run_id: Optional[int] = None

        try:
            if manage_etl_run:
                etl_run_id = start_etl_run(
                    conn,
                    pipeline_name=pipeline_name,
                    dag_id=dag_id,
                    source=source,
                )

            # Сначала фиксируем, какие raw/clean файлы участвуют в загрузке.
            manifest_result = upsert_manifest_records(conn, manifest_records)

            # Нормализуем curated-записи заранее, чтобы далее работать уже
            # с единообразным форматом данных.
            normalized_curated = [_normalize_curated_record(record) for record in curated_records]
            existing_curated_state = _get_existing_curated_state(
                conn,
                [record["job_id"] for record in normalized_curated],
            )

            # Обновляем текущий реестр вакансий.
            upsert_job_registry(conn, normalized_curated)
            curated_result = upsert_curated_jobs(conn, normalized_curated)
            audit_result = insert_job_audit_rows(conn, normalized_curated, existing_curated_state)

            unchanged = sum(
                1
                for record in normalized_curated
                if existing_curated_state.get(record["job_id"]) == record.get("content_hash")
            )

            if manage_etl_run and etl_run_id is not None:
                finish_etl_run(
                    conn,
                    etl_run_id,
                    status="success",
                    jobs_extracted=len(curated_records),
                    jobs_new_raw=manifest_result.inserted,
                    jobs_processed_raw=manifest_result.total_received - manifest_result.failed,
                    jobs_curated_inserted=curated_result.inserted,
                    jobs_curated_updated=curated_result.updated,
                    jobs_duplicates=unchanged,
                )

            conn.commit()
            return {
                "etl_run_id": etl_run_id,
                "manifest": manifest_result.__dict__,
                "curated": curated_result.__dict__,
                "audit": audit_result.__dict__,
            }

        except Exception as exc:
            conn.rollback()
            if manage_etl_run and etl_run_id is not None:
                with get_connection() as err_conn:
                    finish_etl_run(
                        err_conn,
                        etl_run_id,
                        status="failed",
                        error_message=str(exc),
                    )
                    err_conn.commit()
            raise