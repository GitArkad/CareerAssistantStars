"""
load_to_db.py

Airflow task helper для загрузки cleaned snapshot в PostgreSQL.

Роль файла в пайплайне:
- скачать cleaned snapshot из S3/MinIO
- привести строки DataFrame к формату, который ожидает db_loader.py
- собрать file-level manifest records
- передать manifest + curated records в единый DB loader

Важно:
- raw payload вакансий остаётся в S3/MinIO, не переносится в PostgreSQL
- ingestion_manifest здесь формируется на УРОВНЕ ФАЙЛОВ, а не по строкам
- для корректной связи parse -> clean -> load желательно передавать raw_s3_keys из task_parse
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

LIST_COLUMNS = [
    "key_skills",
    "skills_extracted",
    "skills_normalized",
    "tech_stack_tags",
    "tools",
    "methodologies",
    "spoken_languages",
]

TEXT_COLUMNS = [
    "benefits",
    "education",
    "certifications",
    "equity_bonus",
    "security_clearance",
]


def _parse_list_like(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"none", "null", "nan"}:
            return []
        if text.startswith("{") and text.endswith("}"):
            inner = text[1:-1].strip()
            if not inner:
                return []
            return [part.strip().strip('"').strip("'") for part in inner.split(",") if part.strip()]
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                pass
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                pass
        return [part.strip() for part in text.split(",") if part.strip()]
    return [str(value).strip()] if str(value).strip() else []


def _none_if_nan(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n", "", "onsite", "on-site", "office"}:
        return False
    return default


def _safe_source(source: str) -> str:
    return str(source).replace(".", "_").replace(" ", "_").replace("/", "_")


def _prepare_record_dict(row: pd.Series) -> dict[str, Any]:
    d = {k: _none_if_nan(v) for k, v in row.to_dict().items()}

    if not d.get("company_name") and d.get("company"):
        d["company_name"] = d.get("company")

    if d.get("salary_from") is None and d.get("salary_min") is not None:
        d["salary_from"] = d.get("salary_min")
    if d.get("salary_to") is None and d.get("salary_max") is not None:
        d["salary_to"] = d.get("salary_max")

    if not d.get("source_job_id") and d.get("job_id"):
        d["source_job_id"] = d.get("job_id")

    for col in LIST_COLUMNS:
        d[col] = _parse_list_like(d.get(col))

    for col in TEXT_COLUMNS:
        value = d.get(col)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            d[col] = None
        elif isinstance(value, (list, tuple)):
            joined = "; ".join(str(item).strip() for item in value if str(item).strip())
            d[col] = joined or None
        else:
            text_value = str(value).strip()
            d[col] = text_value or None

    if d.get("remote") is None and d.get("remote_type"):
        d["remote"] = str(d["remote_type"]).strip().lower() in {"remote", "hybrid"}
    else:
        d["remote"] = _normalize_bool(d.get("remote"), default=False)

    for bool_col in [
        "visa_sponsorship",
        "relocation",
        "is_data_role",
        "is_ml_role",
        "is_python_role",
        "is_analyst_role",
    ]:
        if bool_col in d:
            d[bool_col] = _normalize_bool(d.get(bool_col), default=False)

    if not d.get("embedding_status"):
        d["embedding_status"] = "pending"
    if not d.get("role_family"):
        d["role_family"] = "other"
    if not d.get("seniority_normalized"):
        d["seniority_normalized"] = "unknown"
    if not d.get("country_normalized") and d.get("country"):
        d["country_normalized"] = d.get("country")

    return d


def _extract_clean_key_parts(clean_s3_key: str) -> tuple[Optional[str], Optional[str]]:
    parts = str(clean_s3_key).strip("/").split("/")
    if len(parts) >= 4 and parts[0] == "clean":
        return parts[1], parts[2]
    return None, None


def _extract_raw_key_parts(raw_s3_key: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    parts = str(raw_s3_key).strip("/").split("/")
    # raw/{date}/{run_id}/{safe_source}.csv
    if len(parts) >= 4 and parts[0] == "raw":
        source_file = parts[3]
        source_safe = source_file[:-4] if source_file.endswith(".csv") else source_file
        return parts[1], parts[2], source_safe
    return None, None, None


def _build_content_hash(d: dict[str, Any]) -> str:
    stable = "|".join(
        [
            str(d.get("job_id") or ""),
            str(d.get("source") or ""),
            str(d.get("source_job_id") or ""),
            str(d.get("url") or ""),
            str(d.get("title") or ""),
            str(d.get("company_name") or ""),
        ]
    )
    return hashlib.sha1(stable.encode("utf-8")).hexdigest()


def _build_raw_s3_key(date_part: Optional[str], run_part: Optional[str], source: Any) -> Optional[str]:
    if not date_part or not run_part or not source:
        return None
    source_name = str(source).strip()
    if not source_name:
        return None
    return f"raw/{date_part}/{run_part}/{_safe_source(source_name)}.csv"


def _raw_key_map(raw_s3_keys: Optional[list[str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for key in raw_s3_keys or []:
        _, _, source_safe = _extract_raw_key_parts(key)
        if source_safe:
            mapping[source_safe] = key
    return mapping


def build_file_manifest_records(
    df: pd.DataFrame,
    clean_s3_key: str,
    raw_s3_keys: Optional[list[str]],
) -> list[dict[str, Any]]:
    date_part, run_part = _extract_clean_key_parts(clean_s3_key)
    df2 = df.copy()
    if "source" in df2.columns:
        df2["_source_safe"] = df2["source"].astype(str).map(_safe_source)
        counts = df2.groupby("_source_safe").size().to_dict()
        source_names = (
            df2.groupby("_source_safe")["source"].agg(lambda s: str(s.iloc[0])).to_dict()
        )
    else:
        counts = {}
        source_names = {}

    records: list[dict[str, Any]] = []

    if raw_s3_keys:
        for raw_key in raw_s3_keys:
            raw_date, raw_run_id, source_safe = _extract_raw_key_parts(raw_key)
            records.append(
                {
                    "run_id": raw_run_id or run_part or date_part,
                    "source": source_names.get(source_safe, source_safe),
                    "raw_s3_key": raw_key,
                    "clean_s3_key": clean_s3_key,
                    "raw_file_hash": None,
                    "raw_row_count": None,
                    "clean_row_count": counts.get(source_safe),
                    "loaded_row_count": counts.get(source_safe),
                    "fetched_at": None,
                    "parsed_at": None,
                    "cleaned_at": datetime.utcnow(),
                    "loaded_at": datetime.utcnow(),
                    "status": "loaded",
                    "error_message": None,
                    "metadata": {"date": raw_date, "source_safe": source_safe},
                }
            )
        return records

    # manual / fallback mode without raw_s3_keys
    for source_safe, clean_count in counts.items():
        source_name = source_names.get(source_safe, source_safe)
        records.append(
            {
                "run_id": run_part or date_part,
                "source": source_name,
                "raw_s3_key": _build_raw_s3_key(date_part, run_part, source_name),
                "clean_s3_key": clean_s3_key,
                "raw_file_hash": None,
                "raw_row_count": None,
                "clean_row_count": clean_count,
                "loaded_row_count": clean_count,
                "fetched_at": None,
                "parsed_at": None,
                "cleaned_at": datetime.utcnow(),
                "loaded_at": datetime.utcnow(),
                "status": "loaded",
                "error_message": None,
                "metadata": {"fallback": True, "source_safe": source_safe},
            }
        )
    return records


def df_to_curated_records(
    df: pd.DataFrame,
    clean_s3_key: str,
    raw_s3_keys: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    date_part, run_part = _extract_clean_key_parts(clean_s3_key)
    records: list[dict[str, Any]] = []
    raw_map = _raw_key_map(raw_s3_keys)

    for _, row in df.iterrows():
        d = _prepare_record_dict(row)
        d["run_id"] = run_part or date_part
        d["clean_s3_key"] = clean_s3_key

        if not d.get("raw_s3_key"):
            source_safe = _safe_source(d.get("source") or "")
            d["raw_s3_key"] = raw_map.get(source_safe) or _build_raw_s3_key(date_part, run_part, d.get("source"))

        d["content_hash"] = d.get("content_hash") or _build_content_hash(d)
        records.append(d)

    return records


def resolve_clean_s3_key(date_str: Optional[str] = None, clean_s3_key: Optional[str] = None) -> str:
    from src.loaders.s3_storage import key_exists, latest_clean_key, list_keys

    if clean_s3_key:
        if key_exists(clean_s3_key):
            return clean_s3_key
        raise FileNotFoundError(f"Provided clean_s3_key does not exist: {clean_s3_key}")

    latest_key = latest_clean_key()
    if key_exists(latest_key):
        logger.info("Using latest clean dataset: %s", latest_key)
        return latest_key

    if date_str:
        prefix = f"clean/{date_str}/"
        keys = [k for k in list_keys(prefix) if k.endswith(".csv")]
        if keys:
            resolved = sorted(keys)[-1]
            logger.info("Using most recent dated clean snapshot: %s", resolved)
            return resolved

    raise FileNotFoundError(
        "Could not resolve cleaned dataset key. Pass clean_s3_key from task_clean or ensure clean/latest exists."
    )


def run_load_step(
    date_str: Optional[str] = None,
    clean_s3_key: Optional[str] = None,
    raw_s3_keys: Optional[list[str]] = None,
) -> dict[str, Any]:
    from src.loaders.db_loader import run_db_load
    from src.loaders.s3_storage import download_df

    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    key = resolve_clean_s3_key(date_str=date_str, clean_s3_key=clean_s3_key)

    logger.info("Downloading cleaned dataset from key=%s", key)
    df = download_df(key)
    logger.info("Downloaded %s cleaned rows", len(df))

    if df.empty:
        logger.warning("Cleaned dataset is empty: key=%s", key)

    manifest_records = build_file_manifest_records(df, key, raw_s3_keys)
    curated_records = df_to_curated_records(df, key, raw_s3_keys=raw_s3_keys)

    summary = run_db_load(
        manifest_records=manifest_records,
        curated_records=curated_records,
        pipeline_name="jobs_pipeline",
        dag_id="jobs_pipeline_weekly",
        source="multi-source",
        manage_etl_run=True,
    )
    summary["clean_s3_key"] = key
    summary["manifest_record_count"] = len(manifest_records)
    logger.info("DB load completed: %s", summary)
    return summary


if __name__ == "__main__":
    print(run_load_step())