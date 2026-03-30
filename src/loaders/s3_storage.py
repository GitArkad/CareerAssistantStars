from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Импорт boto3 и связанных классов
def _import_boto3():
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
    return boto3, Config, ClientError

# Создание S3-клиента для S3 или MinIO
def get_s3_client():
    boto3, Config, _ = _import_boto3()

    endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    region = os.getenv("S3_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )

# Получение имени bucket из env
def get_bucket() -> str:
    return os.getenv("S3_BUCKET", "jobs-pipeline")

# Создание bucket, если его ещё нет
def ensure_bucket() -> None:
    """Create bucket only when it is actually missing."""
    client = get_s3_client()
    bucket = get_bucket()
    _, _, ClientError = _import_boto3()

    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        # Missing bucket -> create it. Permission/network problems -> raise.
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    client.create_bucket(Bucket=bucket)
    logger.info("Created S3 bucket: %s", bucket)

# Загрузка DataFrame в S3 как CSV
def upload_df(df: pd.DataFrame, s3_key: str) -> str:
    """Upload a DataFrame as UTF-8-SIG CSV."""
    ensure_bucket()
    client = get_s3_client()
    bucket = get_bucket()

    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    buffer.seek(0)

    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )
    logger.info("Uploaded %s rows to s3://%s/%s", len(df), bucket, s3_key)
    return s3_key


# Загрузка CSV из S3 в DataFrame
def download_df(s3_key: str) -> pd.DataFrame:
    client = get_s3_client()
    bucket = get_bucket()

    response = client.get_object(Bucket=bucket, Key=s3_key)
    body = response["Body"].read()
    df = pd.read_csv(io.BytesIO(body), encoding="utf-8-sig")
    logger.info("Downloaded %s rows from s3://%s/%s", len(df), bucket, s3_key)
    return df

# Безопасная загрузка DataFrame: пустой DataFrame, если ключа нет
def safe_download_df(s3_key: str) -> pd.DataFrame:
    """Return empty DataFrame if key is absent."""
    return download_df(s3_key) if key_exists(s3_key) else pd.DataFrame()

# Загрузка локального файла в S3
def upload_file(local_path: str, s3_key: str) -> str:
    ensure_bucket()
    client = get_s3_client()
    bucket = get_bucket()
    client.upload_file(local_path, bucket, s3_key)
    logger.info("Uploaded %s to s3://%s/%s", local_path, bucket, s3_key)
    return s3_key

# Загрузка файла из S3 на локальный диск
def download_file(s3_key: str, local_path: str) -> str:
    client = get_s3_client()
    bucket = get_bucket()
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    client.download_file(bucket, s3_key, local_path)
    logger.info("Downloaded s3://%s/%s to %s", bucket, s3_key, local_path)
    return local_path

# Список ключей по prefix
def list_keys(prefix: str) -> list[str]:
    client = get_s3_client()
    bucket = get_bucket()
    keys: list[str] = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return sorted(keys)

# Проверка существования объекта в S3
def key_exists(s3_key: str) -> bool:
    client = get_s3_client()
    bucket = get_bucket()
    _, _, ClientError = _import_boto3()

    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


# Вспомогательные функции для дат и путей
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_run_id(date_str: Optional[str] = None) -> str:
    now = utc_now()
    date_part = date_str or now.strftime("%Y-%m-%d")
    return f"{date_part}T{now.strftime('%H%M%S')}Z"

# Нормализация source для безопасного имени файла
def _safe_source(source: str) -> str:
    return source.replace(".", "_").replace(" ", "_").replace("/", "_")

# Ключ raw snapshot для одного источника
def raw_key(source: str, date_str: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Append-only raw snapshot key for one parser/source output."""
    date_str = date_str or utc_now().strftime("%Y-%m-%d")
    run_id = run_id or make_run_id(date_str)
    return f"raw/{date_str}/{run_id}/{_safe_source(source)}.csv"

# Ключ merged raw snapshot для одного запуска
def merged_raw_key(date_str: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Append-only merged raw snapshot for a single pipeline run."""
    date_str = date_str or utc_now().strftime("%Y-%m-%d")
    run_id = run_id or make_run_id(date_str)
    return f"raw/{date_str}/{run_id}/all_raw_merged.csv"

# Ключ clean snapshot для одного запуска
def clean_key(date_str: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Append-only clean snapshot for a single pipeline run."""
    date_str = date_str or utc_now().strftime("%Y-%m-%d")
    run_id = run_id or make_run_id(date_str)
    return f"clean/{date_str}/{run_id}/jobs_clean.csv"

# Стабильный ключ latest clean snapshot
def latest_clean_key() -> str:
    """Stable accumulated clean dataset pointer."""
    return "clean/latest/jobs_clean_latest.csv"

# Prefix для raw snapshot за дату
def latest_raw_snapshot_prefix(date_str: Optional[str] = None) -> str:
    date_str = date_str or utc_now().strftime("%Y-%m-%d")
    return f"raw/{date_str}/"