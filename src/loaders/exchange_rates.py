"""
exchange_rates.py

Обновление официальных курсов валют для нормализации зарплат.
Источники:
- ECB
- Банк России (CBR)

Результат:
- расчёт EUR-базовых курсов
- построение cross-rates
- upsert в exchange_rates
"""

from __future__ import annotations

import csv
import io
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Валюты, которые должны поддерживаться в отображении.
TARGET_CURRENCIES = ["USD", "EUR", "RUB"]

# Валюты, встречающиеся в исходных данных вакансий.
SOURCE_CURRENCIES = ["GBP", "KZT", "PLN", "UAH", "CAD", "AUD", "INR", "SGD", "BYN"]

PIPELINE_CURRENCIES = sorted(set(TARGET_CURRENCIES + SOURCE_CURRENCIES))
PIPELINE_NON_EUR = sorted(c for c in PIPELINE_CURRENCIES if c != "EUR")

# Официальные API.
ECB_API_BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
CBR_DAILY_URL = "https://www.cbr.ru/scripts/XML_daily_eng.asp"

# Валюты, которые считаются основными для загрузки из ECB.
# Остальные дополняются из CBR.
ECB_DAILY_CURRENCIES = {
    "USD",
    "GBP",
    "PLN",
    "CAD",
    "AUD",
    "INR",
    "SGD",
}

LOOKBACK_DAYS = 10
REQUEST_TIMEOUT = 20


###########################################################
# Вспомогательные функции
###########################################################

def _parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_cbr_date(value: str) -> str:
    return _parse_iso_date(value).strftime("%d/%m/%Y")


def _pick_as_of_date(rate_date: Optional[str]) -> date:
    return _parse_iso_date(rate_date) if rate_date else date.today()


def _ecb_requested_currencies() -> list[str]:
    return [c for c in PIPELINE_NON_EUR if c in ECB_DAILY_CURRENCIES]


###########################################################
# ECB
###########################################################

def _build_ecb_series_key(currencies: list[str]) -> str:
    if not currencies:
        raise ValueError("No currencies configured for ECB request.")
    return f"D.{'+'.join(currencies)}.EUR.SP00.A"


@lru_cache(maxsize=128)
def _fetch_ecb_window(start_period: str, end_period: str) -> list[dict[str, str]]:
 # Загружаем окно дат из ECB одним запросом.

    requested = _ecb_requested_currencies()
    series_key = _build_ecb_series_key(requested)
    url = f"{ECB_API_BASE_URL}/{series_key}"

    params = {
        "format": "csvdata",
        "startPeriod": start_period,
        "endPeriod": end_period,
    }

    resp = requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "text/csv"},
    )
    resp.raise_for_status()

    rows = list(csv.DictReader(io.StringIO(resp.text)))
    if not rows:
        raise ValueError(f"ECB returned no data for {start_period} .. {end_period}")
    return rows



def _build_ecb_by_date(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    # Собираем курсы по датам и оставляем только полные даты.

    requested = _ecb_requested_currencies()
    by_date: dict[str, dict[str, float]] = {}

    for row in rows:
        currency = row.get("CURRENCY")
        time_period = row.get("TIME_PERIOD")
        obs_value = row.get("OBS_VALUE")

        if not currency or not time_period or not obs_value:
            continue
        if currency not in requested:
            continue

        try:
            rate = float(obs_value)
        except ValueError:
            logger.warning("Skipping invalid ECB row: %s", row)
            continue

        by_date.setdefault(time_period, {})[currency] = rate

    complete_only: dict[str, dict[str, float]] = {}
    for d, rates in by_date.items():
        if all(cur in rates for cur in requested):
            complete_only[d] = rates

    return complete_only


###########################################################
# Bank of Russia (CBR)
###########################################################

def _parse_cbr_document(xml_text: str) -> tuple[str, dict[str, float]]:
    # Парсим XML CBR в словарь RUB-based курсов.

    root = ET.fromstring(xml_text)

    raw_date = (root.attrib.get("Date") or "").strip()
    if not raw_date:
        raise ValueError("CBR XML does not contain Date attribute")

    actual_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")

    rub_rates: dict[str, float] = {"RUB": 1.0}

    for node in root.findall("Valute"):
        code = (node.findtext("CharCode") or "").strip().upper()
        nominal_text = (node.findtext("Nominal") or "1").strip()
        value_text = (node.findtext("Value") or "").strip()

        if not code or not value_text:
            continue

        try:
            nominal = int(nominal_text)
            value = float(value_text.replace(",", "."))
        except ValueError:
            logger.warning("Skipping invalid CBR node for currency %s", code)
            continue

        if nominal <= 0:
            logger.warning("Skipping CBR currency %s with non-positive nominal %s", code, nominal)
            continue

        rub_rates[code] = value / nominal

    if "EUR" not in rub_rates:
        raise ValueError("CBR XML does not contain EUR rate, cannot compute EUR-based supplements")

    return actual_date, rub_rates


@lru_cache(maxsize=128)
def _fetch_cbr_for_requested_date(requested_date: str) -> tuple[str, dict[str, float]]:
    # Загружаем ежедневный XML CBR за указанную дату.

    resp = requests.get(
        CBR_DAILY_URL,
        params={"date_req": _to_cbr_date(requested_date)},
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "application/xml, text/xml"},
    )
    resp.raise_for_status()
    return _parse_cbr_document(resp.text)


###########################################################
# Сведение источников
###########################################################

def _compose_eur_rates_for_date(candidate_date: str, ecb_by_date: dict[str, dict[str, float]]) -> tuple[dict[str, float], dict]:
# Собираем единый набор EUR-based курсов для одной общей официальной даты.

    if candidate_date not in ecb_by_date:
        raise ValueError(f"ECB has no complete observation for {candidate_date}")

    cbr_actual_date, cbr_rub_rates = _fetch_cbr_for_requested_date(candidate_date)
    if cbr_actual_date != candidate_date:
        raise ValueError(
            f"CBR returned {cbr_actual_date} for requested {candidate_date}; exact common date not available"
        )

    eur_rates = dict(ecb_by_date[candidate_date])
    eur_rub = cbr_rub_rates["EUR"]

    # Прямой курс EUR -> RUB.
    eur_rates["RUB"] = eur_rub

    supplemented_from_cbr: list[str] = []

    for cur in PIPELINE_NON_EUR:
        if cur in eur_rates:
            continue
        if cur not in cbr_rub_rates:
            continue

        # Достраиваем недостающие валюты через RUB-базу CBR.
        eur_rates[cur] = eur_rub / cbr_rub_rates[cur]
        supplemented_from_cbr.append(cur)

    missing_from_pipeline = sorted(set(PIPELINE_NON_EUR) - set(eur_rates.keys()))

    meta = {
        "candidate_date": candidate_date,
        "ecb_currencies": sorted(ecb_by_date[candidate_date].keys()),
        "cbr_supplemented": sorted(set(supplemented_from_cbr + ["RUB"])),
        "missing_from_pipeline": missing_from_pipeline,
    }
    return eur_rates, meta



def fetch_official_rates(rate_date: Optional[str] = None) -> tuple[dict[str, float], str, dict]:
    # Ищем ближайшую общую официальную дату на или до requested date.

    as_of_date = _pick_as_of_date(rate_date)
    start_period = (as_of_date - timedelta(days=LOOKBACK_DAYS)).isoformat()
    end_period = as_of_date.isoformat()

    ecb_rows = _fetch_ecb_window(start_period, end_period)
    ecb_by_date = _build_ecb_by_date(ecb_rows)

    if not ecb_by_date:
        raise ValueError(f"ECB returned no complete dates for {start_period} .. {end_period}")

    for offset in range(LOOKBACK_DAYS + 1):
        candidate = (as_of_date - timedelta(days=offset)).isoformat()
        if candidate not in ecb_by_date:
            continue

        try:
            eur_rates, meta = _compose_eur_rates_for_date(candidate, ecb_by_date)
            logger.info(
                "Resolved official FX date %s (requested=%s): ECB=%s, CBR supplemented=%s, missing=%s",
                candidate,
                rate_date,
                meta["ecb_currencies"],
                meta["cbr_supplemented"],
                meta["missing_from_pipeline"],
            )
            return eur_rates, candidate, meta
        except Exception as exc:
            logger.warning("Skipping candidate FX date %s: %s", candidate, exc)
            continue

    raise ValueError(
        f"Could not resolve one common official FX date on or before {as_of_date.isoformat()}"
    )


###########################################################
# Расчёт cross-rates
###########################################################

def compute_cross_rates(eur_rates: dict[str, float]) -> list[dict]:
# Строим все пары base_currency -> target_currency из EUR-базы.

    full_rates = {"EUR": 1.0, **eur_rates}
    records: list[dict] = []

    for base_cur, base_rate in full_rates.items():
        for target_cur, target_rate in full_rates.items():
            if base_cur == target_cur:
                rate = 1.0
            else:
                rate = round(target_rate / base_rate, 6)

            records.append(
                {
                    "base_currency": base_cur,
                    "target_currency": target_cur,
                    "rate": rate,
                }
            )

    return records


###########################################################
# Запись в Postgres
###########################################################

def _get_connection():
    # Поддерживаем импорт как из src, так и из локального запуска.

    try:
        from src.loaders.db_loader import get_connection
    except ImportError:
        from db_loader import get_connection
    return get_connection()



def upsert_rates(rate_date: str, records: list[dict]) -> None:
    # Записываем курсы в exchange_rates через upsert.

    from psycopg2.extras import execute_batch

    conn = _get_connection()
    cur = conn.cursor()

    sql = """
        INSERT INTO exchange_rates (rate_date, base_currency, target_currency, rate)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (rate_date, base_currency, target_currency)
        DO UPDATE SET rate = EXCLUDED.rate, updated_at = NOW()
    """

    rows = [
        (rate_date, r["base_currency"], r["target_currency"], r["rate"])
        for r in records
    ]

    execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    cur.close()
    conn.close()

    logger.info("Upserted %d exchange-rate rows for %s", len(rows), rate_date)


###########################################################
# Основной шаг пайплайна
###########################################################

def run_update_rates(rate_date: Optional[str] = None) -> dict:
# Основной entrypoint для Airflow.

    eur_rates, actual_date, meta = fetch_official_rates(rate_date)
    cross_rates = compute_cross_rates(eur_rates)
    upsert_rates(actual_date, cross_rates)

    return {
        "requested_date": rate_date,
        "actual_date": actual_date,
        "status": "success",
        "eur_loaded_currencies": sorted(eur_rates.keys()),
        "eur_loaded_count": len(eur_rates),
        "cross_rate_pairs": len(cross_rates),
        "ecb_primary": meta["ecb_currencies"],
        "cbr_supplemented": meta["cbr_supplemented"],
        "missing_from_pipeline": meta["missing_from_pipeline"],
    }



def backfill_rates(start_date: str, end_date: Optional[str] = None) -> None:
    # Историческая дозагрузка курсов по диапазону дат.
    
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date) if end_date else date.today()

    current = start
    while current <= end:
        current_str = current.isoformat()
        try:
            result = run_update_rates(current_str)
            logger.info(
                "Backfilled FX for requested=%s actual=%s",
                current_str,
                result["actual_date"],
            )
        except Exception as exc:
            logger.error("Failed to backfill %s: %s", current_str, exc)
        current += timedelta(days=1)


if __name__ == "__main__":
    print(run_update_rates())