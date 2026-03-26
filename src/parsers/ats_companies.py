"""
ats_companies.py

ATS company registry with health tracking.
Permanent failures deactivate faster, temporary failures require repeated misses.
Status persists to S3 with local fallback.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_REGISTRY: Dict[str, List[str]] = {
    "greenhouse": [
        "airbnb", "alma", "andurilindustries", "axon", "bird",
        "braze", "calendly", "cloudflare", "coinbase", "databricks",
        "engine", "fanduel", "federato", "greenhouse", "hubspotjobs",
        "kayzen", "kaizengaming", "lyft", "mark43", "mongodb",
        "nmi", "nuitee", "orennia", "oura", "pinterest",
        "praxisprecisionmedicines", "samsara", "scopely", "sofi", "tailscale",
        "tailorcare2023", "terraclear", "torcrobotics", "valtech", "veracyte",
        "veeamsoftware", "wakam", "waymo", "wheely", "yld",
        "6sense", "attune", "blip-global", "exadelinc", "honehealth",
        "hungryroot", "innovecs", "manychat", "moduscreate", "nucleo",
    ],
    "lever": [
        "achievers", "angel", "arcteryx.com", "artera", "BDG",
        "binance", "canvasww", "dnb", "egen", "gohighlevel",
        "hive", "jobgether", "kabam", "kitware", "levelai",
        "metabase", "mistral", "neighbor", "palantir", "plaid",
        "pointclickcare", "quantco-", "raya", "regrello", "sanctuary",
        "swile", "voleon", "waveapps", "welocalize", "zopa",
        "daniels-sharpsmart", "nmi", "oura", "mark43", "nuitee",
    ],
    "ashby": [
        "openai", "ramp", "evenup", "perplexity", "suno",
        "faculty", "deel", "kraken.com", "trm-labs", "super.com",
        "unify", "rula", "notable", "ashby", "middesk",
        "concourse", "fluency", "wynd-labs", "dust", "mai",
        "Linkup", "the-exploration-company", "foundry-for-good",
    ],
}

_S3_KEY = "config/ats_status.json"
_LOCAL = "/tmp/ats_status.json"
_cache: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
_TEMP_REASONS = ("timeout", "timed out", "503", "502", "500", "429", "connection", "temporary")
_PERM_REASONS = ("404", "410", "invalid board", "not found")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_reason(reason: str) -> str:
    return (reason or "unknown").strip().lower()


def _threshold_for_reason(reason: str) -> int:
    rs = _normalize_reason(reason)
    if any(token in rs for token in _PERM_REASONS):
        return 2
    if any(token in rs for token in _TEMP_REASONS):
        return 5
    return 3


def _load() -> Dict[str, Dict[str, Dict[str, Any]]]:
    global _cache
    if _cache is not None:
        return _cache

    try:
        from src.loaders.s3_storage import get_bucket, get_s3_client

        body = get_s3_client().get_object(Bucket=get_bucket(), Key=_S3_KEY)["Body"].read()
        _cache = json.loads(body.decode("utf-8"))
        return _cache
    except Exception:
        pass

    if os.path.exists(_LOCAL):
        try:
            with open(_LOCAL, "r", encoding="utf-8") as f:
                _cache = json.load(f)
                return _cache
        except Exception:
            pass

    _cache = {}
    return _cache


def _save() -> None:
    if _cache is None:
        return

    data = json.dumps(_cache, indent=2, ensure_ascii=False, default=str)

    try:
        with open(_LOCAL, "w", encoding="utf-8") as f:
            f.write(data)
    except Exception:
        pass

    try:
        from src.loaders.s3_storage import get_bucket, get_s3_client

        get_s3_client().put_object(
            Bucket=get_bucket(),
            Key=_S3_KEY,
            Body=data.encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        logger.warning("ATS status S3 save failed: %s", e)


def get_active_companies(platform: str) -> List[str]:
    all_companies = _REGISTRY.get(platform, [])
    st = _load().get(platform, {})
    active = [c for c in all_companies if st.get(c, {}).get("active", True)]
    skipped = len(all_companies) - len(active)
    if skipped:
        logger.info("[%s] %s active, %s skipped", platform, len(active), skipped)
    return active


def mark_inactive(platform: str, company: str, reason: str = "unknown") -> None:
    st = _load()
    st.setdefault(platform, {})
    entry = st[platform].get(company, {"active": True, "fail_count": 0})
    fail_count = int(entry.get("fail_count", 0)) + 1
    threshold = _threshold_for_reason(reason)
    now = _utc_now_iso()

    payload: Dict[str, Any] = {
        "active": fail_count < threshold,
        "reason": reason,
        "fail_count": fail_count,
        "last_fail_at": now,
        "threshold": threshold,
    }
    if fail_count >= threshold:
        payload["deactivated_at"] = now
        logger.warning("[%s] DEACTIVATED '%s': %s (%s/%s)", platform, company, reason, fail_count, threshold)
    else:
        logger.info("[%s] '%s' fail %s/%s: %s", platform, company, fail_count, threshold, reason)

    st[platform][company] = payload
    _save()


def record_success(platform: str, company: str) -> None:
    st = _load()
    if platform in st and company in st[platform]:
        st[platform][company] = {
            "active": True,
            "fail_count": 0,
            "last_success_at": _utc_now_iso(),
        }
        _save()


def reset_fail_count(platform: str, company: str) -> None:
    record_success(platform, company)


def mark_active(platform: str, company: str) -> None:
    st = _load()
    st.setdefault(platform, {})
    st[platform][company] = {
        "active": True,
        "fail_count": 0,
        "reactivated_at": _utc_now_iso(),
    }
    _save()
    logger.info("[%s] Reactivated '%s'", platform, company)


def get_status_summary() -> Dict[str, Dict[str, Any]]:
    st = _load()
    out: Dict[str, Dict[str, Any]] = {}
    for platform, companies in _REGISTRY.items():
        ps = st.get(platform, {})
        dead = [c for c in companies if not ps.get(c, {}).get("active", True)]
        out[platform] = {
            "total": len(companies),
            "active": len(companies) - len(dead),
            "inactive": len(dead),
            "inactive_list": dead,
        }
    return out


GREENHOUSE_COMPANIES = _REGISTRY["greenhouse"]
LEVER_COMPANIES = _REGISTRY["lever"]
ASHBY_COMPANIES = _REGISTRY["ashby"]