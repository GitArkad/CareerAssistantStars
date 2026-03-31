from __future__ import annotations

import os
import logging
from typing import Optional, List, Dict, Any

from qdrant_client import QdrantClient

from app.agents.services.city_map import CITY_MAP

logger = logging.getLogger(__name__)

COLLECTION_NAME = "vacancies"
MODEL_NAME = "intfloat/multilingual-e5-large"

_client: Optional[QdrantClient] = None


# ============================================================================
# CLIENT
# ============================================================================

def get_client() -> QdrantClient:
    global _client

    if _client:
        return _client

    client = QdrantClient(
        url=os.getenv("QDRANT_URL", "http://localhost:6333"),
        check_compatibility=False,
    )

    client.set_model(MODEL_NAME)

    _client = client
    return _client


# ============================================================================
# CITY NORMALIZATION
# ============================================================================

def normalize_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None

    city = city.lower().strip()

    if city in CITY_MAP:
        return CITY_MAP[city].lower()

    return None  # нет города → нет фильтра


# ============================================================================
# SEARCH
# ============================================================================

def search_vacancies(
    query: str,
    city: Optional[str] = None,
    relocation: bool = True,
    limit: int = 5,
) -> List[Dict]:

    client = get_client()

    query_text = f"query: {query}"
    normalized_city = normalize_city(city)

    results = client.query(
        collection_name=COLLECTION_NAME,
        query_text=query_text,
        limit=limit * 5,  # чуть больше для фильтрации
    )

    scored = []

    for hit in results:
        meta = hit.metadata or {}

        db_city = meta.get("city")
        db_city = db_city.lower() if isinstance(db_city, str) else None

        score = hit.score if hasattr(hit, "score") else 0

        # =========================
        # FILTER
        # =========================
        if not relocation and normalized_city and db_city:
            if db_city != normalized_city:
                continue

        # =========================
        # BOOST
        # =========================
        if normalized_city and db_city == normalized_city:
            score += 0.15

        scored.append((score, meta))

    # =========================
    # FALLBACK (ВАЖНО)
    # =========================
    if not scored:
        logger.warning("No results — fallback to global search")

        for hit in results:
            meta = hit.metadata or {}
            score = hit.score if hasattr(hit, "score") else 0
            scored.append((score, meta))

    # =========================
    # SORT
    # =========================
    scored.sort(key=lambda x: x[0], reverse=True)

    # =========================
    # FORMAT
    # =========================
    output = []

    for score, meta in scored[:limit]:
        output.append({
            "title": meta.get("title"),
            "company": meta.get("company"),
            "skills": meta.get("skills", []),
            "city": meta.get("city"),
            "salary_from": meta.get("salary_from"),
            "salary_to": meta.get("salary_to"),
            "url": meta.get("url"),  # 👈 теперь точно есть
            "score": score,
        })

    return output