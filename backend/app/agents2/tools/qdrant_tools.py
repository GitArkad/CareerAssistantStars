# app/agents2/tools/qdrant_tools.py
import os
import logging
from typing import Optional, List, Dict, Any
from qdrant_client import QdrantClient

logger = logging.getLogger(__name__)

COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "vacancies")
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")

_client: Optional[QdrantClient] = None

def get_client() -> QdrantClient:
    global _client
    if _client:
        return _client
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    client.set_model(MODEL_NAME)
    _client = client
    return _client

def normalize_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    return city.strip().lower()

def search_vacancies(query: str, city: Optional[str] = None, relocation: bool = True, limit: int = 5) -> List[Dict]:
    client = get_client()
    query_text = f"query: {query}"
    normalized_city = normalize_city(city)

    results = client.query(
        collection_name=COLLECTION_NAME,
        query_text=query_text,
        limit=limit*5
    )

    scored = []
    for hit in results:
        meta = hit.metadata or {}
        db_city = meta.get("city")
        db_city = db_city.lower() if isinstance(db_city, str) else None
        score = hit.score if hasattr(hit, "score") else 0

        if not relocation and normalized_city and db_city:
            if db_city != normalized_city:
                continue
        if normalized_city and db_city == normalized_city:
            score += 0.15
        scored.append((score, meta))

    if not scored:
        logger.warning("No results — fallback to global search")
        for hit in results:
            meta = hit.metadata or {}
            score = hit.score if hasattr(hit, "score") else 0
            scored.append((score, meta))

    scored.sort(key=lambda x: x[0], reverse=True)

    output = []
    for score, meta in scored[:limit]:
        output.append({
            "title": meta.get("title"),
            "company": meta.get("company"),
            "skills": meta.get("skills", []),
            "city": meta.get("city"),
            "salary_from": meta.get("salary_from"),
            "salary_to": meta.get("salary_to"),
            "url": meta.get("url"),
            "score": score
        })
    return output