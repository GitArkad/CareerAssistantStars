import os
import logging
import hashlib
from typing import Optional, List, Dict, Any

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchAny
from fastembed import TextEmbedding

logger = logging.getLogger(__name__)

COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "vacancies")
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")

embedding_model = TextEmbedding(model_name=MODEL_NAME)

_client: Optional[QdrantClient] = None


# =========================
# 🔌 CLIENT
# =========================
def get_client() -> QdrantClient:
    global _client
    if _client:
        return _client

    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    client.set_model(MODEL_NAME)
    _client = client
    return _client


# =========================
# 🧠 EMBEDDING
# =========================
def embed_text(text: str) -> List[float]:
    return list(embedding_model.embed([text]))[0]


# =========================
# 🧹 NORMALIZATION
# =========================
def normalize_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    return city.strip().upper()


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return url.split("?")[0].strip()


def make_id(url: str) -> int:
    return int(hashlib.md5(url.encode()).hexdigest(), 16) % (10**12)


# =========================
# 🎨 FORMAT
# =========================
def format_results(scored):
    results = []

    for score, meta in scored:
        results.append({
            "title": meta.get("title"),
            "company": meta.get("company"),
            "skills": meta.get("skills", []),
            "city": meta.get("city"),
            "salary_from": meta.get("salary_from"),
            "salary_to": meta.get("salary_to"),
            "url": meta.get("url"),
            "score": round(float(score), 3)
        })

    return results


# =========================
# 🔍 SEARCH
# =========================
def search_vacancies(
    query_text: str,
    skills: Optional[List[str]] = None,
    normalized_city: Optional[str] = None,
    relocation: bool = False,
    limit: int = 5,
) -> List[Dict[str, Any]]:

    client = get_client()
    query = f"query: {query_text}"
    normalized_city = normalize_city(normalized_city)

    # =========================
    # 🎯 FILTER
    # =========================
    query_filter = None

    if normalized_city and not relocation:
        # 🔥 ТОЛЬКО выбранный город (строго)
        query_filter = Filter(
            must=[
                FieldCondition(
                    key="city",
                    match=MatchValue(value=normalized_city)
                )
            ]
        )

    elif normalized_city and relocation:
        # город + remote
        query_filter = Filter(
            must=[
                FieldCondition(
                    key="city",
                    match=MatchAny(any=[normalized_city, "REMOTE"])
                )
            ]
        )

    # =========================
    # 🔎 QUERY
    # =========================
    query_vector = embed_text(query_text)

    print("QUERY TEXT:", query_text)
    print("VECTOR LEN:", len(query_vector))
    print("VECTOR[:5]:", query_vector[:5])

    search_res = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        query_filter=query_filter,
        limit=limit * 5
    ).points

    print("FILTER:", query_filter)
    for hit in search_res:
        print(hit.metadata.get("city"))

    scored = []
    seen_urls = set()
    user_skills = [s.lower() for s in (skills or [])]

    # =========================
    # 🧠 RERANK + DEDUP
    # =========================
    for hit in search_res:
        meta = hit.metadata or {}

        raw_url = meta.get("url")
        url = normalize_url(raw_url)

        if not url or url in seen_urls:
            continue

        seen_urls.add(url)

        score = hit.score

        # 🔥 skill boosting
        db_skills = [s.lower() for s in meta.get("skills", [])]
        match_count = sum(1 for s in user_skills if s in db_skills)
        score += match_count * 0.1

        meta["url"] = url  # сохраняем нормализованный URL

        scored.append((score, meta))

    # =========================
    # 📊 SORT
    # =========================
    scored.sort(key=lambda x: x[0], reverse=True)

    return format_results(scored[:limit])