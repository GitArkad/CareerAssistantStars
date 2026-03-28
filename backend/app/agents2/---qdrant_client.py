# app/agents2/qdrant_client.py
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

def search_vacancies(candidate: dict, client):
    # -----------------------------
    # 1. Query
    # -----------------------------
    query_parts = []

    if candidate.get("specialization"):
        query_parts.append(candidate["specialization"])

    if candidate.get("skills"):
        query_parts.extend(candidate["skills"])

    query = " ".join(query_parts)
    query_text = f"query: {query}"

    city = (candidate.get("city") or "").lower()

    print("QUERY:", query_text)
    print("CITY:", city)

    # -----------------------------
    # 2. Берём много кандидатов
    # -----------------------------
    results = client.query(
        collection_name="vacancies",
        query_text=query_text,
        limit=20
    )

    print("RAW RESULTS:", len(results))

    scored = []

    for hit in results:
        meta = hit.metadata or {}

        db_city = (meta.get("city") or "").lower()
        score = hit.score if hasattr(hit, "score") else 0

        # -----------------------------
        # 🧠 РАНЖИРОВАНИЕ (а не фильтр)
        # -----------------------------

        # 📍 город
        if city and db_city:
            if city in db_city or db_city in city:
                score += 0.2

        # 🌍 remote
        if "remote" in db_city or "удален" in db_city:
            score += 0.1

        scored.append((score, meta))

        print("DB CITY:", db_city, "FINAL:", score)

    # -----------------------------
    # 3. Сортировка
    # -----------------------------
    scored.sort(key=lambda x: x[0], reverse=True)

    # -----------------------------
    # 4. Формируем ответ
    # -----------------------------
    output = []
    for score, meta in scored[:5]:
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