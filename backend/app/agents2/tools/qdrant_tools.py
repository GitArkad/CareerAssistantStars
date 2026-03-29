import os
import logging
from typing import Optional, List, Dict, Any

from qdrant_client import QdrantClient, models
from qdrant_client.models import Filter, FieldCondition, MatchAny, Document
from langchain_qdrant import QdrantVectorStore
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings

logger = logging.getLogger(__name__)

COLLECTION_NAME = "vacancies"
MODEL_NAME = "intfloat/multilingual-e5-large"
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")

embeddings = FastEmbedEmbeddings(model_name=MODEL_NAME)

_client: Optional[QdrantClient] = None

def get_client() -> QdrantClient:
    global _client
    if not _client:
        _client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    return _client

def search_vacancies(
    query_text: str,
    skills: Optional[List[str]] = None,
    normalized_city: Optional[str] = None,
    relocation: bool = False,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    
    client = get_client()
    
    # 1. Фильтр (без изменений)
    qdrant_filter = None
    if not relocation and normalized_city:
        city_val = normalized_city.strip().upper() 
        qdrant_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="city", 
                    match=models.MatchAny(any=[city_val])
                )
            ]
        )

    # 2. ПОЛНОСТЬЮ ОБНОВЛЕННЫЙ ПОИСК
    try:
        clean_query = query_text if query_text and query_text != "string" else "Junior Machine Learning Engineer"
        
        # 1. Генерируем вектор через правильный метод LangChain
        # embed_query — это стандарт для одиночного поискового запроса
        query_vector = embeddings.embed_query(f"query: {clean_query}")

        # 2. Передаем в Qdrant
        response = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            using="fast-multilingual-e5-large",
            query_filter=qdrant_filter,
            limit=limit,
        )
        search_results = response.points
        
        if search_results:
            print(f"✅ ВЕКТОРНЫЙ ПОИСК ЖИВ! Top Score: {search_results[0].score}")
            
    except Exception as e:
        print(f"❌ ОШИБКА ВЕКТОРНОГО ПОИСКА: {e}")
        # Emergency scroll
        search_results = client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=qdrant_filter,
            limit=limit,
            with_payload=True
        )[0]

    scored_results = []
    seen_urls = set()
    user_skills = [s.lower() for s in (skills or [])]

    # 3. Обработка результатов
    for res in search_results:
        # В query_points данные в .payload, в scroll — тоже
        payload = getattr(res, 'payload', None)
        if not payload:
            continue

        title = payload.get("title")
        if not title:
            continue

        url = payload.get("url") or "#"
        clean_url = str(url).split("?")[0].strip()
        
        if clean_url != "#" and clean_url in seen_urls:
            continue
        seen_urls.add(clean_url)

        db_skills = payload.get("skills", [])
        match_count = sum(1 for s in user_skills if s in [str(ds).lower() for ds in db_skills])
        
        # У scroll нет score, ставим заглушку 0.5
        score = getattr(res, 'score', 0.5)
        final_score = float(score) + (match_count * 0.15)

        scored_results.append({
            "title": title,
            "company": payload.get("company", "Unknown"),
            "skills": db_skills,
            "city": payload.get("city"),
            "salary_from": payload.get("salary_from"),
            "url": url,
            "score": round(final_score, 3)
        })

    scored_results.sort(key=lambda x: x["score"], reverse=True)
    return scored_results[:limit]