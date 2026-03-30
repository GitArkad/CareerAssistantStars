# app/agents/tools/retriever.py
from qdrant_client import QdrantClient
from app.agents.services.utils import get_embedding
from app.agents.services.state import CandidateProfile

class QdrantRetriever:
    def __init__(self, url: str, collection_name: str):
        self.client = QdrantClient(url=url)
        self.collection_name = collection_name

    def retrieve(self, candidate: CandidateProfile, top_k: int = 20):
        query_text = " ".join(candidate.skills + [candidate.specialization])
        query_vector = get_embedding(query_text)

        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=top_k
        )

        # Фильтр по городу если релокация False
        if candidate.city and not candidate.relocation:
            results = [v for v in results if v.payload.get("city") == candidate.city]

        vacancies = []
        for hit in results:
            payload = hit.payload
            vacancies.append({
                "title": payload.get("title"),
                "company": payload.get("company"),
                "city": payload.get("city"),
                "skills": payload.get("skills", []),
                "salary": payload.get("salary", 0)
            })
        return vacancies