import os
from typing import List, Optional
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document
from qdrant_client import QdrantClient
from qdrant_client.http import models

class VacancyRetriever(BaseRetriever):
    collection_name: str = "vacancies"
    top_k: int = 10
    score_threshold: float = 0.5
    client: QdrantClient = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = QdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
        self.collection_name = os.getenv("QDRANT_COLLECTION", "vacancies")
    
    def _get_relevant_documents(self, query: str, run_manager=None, filters: Optional[dict] = None) -> List[Document]:
        query_text = f"query: {query}"
        query_filter = None
        if filters:
            must = []
            if filters.get("city"): must.append(models.FieldCondition(key="city", match=models.MatchValue(value=filters["city"])))
            if filters.get("salary_min"): must.append(models.FieldCondition(key="salary_from", range=models.Range(gte=filters["salary_min"])))
            if must: query_filter = models.Filter(must=must)
        
        try:
            results = self.client.query_points(collection_name=self.collection_name, query=models.Document(text=query_text),
                                                query_filter=query_filter, limit=self.top_k, with_payload=True, with_vectors=False)
        except Exception as e:
            print(f"Qdrant error: {e}")
            return []
        
        documents = []
        for hit in results.points:
            if hit.score < self.score_threshold: continue
            vac = hit.payload or {}
            doc_text = f"{vac.get('title')} в {vac.get('company')}. Навыки: {', '.join(vac.get('skills', [])[:10])}"
            documents.append(Document(page_content=doc_text, metadata={"id": str(hit.id), "title": vac.get("title"), 
                            "company": vac.get("company"), "score": hit.score, **vac}))
        return documents[:self.top_k]

def create_vacancy_retriever(top_k: int = 10, score_threshold: float = 0.5):
    return VacancyRetriever(top_k=top_k, score_threshold=score_threshold)