from fastapi import APIRouter
from app.services.qdrant_service import client, COLLECTION_NAME

router = APIRouter()

@router.post("/semantic")
def semantic_search(request: dict):
    """
    🔍 Semantic search через Qdrant
    """

    query_vector = request.get("vector")
    limit = request.get("limit", 5)

    if not query_vector:
        return {"error": "vector is required"}

    results = client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=limit
    )

    response = []

    for r in results:
        response.append({
            "job_id": r.payload.get("job_id"),
            "title": r.payload.get("title"),
            "company": r.payload.get("company"),
            "score": r.score,
            "url": r.payload.get("url")
        })

    return {
        "count": len(response),
        "results": response
    }