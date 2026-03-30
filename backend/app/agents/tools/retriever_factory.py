# app/agents/tools/retriever_factory.py
import os
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams

from langchain.vectorstores import Qdrant

def get_qdrant_retriever():
    qdrant_url = os.getenv("QDRANT_URL")
    collection_name = os.getenv("QDRANT_COLLECTION_NAME")

    client = QdrantClient(url=qdrant_url)

    retriever = Qdrant(
        client=client,
        collection_name=collection_name,
        similarity_search_kwargs={"top_k": 5}
    ).as_retriever()

    return retriever