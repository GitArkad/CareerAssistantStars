from qdrant_client import QdrantClient

client = QdrantClient(
    url="http://3.99.208.202:6333"
)

COLLECTION_NAME = "vacancies"