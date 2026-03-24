from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
import os

def init_db():
    try:
        client = QdrantClient(
            host=os.getenv("QDRANT_HOST", "qdrant"),  # ВАЖНО
            port=int(os.getenv("QDRANT_PORT", 6333))
        )

        collection_name = "vacancies"

        collections = [c.name for c in client.get_collections().collections]

        if collection_name not in collections:
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=None,  # ⚠️ проверь под свою модель
                    distance=Distance.COSINE
                )
            )
            print("✅ Создана новая коллекция")
        else:
            print("ℹ️ Коллекция уже существует")

    except Exception as e:
        print(f"❌ Ошибка подключения к Qdrant: {e}")


if __name__ == "__main__":
    init_db()