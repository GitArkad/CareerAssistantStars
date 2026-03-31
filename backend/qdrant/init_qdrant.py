from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
import os
import time

def get_qdrant_client(host="qdrant", port=6333, retries=20, delay=3):
    last_error = None
    for attempt in range(retries):
        try:
            client = QdrantClient(host=host, port=port)
            client.get_collections()
            print(f"✅ Qdrant доступен (attempt {attempt+1})")
            return client
        except Exception as e:
            last_error = e
            print(f"⏳ Ждём Qdrant... попытка {attempt+1}/{retries}")
            time.sleep(delay)

    raise RuntimeError(f"❌ Qdrant is not available: {last_error}")


def init_db():
    try:
        client = get_qdrant_client(
            host=os.getenv("QDRANT_HOST", "qdrant"),
            port=int(os.getenv("QDRANT_PORT", 6333))
        )

        collection_name = "vacancies"

        collections = [c.name for c in client.get_collections().collections]
        print(f"📦 Коллекции: {collections}")

        if collection_name not in collections:
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=None,  # 👈 ОБЯЗАТЕЛЬНО
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