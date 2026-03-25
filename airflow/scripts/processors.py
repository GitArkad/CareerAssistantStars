import json
import hashlib
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
from langchain_openai import OpenAIEmbeddings

def process_multi_source_data(file_paths):
    client = QdrantClient(host="qdrant", port=6333)
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small") # --- ОПРЕДЕЛИТЬ КАКУЮ МОДЕЛЬ ИСПОЛЬЗУЕМ
    
    all_jobs = []
    for path in file_paths:
        if path and os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                all_jobs.extend(json.load(f))

    # Дедупликация по URL (если вакансия на нескольких сайтах)
    unique_jobs = {job['url']: job for job in all_jobs}
    
    points = []
    for url, job in unique_jobs.items():
        # Генерация ID из URL (всегда одинаковый для одной и той же ссылки)
        point_id = hashlib.md5(url.encode()).hexdigest()
        
        # Эмбеддинг (Title + Description)
        text_vector = embeddings.embed_query(f"{job['title']}. {job['description']}")
        
        points.append(PointStruct(
            id=point_id,
            vector=text_vector,
            payload={
                "title": job['title'],
                "url": url,
                "salary": job['salary'],
                "source": job['source'] # Теперь мы видим, откуда вакансия
            }
        ))

    if points:
        client.upsert(collection_name="vacancies", points=points)
        return f"База обновлена: {len(points)} вакансий из разных источников."
    return "Нет данных для загрузки."