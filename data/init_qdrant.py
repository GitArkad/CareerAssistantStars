import uuid
import os
from typing import Optional, List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http import models

# 1. Инициализация клиента
# Если запускаешь в Docker, замени localhost на имя контейнера qdrant
client = QdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
COLLECTION_NAME = "vacancies"

# Используем модель, которая отлично справляется с мультиязычностью (RU/EN)
client.set_model("intfloat/multilingual-e5-large")

def generate_deterministic_uuid(company: str, title: str) -> str:
    """
    Создает постоянный UUID на основе пары Компания + Название вакансии.
    Это гарантирует, что при повторном запуске скрипта записи будут обновляться, 
    а не дублироваться.
    """
    namespace = uuid.NAMESPACE_DNS
    comp = (company or "unknown").lower()
    pos = (title or "position").lower()
    return str(uuid.uuid5(namespace, f"{comp}_{pos}"))

def init_db_uuid():
    print(f"⏳ Проверка и пересоздание коллекции '{COLLECTION_NAME}'...")
    
    if client.collection_exists(COLLECTION_NAME):
        client.delete_collection(COLLECTION_NAME)

    client.create_collection(
        collection_name=COLLECTION_NAME,
        # get_fastembed_vector_params автоматически подтянет размерность для E5 (1024)
        vectors_config=client.get_fastembed_vector_params(),
    )

    # 2. Полный список вакансий с полем 'country' и новым полем 'url'
    raw_vacancies: List[Dict[str, Any]] = [
        {
            "title": "Junior ML Engineer", "company": "SberAI", "city": "Moscow", "country": "Russia",
            "work_format": "Office", "relocation": False, "grade": "Junior",
            "salary_from": 150000, "salary_to": 200000, "currency": "RUB", 
            "skills": ["Python", "PyTorch", "SQL"],
            "description": "Разработка моделей в московском офисе. Удаленки нет.",
            "url": "https://career.sber.ru/vacancies/sberai-jr-ml"
        },
        {
            "title": "AI Developer", "company": "Global Tech", "city": "Remote", "country": "Germany",
            "work_format": "Remote", "relocation": True, "grade": "Middle",
            "salary_from": 4500, "salary_to": None, "currency": "USD", 
            "skills": ["Python", "Docker"],
            "description": "Full remote. We help with relocation to EU countries.",
            "url": "https://globaltech.com/careers/ai-dev-remote"
        },
        {
            "title": "ML Разработчик (NLP)", "company": "T-Bank", "city": "Saint-Petersburg", "country": "Russia",
            "work_format": "Hybrid", "relocation": True, "grade": "Middle",
            "salary_from": None, "salary_to": None, "currency": "RUB", 
            "skills": ["Python", "Transformers"],
            "description": "Гибридный график (2-3 дня в офисе). Помогаем с переездом.",
            "url": "https://tbank.ru/career/it/ml-nlp"
        },
        {
            "title": "Data Scientist", "company": "Ozon", "city": "Moscow", "country": "Russia",
            "work_format": "Hybrid", "relocation": False, "grade": "Middle", 
            "salary_from": 170000, "salary_to": 250000, "currency": "RUB", 
            "skills": ["SQL", "Pandas", "CatBoost"],
            "description": "Анализ данных для маркетплейса. Гибрид в Москве.",
            "url": "https://job.ozon.ru/vacancy/ds-middle"
        },
        {
            "title": "NLP Researcher", "company": "OpenAI Partner", "city": "San Francisco", "country": "USA",
            "work_format": "Office", "relocation": True, "grade": "Senior",
            "salary_from": 5500, "salary_to": 8500, "currency": "USD", 
            "skills": ["Python", "LLM", "LangChain"],
            "description": "Research and development of agentic workflows.",
            "url": "https://openaipartner.ai/jobs/nlp-senior-researcher"
        },
        {
            "title": "Computer Vision Engineer", "company": "DeepVision", "city": "Belgrade", "country": "Serbia",
            "work_format": "Office", "relocation": True, "grade": "Middle",
            "salary_from": 3000, "salary_to": 4500, "currency": "EUR", 
            "skills": ["OpenCV", "C++", "PyTorch"],
            "description": "Разработка систем распознавания лиц. Релокационный пакет.",
            "url": "https://deepvision.rs/career/cv-engineer"
        },
        {
            "title": "MLOps Engineer", "company": "Fintech Global", "city": "Astana", "country": "Kazakhstan",
            "work_format": "Hybrid", "relocation": False, "grade": "Middle",
            "salary_from": 2500, "salary_to": 3500, "currency": "USD", 
            "skills": ["Kubernetes", "MLflow", "Python"],
            "description": "Поддержка жизненного цикла моделей. Офис в центре Астаны.",
            "url": "https://fintechglobal.kz/jobs/mlops"
        },
        {
            "title": "Junior Data Analyst", "company": "Retail Group", "city": "Minsk", "country": "Belarus",
            "work_format": "Office", "relocation": False, "grade": "Junior",
            "salary_from": 1000, "salary_to": 1500, "currency": "USD", 
            "skills": ["SQL", "Tableau", "Excel"],
            "description": "Аналитика продаж. Отличный старт для карьеры.",
            "url": "https://retailgroup.by/vacancies/jr-data-analyst"
        },
        {
            "title": "Lead AI Architect", "company": "Neural Systems", "city": "Limassol", "country": "Cyprus",
            "work_format": "Remote", "relocation": True, "grade": "Senior",
            "salary_from": 6000, "salary_to": 9000, "currency": "EUR", 
            "skills": ["PyTorch", "System Design", "Cloud Architecture"],
            "description": "Проектирование архитектуры ИИ-продуктов. Налоговые льготы Кипра.",
            "url": "https://neuralsystems.cy/careers/lead-ai-arch"
        }
    ]

    documents = []
    metadata = []
    ids = []

    # 3. Подготовка данных для загрузки
    for v in raw_vacancies:
        # Генерация уникального ID
        u_id = generate_deterministic_uuid(v.get('company'), v.get('title'))
        ids.append(u_id)
        
        # Обогащаем текстовое описание для более точного векторного поиска
        grade_info = v.get('grade') or "Specialist"
        country_info = f"in {v['country']}" if v.get('country') else ""
        city_info = f"({v['city']})" if v.get('city') else ""
        format_info = f"Format: {v['work_format']}" if v.get('work_format') else ""
        
        # Формируем строку с префиксом 'passage:', как того требует модель E5
        text_content = (
            f"passage: {grade_info} {v.get('title', 'Position')} at {v.get('company', 'Company')} "
            f"{country_info} {city_info}. {format_info}. "
            f"Skills: {', '.join(v.get('skills', []))}. {v.get('description', '')}"
        ).strip()
        
        documents.append(text_content)
        metadata.append(v)

    # 4. Загрузка в базу
    print(f"🚀 Выполняется эмбеддинг и загрузка {len(ids)} вакансий в Qdrant...")
    
    # Метод add автоматически вызывает FastEmbed для создания векторов
    client.add(
        collection_name=COLLECTION_NAME,
        documents=documents,
        metadata=metadata,
        ids=ids
    )
    
    print("✅ База данных успешно инициализирована и готова к работе!")

if __name__ == "__main__":
    init_db_uuid()