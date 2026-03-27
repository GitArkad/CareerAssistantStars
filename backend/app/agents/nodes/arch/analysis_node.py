import os
from dotenv import load_dotenv
import numpy as np
from collections import Counter
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client import models
from app.agents.state import AgentState
from app.agents.services.qdrant_service import get_geo_filters


load_dotenv()

# Настройки
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "vacancies")

# Инициализируем клиента и модель ОДИН РАЗ при запуске модуля
client = QdrantClient(url=QDRANT_URL)
# ВАЖНО: модель та же, что была при индексации
client.set_model(os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-large"))

def clean_skills(raw_data):
    if isinstance(raw_data, list):
        return [s.lower().strip() for s in raw_data if s]
    if isinstance(raw_data, str):
        # Очистка строки от лишних символов, которые могут прийти из LLM или CSV
        clean_str = raw_data.replace("{", "").replace("}", "").replace('"', "").replace("'", "")
        return [s.lower().strip() for s in clean_str.split(",") if s.strip()]
    return []

def analysis_node(state: AgentState):
    print("\n--- [START] ANALYSIS: Qdrant Retrieval & Stats ---")
    
    candidate = state.get("candidate", {})
    
    user_skills_list = candidate.get("skills", [])
    user_skills_set = set(s.lower().strip() for s in user_skills_list)
    # user_grade = candidate.get("grade")
    user_salary = candidate.get("desired_salary")
    
    # 1. Жесткие фильтры
    must_filters = []
    
    # if user_grade:
    #     must_filters.append(models.FieldCondition(key="grade", match=models.MatchValue(value=user_grade)))
    
    # 1. Получаем список условий на основе города/страны/переезда
    must_filters = get_geo_filters(candidate)
    print("********************")
    print(candidate)
    print(must_filters)
    print("********************")
    
    # Фильтр по зарплате (desired_salary кандидата <= salary_to вакансии или salary_to пусто)
    if user_salary:
        must_filters.append(
            models.Filter(should=[
                models.FieldCondition(key="salary_to", range=models.Range(gte=user_salary)),
                models.IsEmptyCondition(is_empty=models.PayloadField(key="salary_to"))
            ])
        )

    # 2. Векторный поиск
    # Добавляем префикс "query: " для модели E5 (важно для точности)
    query_text = f"query: {candidate.get('specialization', 'ML Engineer')}. Skills: {', '.join(user_skills_list)}"
    
    search_results = []
    try:
        # Пытаемся использовать метод .query (современный стандарт для FastEmbed)
        results = client.query(
            collection_name=COLLECTION_NAME,
            query_text=query_text,
            query_filter=models.Filter(must=must_filters) if must_filters else None,
            limit=10
        )
        search_results = results
    except Exception as e:
        print(f"⚠️ Ошибка метода .query: {e}. Пробуем .search...")
        try:
            # Откат к .search, если .query не поддерживается или настроен иначе
            # Генерируем вектор явно через модель клиента
            vector = client.embed_query(query_text)[0]
            search_results = client.search(
                collection_name=COLLECTION_NAME,
                query_vector=vector,
                query_filter=models.Filter(must=must_filters) if must_filters else None,
                limit=10,
                with_payload=True
            )
        except Exception as e2:
            print(f"Критическая ошибка Qdrant: {e2}")
            return {"error": str(e2), "next_step": "end"}

    # 3. Сбор данных для аналитики
    top_vacancies = []
    gap_counter = Counter()
    salaries = []

    for hit in search_results:
        # В новых версиях (QueryResponse) данные могут быть в metadata или payload
        # Используем getattr для безопасного извлечения
        vac = getattr(hit, 'payload', None) or getattr(hit, 'metadata', None)
        
        # Если это старый формат ScoredPoint, данные будут в .payload
        if vac is None:
            continue

        vac_skills = clean_skills(vac.get("skills") or [])
        
        # Собираем зарплаты (используем salary_from для статистики)
        if vac.get("salary_from"):
            try:
                salaries.append(float(vac["salary_from"]))
            except (ValueError, TypeError):
                pass

        # Формируем структуру вакансии для стейта
        top_vacancies.append({
            "id": str(getattr(hit, 'id', 'N/A')),
            "title": vac.get("title", "N/A"),
            "company": vac.get("company", "N/A"),
            "city": vac.get("city", "N/A"),
            "match_score": round(getattr(hit, 'score', 0) * 100, 1),
            "salary_from": vac.get("salary_from"),
            "salary_to": vac.get("salary_to"),
            "skills": vac_skills
        })
        
        # Считаем отсутствующие навыки (Gaps)
        for s in vac_skills:
            if s not in user_skills_set:
                gap_counter[s] += 1

    # 4. Расчет зарплатных метрик
    sal_np = np.array(salaries) if salaries else np.array([0])
    
    market_context = {
        "top_vacancies": top_vacancies[:5],
        "match_score": round(float(np.mean([v["match_score"] for v in top_vacancies[:3]])), 1) if top_vacancies else 0.0,
        "skill_gaps": [skill for skill, count in gap_counter.most_common(10)],
        "salary_median": int(np.median(sal_np)),
        "salary_top_10": int(np.percentile(sal_np, 90)) if salaries else 0,
        "market_range": [int(np.min(sal_np)), int(np.max(sal_np))] if salaries else [0, 0]
    }

    print(f"Анализ завершен. Найдено вакансий: {len(top_vacancies)}")
    print(market_context)
    
    return {
        "market": market_context,
        "next_step": "strategy"
    }