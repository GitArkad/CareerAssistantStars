import json
import os
import requests
from datetime import datetime

def save_to_staging(data, source_name):
    """Общая функция сохранения в файл"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"/opt/airflow/data/staging/{source_name}_{timestamp}.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return filename

def fetch_hh_ru(limit=20):
    """Адаптер для HeadHunter API"""
    url = "https://api.hh.ru/vacancies"
    params = {"text": "Machine Learning", "per_page": limit}
    res = requests.get(url, params=params).json()
    
    standardized = []
    for item in res.get('items', []):
        standardized.append({
            "title": item.get('name'),
            "url": item.get('alternate_url'),
            "description": item.get('snippet', {}).get('responsibility', ''),
            "salary": (item.get('salary') or {}).get('from'),
            "source": "hh.ru"
        })
    return save_to_staging(standardized, "hh")

def fetch_habr_career(limit=20): ## сложный парсинг через BeautifulSoup
    """Адаптер для Хабр Карьеры (пример структуры)"""
    # Здесь может быть логика парсинга HTML через BeautifulSoup или их API
    # Главное — вернуть такой же список словарей, как в hh_ru
    data = [
        {
            "title": "Middle ML Engineer",
            "url": "https://career.habr.com/vacancies/123",
            "description": "Разработка рекомендательных систем...",
            "salary": 250000,
            "source": "habr.com"
        }
    ]
    return save_to_staging(data, "habr")