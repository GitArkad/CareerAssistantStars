#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ultimate IT Jobs Parser Pipeline (Global)
Собирает IT-вакансии со всех источников: HH, Adzuna, USAJobs, Greenhouse, Lever, Ashby
Охват: Все страны, все IT-специальности (DS, DE, ML, AI, Analytics, Dev)
Output: PostgreSQL-compatible schema
"""

import requests
import pandas as pd
import time
import os
import json
import re
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from to_postgres import load_to_postgres
from data_cleaner import clean_data
# ============================================================================
# ⚙️ ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ============================================================================

# 📋 ВСЕ IT-СПЕЦИАЛЬНОСТИ ДЛЯ ПОИСКА
IT_SEARCH_QUERIES = [
    # Data Science & ML
    "Data Scientist", "Machine Learning Engineer", "ML Engineer", "AI Engineer",
    "Deep Learning Engineer", "Computer Vision Engineer", "NLP Engineer",
    "Research Scientist", "Applied Scientist", "Ученый по данным",
    
    # Data Engineering
    "Data Engineer", "Big Data Engineer", "ETL Developer", "Data Architect",
    "Analytics Engineer", "Data Platform Engineer", "Инженер данных",
    
    # Analytics & BI
    "Data Analyst", "Business Intelligence Developer", "BI Developer",
    "BI Analyst", "SQL Analyst", "Analytics Manager", "Аналитик данных",
    
    # Development
    "Python Developer", "Python Backend Developer", "Backend Developer",
    "Software Engineer", "Full Stack Developer", "Разработчик Python",
    "SQL Developer", "Database Developer",
    
    # MLOps & Infrastructure
    "MLOps Engineer", "DevOps Engineer", "Platform Engineer",
    "Site Reliability Engineer", "SRE", "Cloud Engineer",
    
    # Management
    "Data Science Manager", "Engineering Manager", "Technical Lead",
    "Head of Data", "CTO", "VP Engineering"
]

# 🌍 СТРАНЫ И РЕГИОНЫ ДЛЯ КАЖДОГО ИСТОЧНИКА
COUNTRIES_CONFIG = {
    "hh.ru": {
        "areas": [
            {"id": 113, "name": "Россия"},
            {"id": 1, "name": "Москва"},
            {"id": 2, "name": "Санкт-Петербург"},
            {"id": 88, "name": "Беларусь"},
            {"id": 160, "name": "Казахстан"},
            {"id": 1005, "name": "Украина"},
            {"id": 1002, "name": "Узбекистан"},
            {"id": 95, "name": "Азербайджан"},
        ]
    },
    "adzuna.com": {
        "countries": [
            {"code": "gb", "name": "United Kingdom"},
            {"code": "de", "name": "Germany"},
            {"code": "fr", "name": "France"},
            {"code": "nl", "name": "Netherlands"},
            {"code": "pl", "name": "Poland"},
            {"code": "ua", "name": "Ukraine"},
            {"code": "kz", "name": "Kazakhstan"},
            {"code": "by", "name": "Belarus"},
            {"code": "ca", "name": "Canada"},
            {"code": "au", "name": "Australia"},
            {"code": "sg", "name": "Singapore"},
            {"code": "in", "name": "India"},
        ]
    },
    "usajobs.gov": {
        "countries": [{"code": "us", "name": "United States"}]
    },
    "greenhouse.com": {
        "countries": ["global"]
    },
    "lever.co": {
        "countries": ["global"]
    },
    "ashbyhq.com": {
        "countries": ["global"]
    }
}

# 📊 ПАРАМЕТРЫ СБОРА
TARGET_COUNT_PER_QUERY = 10000
TARGET_COUNT_PER_COUNTRY = 30000
MAX_TOTAL_VACANCIES = 100000
OUTPUT_DIR = "output_global"
MAIN_OUTPUT_FILE = "it_vacancies_global.csv"

# ============================================================================
# 🧰 УТИЛИТЫ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser_global.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TECH_STACK_KEYWORDS = {
    'python', 'java', 'javascript', 'typescript', 'scala', 'go', 'golang', 'rust',
    'sql', 'postgresql', 'mysql', 'mongodb', 'redis', 'elasticsearch',
    'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'jenkins', 'gitlab', 'github',
    'aws', 'azure', 'gcp', 'cloud', 'linux', 'bash', 'shell',
    'spark', 'kafka', 'airflow', 'dbt', 'snowflake', 'bigquery', 'redshift',
    'pytorch', 'tensorflow', 'sklearn', 'pandas', 'numpy', 'scipy',
    'mlflow', 'dvc', 'wandb', 'fastapi', 'flask', 'django', 'react', 'vue',
    'git', 'jira', 'confluence', 'slack', 'notion', 'figma', 'postman'
}

METHODOLOGIES_KEYWORDS = {
    'agile', 'scrum', 'kanban', 'waterfall', 'devops', 'mlops', 'dataops',
    'tdd', 'bdd', 'ci/cd', 'cicd', 'microservices', 'oop', 'functional'
}

SENIORITY_MAP = {
    'начального уровня': 'junior', 'junior': 'junior', 'no experience': 'junior',
    'среднего уровня': 'middle', 'middle': 'middle', 'mid': 'middle',
    'старшего уровня': 'senior', 'senior': 'senior', 'lead': 'senior',
    'ведущий': 'senior', 'principal': 'senior', 'staff': 'senior',
    'intern': 'junior', 'trainee': 'junior', 'стажер': 'junior',
    'младший': 'junior', 'старший': 'senior', 'главный': 'senior'
}

def generate_ob_id(source: str, url: str, title: str) -> str:
    """Генерация уникального ID вакансии"""
    raw = f"{source}:{url}:{title}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def clean_html(html_text: str) -> str:
    """Очистка HTML-тегов"""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(['script', 'style']):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r'\s+', ' ', text).strip()

def extract_years_from_text(text: str) -> tuple:
    """Извлечение диапазона лет опыта"""
    if not text:
        return None, None
    text_lower = text.lower()
    patterns = [
        r'(\d+)\s*[-–—]\s*(\d+)\s*лет', r'(\d+)\s*[-–—]\s*(\d+)\s*years',
        r'от\s*(\d+)\s*лет', r'from\s*(\d+)\s*years',
        r'до\s*(\d+)\s*лет', r'up to\s*(\d+)\s*years',
        r'(\d+)\+\s*лет', r'(\d+)\+\s*years',
        r'(\d+)\s*год', r'(\d+)\s*year',
        r'(\d+)\s*года?', r'(\d+)\s*years?',
    ]
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                return int(groups[0]), int(groups[1])
            else:
                val = int(groups[0])
                return val, val
    return None, None

def normalize_seniority(text: str) -> str:
    """Нормализация грейда"""
    if not text:
        return "unknown"
    text_lower = text.lower()
    for key, value in SENIORITY_MAP.items():
        if key in text_lower:
            return value
    if any(w in text_lower for w in ['junior', 'jr', 'trainee', 'intern', 'стажер', 'без опыта', 'entry']):
        return 'junior'
    if any(w in text_lower for w in ['senior', 'sr', 'lead', 'principal', 'ведущий', 'главный', 'staff']):
        return 'senior'
    if any(w in text_lower for w in ['middle', 'mid', 'мидл', 'regular']):
        return 'middle'
    return "unknown"

def extract_skills_from_text(text: str) -> List[str]:
    """Извлечение технических навыков"""
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for tech in TECH_STACK_KEYWORDS:
        if re.search(rf'\b{re.escape(tech)}\b', text_lower):
            found.append(tech)
    return list(set(found))

def extract_tools_from_text(text: str) -> List[str]:
    """Извлечение инструментов"""
    if not text:
        return []
    text_lower = text.lower()
    tools = {'git', 'jira', 'confluence', 'docker', 'kubernetes', 'jenkins',
             'gitlab', 'github', 'slack', 'notion', 'figma', 'postman'}
    return [t for t in tools if re.search(rf'\b{re.escape(t)}\b', text_lower)]

def extract_methodologies_from_text(text: str) -> List[str]:
    """Извлечение методологий"""
    if not text:
        return []
    text_lower = text.lower()
    return [m for m in METHODOLOGIES_KEYWORDS if re.search(rf'\b{re.escape(m)}\b', text_lower)]

# ============================================================================
# 🗄️ БАЗОВЫЙ КЛАСС ПАРСЕРА
# ============================================================================

class BaseParser(ABC):
    """Абстрактный базовый класс для всех парсеров"""
    
    def __init__(self, source_name: str, output_file: str = "jobs_output.csv",
                 temp_file: str = "parser_progress.json"):
        self.source_name = source_name
        self.output_file = output_file
        self.temp_file = temp_file
        self.headers = {
            "User-Agent": "JobParserPipeline/3.0 (research)",
            "Accept": "text/html,application/xhtml+xml,application/json"
        }
        self.collected_ids = set()
        self.vacancies_data = []
    
    def load_progress(self):
        """Загрузка прогресса"""
        if os.path.exists(self.temp_file):
            try:
                with open(self.temp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.collected_ids = set(data.get("ids", []))
                    self.vacancies_data = data.get("data", [])
                logger.info(f"✓ {self.source_name}: Восстановлено {len(self.vacancies_data)} записей")
            except Exception as e:
                logger.warning(f"⚠ {self.source_name}: Ошибка загрузки прогресса: {e}")
    def safe_get_dict(value):
        if isinstance(value, dict):
            return value
        return {}
    def safe_get_list(value):
        if isinstance(value, list):
            return value
        return []
    def save_progress(self):
        """Сохранение прогресса"""
        data = {
            "ids": list(self.collected_ids),
            "data": self.vacancies_data,
            "source": self.source_name,
            "timestamp": datetime.now().isoformat()
        }
        with open(self.temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def create_vacancy_record(self, **kwargs) -> Dict[str, Any]:
        """Создание записи вакансии по единой схеме"""
        description = kwargs.get('description', '') or ''
        full_text = f"{kwargs.get('title', '')} {description} {kwargs.get('requirements', '')}"
        
        return {
            "ob_id": kwargs.get('ob_id') or generate_ob_id(
                self.source_name, kwargs.get('url', ''), kwargs.get('title', '')),
            "title": kwargs.get('title'),
            "description": clean_html(kwargs.get('description', '')),
            "requirements": clean_html(kwargs.get('requirements', '')),
            "responsibilities": clean_html(kwargs.get('responsibilities', '')),
            "nice_to_have": clean_html(kwargs.get('nice_to_have', '')),
            "salary_from": kwargs.get('salary_from'),
            "salary_to": kwargs.get('salary_to'),
            "currency": kwargs.get('currency', 'USD'),
            "experience_level": kwargs.get('experience_level'),
            "seniority_normalized": normalize_seniority(kwargs.get('experience_level', '')),
            "years_experience_min": kwargs.get('years_experience_min'),
            "years_experience_max": kwargs.get('years_experience_max'),
            "company_name": kwargs.get('company_name'),
            "industry": kwargs.get('industry'),
            "company_size": kwargs.get('company_size'),
            "key_skills": kwargs.get('key_skills', []),
            "skills_extracted": extract_skills_from_text(full_text),
            "skills_normalized": [],
            "tech_stack_tags": extract_skills_from_text(full_text),
            "tools": extract_tools_from_text(full_text),
            "methodologies": extract_methodologies_from_text(full_text),
            "location": kwargs.get('location'),
            "country": kwargs.get('country'),
            "remote": kwargs.get('remote', False),
            "employment_type": kwargs.get('employment_type'),
            "published_at": kwargs.get('published_at'),
            "source": self.source_name,
            "url": kwargs.get('url'),
            "search_query": kwargs.get('search_query'),
            "parsed_at": datetime.now().isoformat()
        }
    
    @abstractmethod
    def fetch_vacancies(self, keyword: str, target_count: int = 10000,
                       country: str = None) -> List[Dict]:
        """Основной метод сбора вакансий"""
        pass
    
    def export_to_csv(self, append: bool = True):
        """Экспорт в CSV с опцией добавления"""
        if not self.vacancies_data:
            logger.warning(f"{self.source_name}: Нет данных для экспорта")
            return
        
        df = pd.DataFrame(self.vacancies_data)
        
        # Конвертация списков в строки
        for col in ['key_skills', 'skills_extracted', 'skills_normalized',
                    'tech_stack_tags', 'tools', 'methodologies']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: '{{{}}}'.format(','.join(x)) if isinstance(x, list) else x)
        
        # ▼▼▼ ЛОГИКА APPEND ▼▼▼
        file_exists = os.path.exists(self.output_file)
        
        if append and file_exists:
            # Читаем существующие ob_id
            try:
                existing_df = pd.read_csv(self.output_file, sep=',', usecols=['ob_id'])
                existing_ids = set(existing_df['ob_id'].tolist())
                
                # Фильтруем новые вакансии
                new_df = df[~df['ob_id'].isin(existing_ids)]
            
                if not new_df.empty:
                    new_df.to_csv(self.output_file, mode='a', header=not file_exists, 
                                index=False, encoding='utf-8-sig', sep=',')
                    logger.info(f"📥 {self.source_name}: Добавлено {len(new_df)} новых записей")
                else:
                    logger.info(f"⏭️ {self.source_name}: Нет новых записей")
            except Exception as e:
                logger.error(f"❌ Ошибка при добавлении: {e}")
                raise
        else:
            # Новый файл — перезаписываем с заголовками
            df.to_csv(self.output_file, mode='w', header=True,
                    index=False, encoding='utf-8-sig', sep=',')
            logger.info(f"🎉 {self.source_name}: Создан файл с {len(df)} записями")
    
    def run(self, keywords: List[str] = IT_SEARCH_QUERIES,
            target_per_keyword: int = TARGET_COUNT_PER_QUERY,
            countries: List[str] = None):
        """Запуск парсинга по всем запросам и странам"""
        logger.info(f"🚀 {self.source_name}: Запуск сбора")
        logger.info(f"   Запросов: {len(keywords)}, Цель на запрос: {target_per_keyword}")
        
        total_start = len(self.vacancies_data)
        
        for keyword in keywords:
            # ✅ ИСПРАВЛЕНИЕ: Проверка на None
            if MAX_TOTAL_VACANCIES is not None and len(self.vacancies_data) >= MAX_TOTAL_VACANCIES:
                logger.info(f"📊 Достигнут лимит вакансий ({MAX_TOTAL_VACANCIES})")
                break
            
            # ✅ ИСПРАВЛЕНИЕ: \n вместо переноса строки
            logger.info(f"\n📌 Запрос: '{keyword}'")
            
            if countries:
                for country in countries:
                    # ✅ ИСПРАВЛЕНИЕ: Проверка на None
                    if MAX_TOTAL_VACANCIES is not None and len(self.vacancies_data) >= MAX_TOTAL_VACANCIES:
                        break
                    
                    new_vacancies = self.fetch_vacancies(
                        keyword=keyword,
                        target_count=target_per_keyword,
                        country=country
                    )
                    for vac in new_vacancies:
                        if vac['ob_id'] not in self.collected_ids:
                            self.vacancies_data.append(vac)
                            self.collected_ids.add(vac['ob_id'])
                    if len(self.vacancies_data) % 100 == 0:
                        self.save_progress()
                        logger.info(f"💾 Промежуточное сохранение: {len(self.vacancies_data)}")
                    time.sleep(0.5)
            else:
                new_vacancies = self.fetch_vacancies(
                    keyword=keyword,
                    target_count=target_per_keyword
                )
                for vac in new_vacancies:
                    if vac['ob_id'] not in self.collected_ids:
                        self.vacancies_data.append(vac)
                        self.collected_ids.add(vac['ob_id'])
                if len(self.vacancies_data) % 100 == 0:
                    self.save_progress()
                    logger.info(f"💾 Промежуточное сохранение: {len(self.vacancies_data)}")
                time.sleep(0.3)
        
        total_new = len(self.vacancies_data) - total_start
        self.save_progress()
        self.export_to_csv()
        logger.info(f"✅ {self.source_name} завершён. Новых: {total_new}, Всего: {len(self.vacancies_data)}")


# ============================================================================
# 📡 ПАРСЕРЫ: API
# ============================================================================

class HHParser(BaseParser):
    """Парсер HeadHunter API (Россия + СНГ)"""
    
    def __init__(self, **kwargs):
        super().__init__("hh.ru", **kwargs)
        self.base_url = "https://api.hh.ru/vacancies"
        self.areas = COUNTRIES_CONFIG["hh.ru"]["areas"]
    
    def fetch_vacancy_details(self, vid: str, max_retries: int = 3) -> Optional[Dict]:
        """Запрос деталей вакансии с повторными попытками"""
        if vid in self.collected_ids:
            return None
        
        url = f"https://api.hh.ru/vacancies/{vid}"
        
        for attempt in range(max_retries):
            try:
                # ✅ ИСПРАВЛЕНИЕ: timeout увеличен до 30
                resp = requests.get(url, headers=self.headers, timeout=30)
                
                if resp.status_code == 429:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"⚠️ Rate limit HH (429). Ждём {wait_time}с...")
                    time.sleep(wait_time)
                    continue
                
                resp.raise_for_status()
                return resp.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Таймаут (попытка {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(3)
                continue
                
            except Exception as e:
                logger.error(f"Ошибка HH {vid}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                continue
        
        logger.error(f"❌ Не удалось получить вакансию {vid} после {max_retries} попыток")
        return None
    
    def parse_vacancy(self, v: Dict, keyword: str, country_name: str) -> Dict:
        salary = v.get('salary') or {}
        area = v.get('area') or {}
        employer = v.get('employer') or {}
        experience = v.get('experience') or {}
        schedule = v.get('schedule') or {}
        work_mode = v.get('work_mode') or {}
        
        exp_text = experience.get('name', '')
        y_min, y_max = extract_years_from_text(exp_text)
        
        return self.create_vacancy_record(
            title=v.get('name'),
            description=v.get('description'),
            requirements="; ".join([s.get('name') for s in v.get('key_skills', []) or []]),
            company_name=employer.get('name'),
            salary_from=salary.get('from'),
            salary_to=salary.get('to'),
            currency=salary.get('currency'),
            experience_level=exp_text,
            years_experience_min=y_min,
            years_experience_max=y_max,
            location=area.get('name'),
            country=country_name,
            employment_type=(v.get('employment') or {}).get('name'),
            remote=work_mode.get('name') == 'REMOTE' or 'удален' in (schedule.get('name') or '').lower(),
            published_at=v.get('published_at'),
            url=f"https://hh.ru/vacancy/{v.get('id')}",
            key_skills=[s.get('name') for s in v.get('key_skills', []) or []],
            search_query=keyword
        )
    
    def fetch_vacancies(self, keyword: str, target_count: int = 10000,
                       country: str = None) -> List[Dict]:
        """Сбор вакансий с поддержкой безлимита"""
        results = []
        page = 0
        per_page = 100
        
        # Определяем region для поиска
        area_id = 113
        country_name = "Россия"
        if country:
            for area in self.areas:
                if area['name'] == country:
                    area_id = area['id']
                    country_name = area['name']
                    break
        
        # ✅ ИСПРАВЛЕНИЕ: поддержка None для target_count
        while target_count is None or len(results) < target_count:
            params = {
                "text": keyword,
                "page": page,
                "per_page": per_page,
                "area": area_id,
                "order_by": "publication_time"
            }
            
            try:
                resp = requests.get(self.base_url, headers=self.headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                items = data.get('items', [])
                
                if not items:
                    logger.info(f"HH: Вакансии по запросу '{keyword}' закончились на странице {page}")
                    break
                
                logger.info(f"HH: страница {page+1}, найдено {len(items)} вакансий")
                
                for item in items:
                    # ✅ ИСПРАВЛЕНИЕ: проверка лимита
                    if target_count is not None and len(results) >= target_count:
                        break
                    
                    vid = item.get('id')
                    details = self.fetch_vacancy_details(vid)
                    
                    if details:
                        # ✅ ИСПРАВЛЕНИЕ: передача keyword и country_name
                        results.append(self.parse_vacancy(details, keyword, country_name))
                    
                    time.sleep(0.3)
                
                page += 1
                
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Таймаут запроса (страница {page}), пробуем ещё раз...")
                time.sleep(3)
                continue
            except Exception as e:
                logger.error(f"HH ошибка: {e}")
                time.sleep(5)
        
        logger.info(f"✅ По запросу '{keyword}' собрано {len(results)} вакансий")
        return results


class AdzunaParser(BaseParser):
    """Парсер Adzuna API (Глобальный) — ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ"""
    
    def __init__(self, app_id: str, app_key: str, **kwargs):
        super().__init__("adzuna.com", **kwargs)
        self.app_id = app_id
        self.app_key = app_key
        # ✅ Правильные заголовки для Adzuna API
        self.headers = {
            "User-Agent": "JobParserPipeline/3.0 (research)",
            "Accept": "application/json"
        }
        # ✅ Только страны, подтверждённо работающие с бесплатным тарифом
        self.supported_countries = [
            {"code": "gb", "name": "United Kingdom"},
        ]
    
    def fetch_vacancies(self, keyword: str, target_count: int = 10000,
                   country: str = None) -> List[Dict]:

        if not self.app_id or not self.app_key:
            logger.error("Adzuna: укажите app_id и app_key")
            return []

        results = []

        countries = [{"code": "gb", "name": "United Kingdom"}]

        for country_info in countries:
            country_code = country_info["code"]
            country_name = country_info["name"]

            page = 1  # 🔥 всегда с 1

            while len(results) < target_count:
                url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/{page}"

                params = {
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "what": keyword,
                    "results_per_page": 20
                }

                try:
                    resp = requests.get(url, params=params, headers=self.headers, timeout=15)

                    logger.debug(f"Adzuna URL: {resp.url}")

                    # ===== ОШИБКИ =====
                    if resp.status_code == 429:
                        logger.warning("⏳ Rate limit Adzuna → sleep 5s")
                        time.sleep(5)
                        continue

                    if resp.status_code == 400:
                        logger.error(f"❌ Adzuna 400: {resp.url}")
                        logger.error(resp.text[:300])
                        break

                    if resp.status_code != 200:
                        logger.warning(f"⚠️ Adzuna HTTP {resp.status_code}")
                        break

                    data = resp.json()
                    items = data.get("results", [])

                    if not items:
                        logger.info("📭 Adzuna: вакансии закончились")
                        break

                    for item in items:
                        if len(results) >= target_count:
                            break

                        results.append(self.create_vacancy_record(
                            title=item.get("title"),
                            description=item.get("description"),
                            company_name=item.get("company", {}).get("display_name"),
                            salary_from=item.get("salary_min"),
                            salary_to=item.get("salary_max"),
                            currency=item.get("salary_currency", "USD"),  # ✅ фикс
                            location=item.get("location", {}).get("display_name"),
                            country=country_name,
                            employment_type=item.get("contract_time"),
                            remote="remote" in (item.get("description", "").lower()),
                            published_at=item.get("created"),
                            url=item.get("redirect_url"),
                            key_skills=[],
                            search_query=keyword
                        ))

                    logger.info(f"Adzuna page {page} → {len(results)}")

                    page += 1
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"❌ Adzuna error: {e}")
                    time.sleep(3)

        logger.info(f"✅ Adzuna собрано: {len(results)}")
        return results
    
class USAJobsParser(BaseParser):
    def __init__(self, api_key: str, email: str, **kwargs):
        super().__init__("usajobs", **kwargs)

        self.api_key = api_key
        self.email = email
        self.query_progress = {}

        self.headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": email,
            "Authorization-Key": api_key
        }

    # =========================
    # 🧠 SAFE HELPERS
    # =========================
    def _safe_dict(self, val):
        return val if isinstance(val, dict) else {}

    def _safe_list(self, val):
        return val if isinstance(val, list) else []

    # =========================
    # 🚀 MAIN FETCH
    # =========================
    def fetch(self, keyword: str, target: int = 500):

        page = self.query_progress.get(keyword, 1)

        logger.info(f"🇺🇸 USAJobs: {keyword} starting from page {page}")

        while len(self.vacancies_data) < target:

            url = "https://data.usajobs.gov/api/Search"

            params = {
                "Keyword": keyword,
                "Page": page,
                "ResultsPerPage": 25
            }

            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=20
                )

                # ===== RATE LIMIT =====
                if response.status_code == 429:
                    logger.warning("⏳ USAJobs rate limit → sleep 5s")
                    time.sleep(5)
                    continue

                if response.status_code != 200:
                    logger.error(f"❌ USAJobs HTTP {response.status_code}")
                    logger.error(response.text[:300])
                    break

                data = response.json()

                search_result = self._safe_dict(data.get("SearchResult"))
                items = self._safe_list(search_result.get("SearchResultItems"))

                if not items:
                    logger.info("📭 USAJobs: вакансии закончились")
                    break

                for item in items:
                    try:
                        job = self._safe_dict(item.get("MatchedObjectDescriptor"))

                        if not job:
                            continue

                        user_area = self._safe_dict(job.get("UserArea"))
                        details = self._safe_dict(user_area.get("Details"))

                        salary = self._safe_dict(details.get("SalaryInfo"))

                        locations = self._safe_list(job.get("PositionLocation"))

                        location = None
                        if locations:
                            loc0 = self._safe_dict(locations[0])
                            location = loc0.get("CityName")

                        # employment type
                        schedule = self._safe_dict(job.get("PositionSchedule"))
                        employment_type = schedule.get("Name")

                        # remote detection
                        description_text = str(details.get("JobSummary", "")).lower()
                        remote_flag = any(
                            word in description_text
                            for word in ["remote", "telework", "work from home"]
                        )

                        vac = self.create_vacancy_record(
                            title=job.get("PositionTitle"),
                            description=details.get("JobSummary"),
                            company_name=job.get("OrganizationName"),
                            salary_from=salary.get("MinimumRange"),
                            salary_to=salary.get("MaximumRange"),
                            currency=salary.get("CurrencyCode", "USD"),
                            experience_level=str(job.get("JobGrade", {})),
                            location=location,
                            employment_type=employment_type,
                            remote=remote_flag,
                            published_at=job.get("PublicationDate"),
                            url=job.get("PositionURI"),
                            source="usajobs"
                        )
                        self.vacancies_data.append(vac)
                    except Exception as item_error:
                        logger.error(f"⚠️ USAJobs item error: {item_error}")
                        continue

                logger.info(f"USAJobs {keyword} → page {page} parsed")

                # ✅ обновляем прогресс
                page += 1
                self.query_progress[keyword] = page

                # ✅ сохраняем после каждой страницы
                self.save_progress()

                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ USAJobs критическая ошибка: {e}")
                time.sleep(3)

        logger.info(f"✅ USAJobs завершён: {len(self.vacancies_data)} вакансий")
        
    def fetch_vacancies(self, keyword: str, target_count: int = 10000, country: str = None):
        before = len(self.vacancies_data)
        self.fetch(keyword, target_count)
        return self.vacancies_data[before:]
# ============================================================================
# 🕷️ ПАРСЕРЫ: SCRAPING
# ============================================================================

class GenericATSScraper(BaseParser):
    """Базовый скрапер для ATS систем"""
    
    def __init__(self, source_name: str, base_url: str, **kwargs):
        super().__init__(source_name, **kwargs)
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            return None
    
    def fetch_vacancies(self, keyword: str, target_count: int = 10000,
                       country: str = None) -> List[Dict]:
        logger.warning(f"{self.source_name}: Scraping требует индивидуальной настройки")
        return []


class GreenhouseParser(GenericATSScraper):
    def __init__(self, **kwargs):
        super().__init__("greenhouse.com", "https://www.greenhouse.com", **kwargs)


class LeverParser(GenericATSScraper):
    def __init__(self, **kwargs):
        super().__init__("lever.co", "https://www.lever.co", **kwargs)


class AshbyParser(GenericATSScraper):
    def __init__(self, **kwargs):
        super().__init__("ashbyhq.com", "https://www.ashbyhq.com", **kwargs)


# ============================================================================
# 🔄 ОРКЕСТРАТОР
# ============================================================================

class JobParserPipeline:
    """Оркестратор для запуска всех парсеров"""
    
    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.parsers = []
        self.global_collected_ids = set()
        self.global_vacancies = []
        self.load_global_progress()
    
    def load_global_progress(self):
        global_temp = os.path.join(self.output_dir, "global_progress.json")
        if os.path.exists(global_temp):
            try:
                with open(global_temp, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.global_collected_ids = set(data.get("ids", []))
                    self.global_vacancies = data.get("data", [])
                logger.info(f"✓ Глобально восстановлено {len(self.global_vacancies)} вакансий")
            except:
                pass
    
    def save_global_progress(self):
        global_temp = os.path.join(self.output_dir, "global_progress.json")
        data = {
            "ids": list(self.global_collected_ids),
            "data": self.global_vacancies,
            "timestamp": datetime.now().isoformat()
        }
        with open(global_temp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def add_parser(self, parser: BaseParser):
        self.parsers.append(parser)
    
    def run_all(self, keywords: List[str] = IT_SEARCH_QUERIES,
                target_per_source: int = 10000):
        for parser in self.parsers:
            try:
                # ▼▼▼ ШАГ 1: Устанавливаем пути ▼▼▼
                parser.output_file = os.path.join(
                    self.output_dir,
                    f"{parser.source_name.replace('.', '_')}_jobs.csv"
                )
                parser.temp_file = os.path.join(
                    self.output_dir,
                    f"{parser.source_name.replace('.', '_')}_progress.json"
                )
                
                # ▼▼▼ ШАГ 2: Загружаем прогресс из ПРАВИЛЬНОГО файла ▼▼▼
                parser.load_progress()  # ← ← ← ДОБАВИТЬ ЭТУ СТРОКУ!
                logger.info(f"✓ {parser.source_name}: Загружено {len(parser.vacancies_data)} из {parser.temp_file}")
                
                # ▼▼▼ ШАГ 3: Запускаем парсинг ▼▼▼
                parser.run(
                    keywords=keywords,
                    target_per_keyword=target_per_source // len(keywords)
                )
                for vac in parser.vacancies_data:
                    if vac['ob_id'] not in self.global_collected_ids:
                        self.global_vacancies.append(vac)
                        self.global_collected_ids.add(vac['ob_id'])
                
                self.save_global_progress()
                # ✅ ИСПРАВЛЕНИЕ: \n вместо переноса строки
                logger.info(f"📊 Глобально собрано: {len(self.global_vacancies)} уникальных вакансий\n")
                
            except Exception as e:
                logger.error(f"❌ Ошибка в {parser.source_name}: {e}")
                continue
        
        self.merge_results()
    
    def merge_results(self):
        """Объединение всех результатов"""
        all_files = [f for f in os.listdir(self.output_dir) if f.endswith('_jobs.csv')]
        
        dfs = []
        for f in all_files:
            try:
                df = pd.read_csv(os.path.join(self.output_dir, f), sep=',')
                dfs.append(df)
            except:
                continue
        
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            combined.drop_duplicates(subset=['ob_id'], inplace=True)
            
            if 'published_at' in combined.columns:
                combined.sort_values('published_at', ascending=False, inplace=True)
            
            output_path = os.path.join(self.output_dir, MAIN_OUTPUT_FILE)
            combined.to_csv(output_path, index=False, encoding='utf-8-sig', sep=',')
            
            # ✅ ИСПРАВЛЕНИЕ: \n вместо переноса строки
            logger.info(f"\n{'='*60}")
            logger.info(f"📦 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ")
            logger.info(f"   Уникальных вакансий: {len(combined)}")
            logger.info(f"   Источников: {len(all_files)}")
            logger.info(f"   Файл: {output_path}")
            logger.info(f"{'='*60}\n")


# ============================================================================
# 🎯 ТОЧКА ВХОДА
# ============================================================================

def main():
    """Пример использования"""
    
    ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
    ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
    USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY")
    
    pipeline = JobParserPipeline(output_dir=OUTPUT_DIR)
    
    pipeline.add_parser(HHParser(output_file="hh_jobs.csv"))
    
    if ADZUNA_APP_ID and ADZUNA_APP_KEY:
        pipeline.add_parser(AdzunaParser(
            app_id=ADZUNA_APP_ID,
            app_key=ADZUNA_APP_KEY,
            output_file="adzuna_jobs.csv"
        ))
    
    if USAJOBS_API_KEY:
        pipeline.add_parser(USAJobsParser(
            api_key=USAJOBS_API_KEY,
            email="EMAIL_YOUR",
            output_file="usajobs_jobs.csv"
        ))
    
    pipeline.run_all(
        keywords=IT_SEARCH_QUERIES,
        target_per_source=10000
    )


#for data cleaner
raw_path = OUTPUT_DIR + MAIN_OUTPUT_FILE
clean_path = OUTPUT_DIR + "it_vacancies_clean.csv"

clean_data(
    input_path=raw_path,
    output_path=clean_path
)
#for to_postgres
load_to_postgres(
    csv_path=OUTPUT_DIR + "it_vacancies_clean.csv",
    table_name="jobs"
)

if __name__ == "__main__":
    main()