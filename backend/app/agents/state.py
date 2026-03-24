from typing import Annotated, List, Optional, TypedDict, Dict, Any
import operator

# 1. ПОЛНЫЙ ПРОФИЛЬ КАНДИДАТА (10+ полей из твоего списка)
class CandidateProfile(TypedDict):
    # Личные данные и локация
    name: str
    country: Optional[str]
    city: Optional[str]
    relocation: bool
    
    # Профессиональный профиль
    grade: str              # Junior, Middle, Senior, Lead
    specialization: str     # Например: Machine Learning Engineer
    experience_years: float
    desired_salary: Optional[int]
    
    # Списки (форматы и языки)
    work_format: List[str]       # Remote, Hybrid, Office
    foreign_languages: List[str] # English B2, etc.
    
    # ОБЪЕДИНЕННЫЙ СТЕК (Весь технический винегрет тут)
    skills: List[str]            # Python, PyTorch, Airflow, SQL, Docker...

# 2. АНАЛИТИКА + ПОИСК В QDRANT (RAG Context)
class MarketContext(TypedDict):
    match_score: float             # Косинусное сходство или кастомный скоринг
    skill_gaps: List[str]          # Чего не хватает из ТОП-вакансий
    
    # Список найденных вакансий (уже не просто ID, а объекты с метаданными)
    top_vacancies: List[Dict[str, Any]] 
    
    # Зарплатная аналитика
    salary_median: int             # P50 (Медиана)
    salary_top_10: int             # P90 (Топ рынка)
    market_range: List[int]        # [min, max] по найденным вакансиям

# 3. ГЛОБАЛЬНОЕ СОСТОЯНИЕ ГРАФА
class AgentState(TypedDict):
    # Входные данные (сырые)
    raw_file_content: bytes        # Содержимое PDF/Docx
    file_name: str
    
    # 1. Профиль (заполняется узлом Ingestion)
    candidate: CandidateProfile
    
    # 2. Рыночный срез (заполняется узлом Analysis через Qdrant)
    market: MarketContext
    
    # Управление логикой
    next_step: str                 # "analysis", "salary_calc", "strategy", "end"
    error: Optional[str]           # Сюда пишем ошибки, если LLM или Qdrant упали
    
    # Накопительный список логов и инсайтов
    messages: Annotated[List[str], operator.add]