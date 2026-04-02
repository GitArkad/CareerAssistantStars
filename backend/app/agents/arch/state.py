# state.py
from typing import Annotated, List, Optional, TypedDict, Dict, Any, Sequence
import operator
from langchain_core.messages import BaseMessage

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
# Входные данные
    raw_file_content: bytes
    file_name: str
    
    # Основные структуры
    candidate: CandidateProfile
    market: MarketContext
    
    # Текстовый обзор (Summary)
    summary: Optional[str]         
    
    # Данные сервисов
    user_input: Optional[str]      
    interview: Optional[Dict[str, Any]]
    roadmap: Optional[str]
    tailored_resume: Optional[str]  

    # Логика графа
    next_step: str                 
    error: Optional[str]    
           
    # messages: Annotated[List[str], operator.add]
    messages: Annotated[Sequence[BaseMessage], operator.add]

    stage: str # этап работы

