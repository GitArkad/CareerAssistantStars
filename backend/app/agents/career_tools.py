import os
from typing import List, Dict, Any, Optional
from langchain.tools import tool
from pydantic import BaseModel, Field
from groq import Groq

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# 1. СХЕМЫ ДЛЯ ВАЛИДАЦИИ (Чтобы Ллама не тупила с аргументами)
class RoadmapInput(BaseModel):
    specialization: str = Field(description="Специализация (напр. ML Engineer)")
    gaps: List[str] = Field(description="Список отсутствующих навыков из MarketContext")
    experience_years: float = Field(description="Стаж кандидата из профиля")

class ResumeInput(BaseModel):
    specialization: str = Field(description="Профессия")
    current_skills: List[str] = Field(description="Текущий стек кандидата")
    target_vacancies: List[Dict[str, Any]] = Field(description="Топ вакансий из Qdrant")

class InterviewInput(BaseModel):
    specialization: str = Field(description="Позиция")
    gaps: List[str] = Field(description="Пробелы в знаниях для вопросов")

# 2. ИНСТРУМЕНТЫ
@tool("generate_roadmap", args_schema=RoadmapInput)
def generate_roadmap(specialization: str, gaps: List[str], experience_years: float) -> str:
    """Генерирует план обучения на основе пробелов (Skill Gaps)."""
    prompt = f"Ты ментор для {specialization} ({experience_years} лет опыта). Составь Roadmap по темам: {', '.join(gaps)}."
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

@tool("improve_resume", args_schema=ResumeInput)
def improve_resume(specialization: str, current_skills: List[str], target_vacancies: List[Dict[str, Any]]) -> str:
    """Адаптирует резюме под конкретные вакансии из поиска."""
    v_list = "\n".join([f"- {v.get('title')} в {v.get('company')}" for v in target_vacancies[:3]])
    prompt = f"Адаптируй резюме {specialization}. Стек: {current_skills}. Вакансии:\n{v_list}"
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

@tool("start_mock_interview", args_schema=InterviewInput)
def start_mock_interview(specialization: str, gaps: List[str]) -> str:
    """Создает вопросы для интервью по самым слабым местам кандидата."""
    prompt = f"Проведи интервью для {specialization}. Спроси по темам: {', '.join(gaps)}."
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )
    return f"### 🎤 Тренировка интервью\n\n{response.choices[0].message.content}"

career_tools_list = [generate_roadmap, improve_resume, start_mock_interview]