from fastapi import APIRouter
from pydantic import BaseModel
from typing import List
from app.services.langgraph_service import run_analysis

router = APIRouter()


# ================================
# МОДЕЛЬ ПРОФИЛЯ КАНДИДАТА
# ================================
# Этот формат ты уже получил от парсера резюме / LangGraph команды.
class CandidateProfile(BaseModel):
    name: str
    country: str
    city: str
    relocation: bool
    grade: str  # Junior | Middle | Senior
    specialization: str
    experience_years: float
    desired_salary: float
    work_format: List[str]
    foreign_languages: List[str]
    skills: List[str]


# ================================
# АНАЛИЗ ПРОФИЛЯ КАНДИДАТА
# ================================
# FastAPI здесь не делает AI-логику.
# Он только принимает нормализованный профиль и передаёт его в сервисный слой.
@router.post("/resume")
def analyze_resume(profile: CandidateProfile):
    result = run_analysis(profile.model_dump())
    return result