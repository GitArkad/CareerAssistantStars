from typing import List, Optional
from langchain.tools import tool
from pydantic import BaseModel, Field
import json

class RoadmapInput(BaseModel):
    gaps: str = Field(description="Пробелы в навыках через запятую")
    current_salary: int = Field(description="Текущая зарплата в рублях")

class ResumeInput(BaseModel):
    vacancy_title: str = Field(description="Название вакансии")
    company: str = Field(description="Название компании")

class InterviewInput(BaseModel):
    topics: str = Field(description="Темы для вопросов через запятую")

class VacancySearchInput(BaseModel):
    city: Optional[str] = Field(default=None, description="Город")
    country: Optional[str] = Field(default=None, description="Страна")
    remote: Optional[bool] = Field(default=None, description="Только удалённые")
    salary_min: Optional[int] = Field(default=None, description="Минимальная ЗП")

class UpdateCandidateInput(BaseModel):
    skills_to_add: Optional[List[str]] = Field(default=None, description="Навыки для добавления")
    skills_to_remove: Optional[List[str]] = Field(default=None, description="Навыки для удаления")
    experience_years: Optional[float] = Field(default=None, description="Новый опыт")
    desired_salary: Optional[int] = Field(default=None, description="Новая ЗП")
    city: Optional[str] = Field(default=None, description="Новый город")

@tool("generate_roadmap", args_schema=RoadmapInput)
def generate_roadmap(gaps: str, current_salary: int) -> str:
    """Сгенерировать план обучения по пробелам с прогнозом ЗП"""
    return f"ROADMAP:{gaps}:{current_salary}"

@tool("improve_resume", args_schema=ResumeInput)
def improve_resume(vacancy_title: str, company: str) -> str:
    """Адаптировать резюме под вакансию"""
    return f"RESUME:{vacancy_title}:{company}"

@tool("start_interview", args_schema=InterviewInput)
def start_interview(topics: str) -> str:
    """Начать mock интервью"""
    return f"INTERVIEW_START:{topics}"

@tool("search_vacancies", args_schema=VacancySearchInput)
def search_vacancies_tool(city: Optional[str] = None, country: Optional[str] = None, 
                          remote: Optional[bool] = None, salary_min: Optional[int] = None) -> str:
    """Поиск вакансий по фильтрам"""
    params = {"city": city, "country": country, "remote": remote, "salary_min": salary_min}
    return f"SEARCH_VACANCIES:{json.dumps(params)}"

@tool("update_candidate", args_schema=UpdateCandidateInput)
def update_candidate(skills_to_add: Optional[List[str]] = None, skills_to_remove: Optional[List[str]] = None,
                     experience_years: Optional[float] = None, desired_salary: Optional[int] = None,
                     city: Optional[str] = None) -> str:
    """Обновить профиль кандидата"""
    updates = {"skills_to_add": skills_to_add, "skills_to_remove": skills_to_remove,
               "experience_years": experience_years, "desired_salary": desired_salary, "city": city}
    return f"UPDATE_CANDIDATE:{json.dumps(updates)}"

career_tools_list = [generate_roadmap, improve_resume, start_interview, search_vacancies_tool, update_candidate]