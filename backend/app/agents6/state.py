"""
Единый AgentState для LangGraph.

ИСПРАВЛЕНИЯ:
- Добавлены поля: query, location, market_context, skills_gap, candidate_resume,
  current_skills, interview, thread_id — без них Pydantic отбрасывал данные.
- visited_nodes: Set → List (Set не сериализуется в JSON → checkpoint ломается).
- messages: Annotated[..., add_messages] для корректного merge в LangGraph.
- country_normalized добавлен в CandidateProfile.
"""
 
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class CandidateProfile(BaseModel):
    name: str = ""
    country: Optional[str] = None
    country_normalized: Optional[str] = None
    city: Optional[str] = None
    city_normalized: Optional[str] = None
    relocation: bool = False
    grade: str = ""
    specialization: str = ""
    experience_years: float = 0.0
    desired_salary: Optional[int] = None
    work_format: List[str] = Field(default_factory=list)
    foreign_languages: List[str] = Field(default_factory=list)
    skills: List[str] = Field(default_factory=list)


class MarketContext(BaseModel):
    match_score: float = 0.0
    skill_gaps: List[str] = Field(default_factory=list)
    top_vacancies: List[Dict[str, Any]] = Field(default_factory=list)
    salary_median: int = 0
    salary_top_10: int = 0
    market_range: List[int] = Field(default_factory=list)


class AgentState(BaseModel):
    """
    Pydantic-state для LangGraph.
    Каждое поле, к которому обращаются nodes / main / services,
    ДОЛЖНО быть объявлено здесь — иначе Pydantic отбросит его при валидации.
    """

    # ── Сообщения (add_messages обеспечивает ДОПОЛНЕНИЕ, а не перезапись) ──
    messages: Annotated[List[BaseMessage], add_messages] = Field(default_factory=list)

    # ── Запрос пользователя ────────────────────────────────────────────
    query: Optional[str] = None
    user_input: Optional[str] = None
    action: Optional[str] = None
    response: Optional[Any] = None

    # ── Локация (фильтр для поиска вакансий) ───────────────────────────
    location: Optional[Dict[str, str]] = None

    # ── Кандидат ───────────────────────────────────────────────────────
    candidate: Optional[CandidateProfile] = None
    candidate_resume: Optional[str] = None
    current_skills: List[str] = Field(default_factory=list)

    # ── Рынок / вакансии ───────────────────────────────────────────────
    market: Optional[MarketContext] = None
    market_context: Optional[Dict[str, Any]] = None       # raw dict из tool
    top_vacancies: Optional[List[Dict[str, Any]]] = None
    selected_vacancy: Optional[Dict[str, Any]] = None

    # ── Навыки ─────────────────────────────────────────────────────────
    resume_skills: Optional[List[str]] = None
    missing_skills: Optional[List[str]] = None  # навыки кандидата с одной конкретной вакансией
    skills_gap: Optional[List[str]] = None    # Результат анализа рынка / Roadmap

    # ── Результаты инструментов ────────────────────────────────────────
    roadmap: Optional[Any] = None
    custom_resume: Optional[Any] = None
    mini_interview: Optional[Any] = None

    # ── Файлы ──────────────────────────────────────────────────────────
    raw_file_content: Optional[bytes] = None
    file_name: Optional[str] = None

    # ── Мета / навигация ───────────────────────────────────────────────
    summary: Optional[str] = None
    next_step: Optional[str] = None
    stage: Optional[str] = None
    intent: Optional[str] = None
    last_action: Optional[str] = None

    # ── История ────────────────────────────────────────────────────────
    history: List[Dict[str, Any]] = Field(default_factory=list)
    steps_taken: int = 0
    max_steps: int = 10
    visited_nodes: List[str] = Field(default_factory=list)  # Set нельзя — не сериализуем

    # ── Защита от зацикливания ─────────────────────────────────────────
    iteration_count: int = 0
    max_iterations: int = 5
    last_tool_call: Optional[str] = None
    consecutive_tool_calls: int = 0

    # ── Интервью ───────────────────────────────────────────────────────
    interview: Optional[Dict[str, Any]] = None

    # ── Сессия ─────────────────────────────────────────────────────────
    thread_id: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
