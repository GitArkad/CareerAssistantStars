# app/agents2/state.py
from typing import TypedDict, List, Optional, Dict, Any, Set


class CandidateProfile(TypedDict, total=False):
    name: str
    country: Optional[str]
    city: Optional[str]
    city_normalized: Optional[str]
    relocation: bool
    grade: str
    specialization: str
    experience_years: float
    desired_salary: Optional[int]
    work_format: List[str]
    foreign_languages: List[str]
    skills: List[str]


class MarketContext(TypedDict, total=False):
    match_score: float
    skill_gaps: List[str]
    top_vacancies: List[Dict[str, Any]]
    salary_median: int
    salary_top_10: int
    market_range: List[int]


class AgentState(TypedDict, total=False):
    # -----------------------------
    # ОСНОВНОЕ (🔥 критично)
    # -----------------------------
    message: Optional[str]
    action: Optional[str]
    response: Optional[Any]

    # -----------------------------
    # КАНДИДАТ / РЫНОК
    # -----------------------------
    candidate: Optional[CandidateProfile]
    market: Optional[MarketContext]
    top_vacancies: Optional[List[Dict[str, Any]]]

    # 👉 ДОБАВЛЕНО (используется в roadmap / resume)
    resume_skills: Optional[List[str]]
    missing_skills: Optional[List[str]]

    # 👉 выбор вакансии пользователем
    selected_vacancy: Optional[Dict[str, Any]]

    # -----------------------------
    # FEATURE OUTPUTS
    # -----------------------------
    roadmap: Optional[Any]
    custom_resume: Optional[Any]
    mini_interview: Optional[Any]

    # -----------------------------
    # PIPELINE
    # -----------------------------
    user_input: Optional[str]
    raw_file_content: Optional[bytes]
    file_name: Optional[str]
    summary: Optional[str]
    next_step: Optional[str]

    # 👉 КРИТИЧНО ДЛЯ ДИАЛОГА
    stage: Optional[str]

    intent: Optional[str]
    last_action: Optional[str]

    # -----------------------------
    # DEBUG / CONTROL
    # -----------------------------
    history: Optional[List[Dict[str, Any]]]
    steps_taken: Optional[int]
    max_steps: Optional[int]
    visited_nodes: Optional[Set[str]]