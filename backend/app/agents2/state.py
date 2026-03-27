# app/agents2/state.py
from typing import TypedDict, List, Optional, Dict, Any

class CandidateProfile(TypedDict):
    name: str
    country: Optional[str]
    city: Optional[str]
    relocation: bool
    grade: str
    specialization: str
    experience_years: float
    desired_salary: Optional[int]
    work_format: List[str]
    foreign_languages: List[str]
    skills: List[str]

class MarketContext(TypedDict):
    match_score: float
    skill_gaps: List[str]
    top_vacancies: List[Dict[str, Any]]
    salary_median: int
    salary_top_10: int
    market_range: List[int]

class AgentState(TypedDict):
    candidate: Optional[CandidateProfile]
    market: Optional[MarketContext]
    user_input: Optional[str]
    raw_file_content: bytes
    file_name: str
    summary: Optional[str]
    next_step: str
    stage: str
    intent: Optional[str]
    last_action: Optional[str]

    top_vacancies: Optional[List[Dict[str, Any]]]
    roadmap: Optional[Dict[str, Any]]
    custom_resume: Optional[Dict[str, Any]]
    mini_interview: Optional[Dict[str, Any]]

    # Защита от зацикливаний
    steps_taken: Optional[int]
    max_steps: Optional[int]
    visited_nodes: Optional[set]