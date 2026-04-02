from typing import List, Optional, TypedDict, Dict, Any

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