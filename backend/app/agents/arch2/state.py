from typing import Annotated, List, Optional, TypedDict, Dict, Any, Sequence
import operator
from langchain_core.messages import BaseMessage

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
    raw_file_content: Optional[bytes]
    file_name: Optional[str]
    candidate: CandidateProfile
    market: MarketContext
    roadmap: Optional[str]
    tailored_resume: Optional[str]
    interview: Optional[Dict[str, Any]]
    stage: str
    error: Optional[str]
    ingestion_attempts: int
    just_processed_tool: bool
    total_iterations: int
    messages: Annotated[Sequence[BaseMessage], operator.add]