from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Set
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage

class CandidateProfile(BaseModel):
    name: str = ""
    country: Optional[str] = None
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
    messages: List[BaseMessage] = Field(default_factory=list)
    action: Optional[str] = None
    response: Optional[Any] = None

    candidate: Optional[CandidateProfile] = None
    market: Optional[MarketContext] = None
    top_vacancies: Optional[List[Dict[str, Any]]] = None
    selected_vacancy: Optional[Dict[str, Any]] = None

    resume_skills: Optional[List[str]] = None
    missing_skills: Optional[List[str]] = None

    roadmap: Optional[Any] = None
    custom_resume: Optional[Any] = None
    mini_interview: Optional[Any] = None

    user_input: Optional[str] = None
    raw_file_content: Optional[bytes] = None
    file_name: Optional[str] = None
    summary: Optional[str] = None
    next_step: Optional[str] = None

    stage: Optional[str] = None
    intent: Optional[str] = None
    last_action: Optional[str] = None

    history: Optional[List[Dict[str, Any]]] = None
    steps_taken: int = 0
    max_steps: int = 10
    visited_nodes: Set[str] = Field(default_factory=set)

    iteration_count: int = 0
    max_iterations: int = 5
    last_tool_call: Optional[str] = None
    consecutive_tool_calls: int = 0