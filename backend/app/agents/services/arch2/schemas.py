from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Union

class ExtractionSchema(BaseModel):
    name: Optional[str] = "Кандидат"
    country: Optional[str] = None
    city: Optional[str] = None
    relocation: Union[bool, str] = False
    grade: Optional[str] = None
    specialization: str = "ML Engineer"
    experience_years: Union[float, str] = 0.0
    desired_salary: Union[int, str] = 0
    work_format: List[str] = []
    foreign_languages: List[str] = []
    skills: List[str] = []
    
    @field_validator("experience_years", mode="before")
    @classmethod
    def validate_exp(cls, v):
        if v is None or str(v).lower() in ("none", ""): return 0.0
        try: return float(v)
        except: return 0.0
    
    @field_validator("desired_salary", mode="before")
    @classmethod
    def validate_sal(cls, v):
        if v is None or str(v).lower() in ("none", ""): return 0
        if isinstance(v, str): v = "".join(filter(str.isdigit, v))
        try: return int(v) if v else 0
        except: return 0
    
    @field_validator("relocation", mode="before")
    @classmethod
    def validate_reloc(cls, v):
        if isinstance(v, str): return v.lower() in ("true", "yes", "1", "готов", "да")
        return bool(v)