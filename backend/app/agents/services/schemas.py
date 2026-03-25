from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Union, Any

class ExtractionSchema(BaseModel):
    # --- Личные данные ---
    name: Optional[str] = Field(
        default="Кандидат",
        description="ФИО (например, Эльбрусовец ...)")
    
    country: Optional[str] = Field(
        default=None, 
        description="Страна проживания, например Озз")
    city: Optional[str] = Field(
        default=None, 
        description="Город проживания, например Зеро")
    
    # Готовность к переезду
    relocation: Union[bool, str] =Field(
        default=False, 
        description="Готовность к переезду. True/False"
    )
    
    # --- Профессиональный профиль ---
    grade: Optional[str] = Field(
        default=None,
        description="Уровень: Junior, Middle, Senior, Lead или None, если не указано")
    
    specialization: str = Field(
        default="ML Engineer",
        description="Роль: Machine Learning Engineer и т.д.")
    
    experience_years: Union[float, str] = Field(
        default=0.0, 
        description="Коммерческий стаж (число, например 2.0 или 3.5 float)")
    
    desired_salary: Union[int, str] = Field(
        default=0,
        description="Желаемая зарплата (число, int)")
    
    work_format: List[str] = Field(
        default_factory=list, 
        description="Remote, Hybrid, Office, Удаленный, Офис, Гибрид"
    )
    foreign_languages: List[str] = Field(default_factory=list, description="English B2 и т.д.")

    # --- Технический стек ---
    skills: List[str] = Field(
        default_factory=list, 
        description="Полный список технических навыков, инструментов и технологий (например: Python, SQL, PostgreSQL, PyTorch, Docker, Airflow)"
    )
    # programming_languages: List[str] = Field(default_factory=list)
    # frameworks: List[str] = Field(default_factory=list)
    # databases: List[str] = Field(default_factory=list)
    # tools: List[str] = Field(default_factory=list)

# --- ВАЛИДАТОРЫ ДЛЯ ИСПРАВЛЕНИЯ ОШИБОК LLM ---

    @field_validator("experience_years", mode="before")
    @classmethod
    def validate_experience(cls, v: Any) -> float:
        if v is None or str(v).strip().lower() in ("none", "", "null"):
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    @field_validator("desired_salary", mode="before")
    @classmethod
    def validate_salary(cls, v: Any) -> int:
        if v is None or str(v).strip().lower() in ("none", ""):
            return 0
        if isinstance(v, str):
            # Убираем пробелы и валюту, если LLM их добавила (напр. "150 000 руб")
            v = "".join(filter(str.isdigit, v))
        try:
            return int(v) if v else 0
        except (ValueError, TypeError):
            return 0

    @field_validator("relocation", mode="before")
    @classmethod
    def validate_relocation(cls, v: Any) -> bool:
        # Если пришла строка "True"/"False" или "yes"/"no"
        if isinstance(v, str):
            return v.lower() in ("true", "yes", "1", "готов", "да")
        return bool(v)