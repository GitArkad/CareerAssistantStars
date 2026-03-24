from pydantic import BaseModel, Field
from typing import List, Optional

class ExtractionSchema(BaseModel):
    # --- Личные данные ---
    name: str = Field(description="ФИО (например, Андрей ...)")
    country: Optional[str] = Field(None, description="Страна проживания")
    city: Optional[str] = Field(None, description="Город проживания")
    
    # Готовность к переезду
    relocation: bool = Field(
        default=False, 
        description="Готовность к переезду. True, если в резюме указано 'готов к переезду', иначе False"
    )
    
    # --- Профессиональный профиль ---
    grade: str = Field(description="Уровень: Junior, Middle, Senior, Lead")
    specialization: str = Field(description="Роль: Machine Learning Engineer и т.д.")
    
    experience_years: float = Field(default=0.0, description="Коммерческий стаж (float)")
    desired_salary: Optional[int] = Field(None, description="Зарплата (число)")
    
    work_format: List[str] = Field(
        default=[], 
        description="Remote, Hybrid, Office, Удаленный, Офис, Гибрид"
    )
    foreign_languages: List[str] = Field(default=[], description="English B2 и т.д.")

    # --- Технический стек ---
    programming_languages: List[str] = Field(default=[])
    frameworks: List[str] = Field(default=[])
    databases: List[str] = Field(default=[])
    tools: List[str] = Field(default=[])