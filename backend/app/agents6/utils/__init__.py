"""
Модуль утилит для проекта Career Assistant.

Экспортирует функции нормализации для удобного импорта:
    from app.utils import normalize_city, normalize_country
"""

from .normalizers import (
    normalize_city,
    normalize_country,
    get_city_aliases,
    get_country_aliases,
)

from .city_map import CITY_NORMALIZATION_MAP
from .country_map import COUNTRY_NORMALIZATION_MAP

from .normalizers import normalize_city, normalize_country
from .skill_normalizer import (
    normalize_skill,
    normalize_skills,
    extract_skills_from_text,
    merge_skills,
    compare_skills
)

__all__ = [
    "normalize_city",
    "normalize_country",
    "get_city_aliases",
    "get_country_aliases",
    "CITY_NORMALIZATION_MAP",
    "COUNTRY_NORMALIZATION_MAP",

    "normalize_city",
    "normalize_country", 
    "normalize_skill",
    "normalize_skills",
    "extract_skills_from_text",
    "merge_skills",
    "compare_skills"
]