"""
Нормализация навыков с использованием skills_map.py.
Учитывает пробелы в ключах/значениях skills_map.py.
"""

import re
from typing import List, Set, Dict
from .skills_map import SKILL_IMPLIES, SKILL_SYNONYMS


def normalize_skill(skill: str) -> str:
    """
    Приводит навык к каноническому виду.
    Учитывает пробелы в skills_map.py.
    """
    if not skill:
        return ""
    
    skill_clean = skill.strip().lower()
    
    # Ищем в синонимах (ключи могут иметь пробелы)
    for key, value in SKILL_SYNONYMS.items():
        if key.strip().lower() == skill_clean:
            return value.strip()
    
    # Если не найдено — возвращаем Title Case
    return skill_clean.title()


def normalize_skills(skills: List[str]) -> List[str]:
    """
    Полная нормализация: синонимы + подразумеваемые навыки.
    Учитывает пробелы в skills_map.py.
    """
    if not skills:
        return []
    
    # Шаг 1: Нормализуем через синонимы
    normalized = [normalize_skill(s) for s in skills if s]
    
    # Шаг 2: Добавляем подразумеваемые (учитываем пробелы в skills_map.py)
    expanded = set(s.lower().strip() for s in normalized)
    
    for skill in list(expanded):
        skill_key = skill.strip()
        # Ищем в SKILL_IMPLIES (ключи с пробелами)
        for implies_key, implies_values in SKILL_IMPLIES.items():
            if implies_key.strip().lower() == skill_key:
                for implied in implies_values:
                    implied_clean = implied.strip().lower()
                    # Находим каноническое имя через синонимы
                    for syn_key, syn_val in SKILL_SYNONYMS.items():
                        if syn_key.strip().lower() == implied_clean:
                            expanded.add(syn_val.strip().lower())
                            break
                    else:
                        expanded.add(implied_clean)
                break
    
    # Шаг 3: Возвращаем в каноническом виде
    result = set()
    for skill in expanded:
        for syn_key, syn_val in SKILL_SYNONYMS.items():
            if syn_key.strip().lower() == skill:
                result.add(syn_val.strip())
                break
        else:
            result.add(skill.title())
    
    return sorted(list(result))


def extract_skills_from_text(text: str) -> List[str]:
    """
    Извлекает навыки из текста (резюме, чат, диалог).
    Использует word boundaries для точного поиска.
    Учитывает пробелы в skills_map.py.
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found_skills = set()
    
    # Ищем синонимы с учётом пробелов в keys
    for synonym, canonical in SKILL_SYNONYMS.items():
        syn_clean = synonym.strip().lower()
        # Word boundary для точного совпадения слов
        pattern = r'\b' + re.escape(syn_clean) + r'\b'
        if re.search(pattern, text_lower):
            found_skills.add(canonical.strip())
    
    # Добавляем подразумеваемые навыки
    expanded = set(found_skills)
    for skill in found_skills:
        skill_key = skill.strip().lower()
        for implies_key, implies_values in SKILL_IMPLIES.items():
            if implies_key.strip().lower() == skill_key:
                for implied in implies_values:
                    implied_clean = implied.strip().lower()
                    for syn_key, syn_val in SKILL_SYNONYMS.items():
                        if syn_key.strip().lower() == implied_clean:
                            expanded.add(syn_val.strip())
                            break
                break
    
    return sorted(list(expanded))


def merge_skills(existing_skills: List[str], new_skills: List[str]) -> List[str]:
    """Объединяет навыки из разных источников (резюме + чат)."""
    all_skills = (existing_skills or []) + (new_skills or [])
    return normalize_skills(all_skills)


def compare_skills(candidate_skills: List[str], vacancy_skills: List[str]) -> Dict[str, any]:
    """Сравнивает навыки кандидата с требованиями вакансии."""
    candidate_norm = normalize_skills(candidate_skills)
    vacancy_norm = normalize_skills(vacancy_skills)
    
    c_lower = set(s.lower() for s in candidate_norm)
    v_lower = set(s.lower() for s in vacancy_norm)
    
    match = c_lower & v_lower
    missing = v_lower - c_lower
    extra = c_lower - v_lower
    
    all_map = {s.lower(): s for s in candidate_norm + vacancy_norm}
    
    return {
        "match": [all_map.get(s, s) for s in sorted(match)],
        "missing": [all_map.get(s, s) for s in sorted(missing)],
        "extra": [all_map.get(s, s) for s in sorted(extra)],
        "match_percentage": round(len(match) / len(v_lower) * 100) if v_lower else 100
    }