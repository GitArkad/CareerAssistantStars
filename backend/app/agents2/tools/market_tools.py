import hashlib
from typing import List, Dict, Any, TypedDict, Optional
from statistics import median


# -----------------------------
# MODEL
# -----------------------------
class MarketContext(TypedDict):
    match_score: float
    skill_gaps: List[str]
    top_vacancies: List[Dict[str, Any]]
    salary_median: int
    salary_top_10: int
    market_range: List[int]


# -----------------------------
# CACHE
# -----------------------------
_market_cache: Dict[str, MarketContext] = {}


# -----------------------------
# HASH TOP VACANCIES
# -----------------------------
def hash_vacancies(vacancies: List[Dict[str, Any]]) -> str:
    ids = []

    for v in vacancies:
        url = str(v.get("url", ""))
        ids.append(url)

    raw = "|".join(sorted(ids))
    return hashlib.md5(raw.encode()).hexdigest()


# -----------------------------
# MAIN COMPUTE
# -----------------------------
def compute_market_context(
    vacancies: List[Dict[str, Any]],
    user_skills: Optional[List[str]] = None,
) -> MarketContext:

    if not vacancies:
        return {
            "match_score": 0.0,
            "skill_gaps": [],
            "top_vacancies": [],
            "salary_median": 0,
            "salary_top_10": 0,
            "market_range": [0, 0],
        }

    user_skills = [s.lower() for s in (user_skills or [])]

    all_skills = []
    salaries = []
    match_scores = []

    # -----------------------------
    # COLLECT DATA
    # -----------------------------
    for v in vacancies:
        db_skills = [str(s).lower() for s in v.get("skills", [])]
        all_skills.extend(db_skills)

        salary = v.get("salary_from")
        if salary:
            salaries.append(int(salary))

        if user_skills:
            match = sum(1 for s in user_skills if s in db_skills)
            match_scores.append(match / max(len(user_skills), 1))

    # -----------------------------
    # SKILL GAPS
    # -----------------------------
    vacancy_skills_set = set()
    user_skills_set = set(s.lower().strip() for s in (user_skills or []))

    for v in vacancies:
        for s in v.get("skills", []):
            vacancy_skills_set.add(str(s).lower().strip())

    skill_gaps = list(vacancy_skills_set - user_skills_set)
    # -----------------------------
    # SALARIES
    # -----------------------------
    if salaries:
        salary_median = int(median(salaries))
        salary_top_10 = int(sorted(salaries, reverse=True)[0])
        market_range = [min(salaries), max(salaries)]
    else:
        salary_median = 0
        salary_top_10 = 0
        market_range = [0, 0]

    # -----------------------------
    # MATCH SCORE
    # -----------------------------
    if match_scores:
        match_score = round(sum(match_scores) / len(match_scores), 3)
    else:
        match_score = 0.0

    # -----------------------------
    # RESULT
    # -----------------------------
    return {
        "match_score": match_score,
        "skill_gaps": skill_gaps,
        "top_vacancies": vacancies,
        "salary_median": salary_median,
        "salary_top_10": salary_top_10,
        "market_range": market_range,
    }


# -----------------------------
# CACHE WRAPPER
# -----------------------------
def get_market_context(
    vacancies: List[Dict[str, Any]],
    user_skills: Optional[List[str]] = None,
) -> MarketContext:

    global _market_cache

    current_hash = hash_vacancies(vacancies)

    if current_hash in _market_cache:
        return _market_cache[current_hash]

    context = compute_market_context(vacancies, user_skills)

    _market_cache[current_hash] = context

    print(context)

    return context