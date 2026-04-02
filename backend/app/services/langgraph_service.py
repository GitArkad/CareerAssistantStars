from typing import Any, Dict, List, Optional


# =========================================
# ВРЕМЕННЫЙ ADAPTER ДЛЯ LANGGRAPH
# =========================================
# Задача этого файла:
# 1. принять profile из FastAPI
# 2. вызвать текущий pipeline / graph (когда подключишь)
# 3. привести любой "сырой" ответ к стабильному API-контракту
#
# ВАЖНО:
# Сейчас LangGraph у сокомандника возвращает список вакансий.
# Поэтому мы нормализуем его в формат:
#
# {
#   "candidate": {...},
#   "match_score": ...,
#   "missing_skills": [...],
#   "top_jobs": [...],
#   "recommendations": [...],
#   "ok": True/False,
#   "error": ...
# }
# =========================================


def _build_candidate_block(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Собираем компактный блок кандидата для ответа API.
    """
    return {
        "name": profile.get("name"),
        "grade": profile.get("grade"),
        "specialization": profile.get("specialization"),
        "country": profile.get("country"),
        "city": profile.get("city"),
        "experience_years": profile.get("experience_years"),
        "desired_salary": profile.get("desired_salary"),
        "skills": profile.get("skills", []),
    }


def _normalize_top_jobs(raw_result: Any) -> List[Dict[str, Any]]:
    """
    Нормализуем текущий ответ LangGraph.

    Сейчас ожидаем, что graph/pipeline может вернуть:
    - список вакансий
    - dict с полем top_jobs
    - dict с полем top_vacancies
    - что-то ещё (тогда вернём пустой список)
    """

    # 1. Если уже list вакансий
    if isinstance(raw_result, list):
        return raw_result

    # 2. Если dict с готовым top_jobs
    if isinstance(raw_result, dict):
        if isinstance(raw_result.get("top_jobs"), list):
            return raw_result["top_jobs"]

        if isinstance(raw_result.get("top_vacancies"), list):
            return raw_result["top_vacancies"]

        if isinstance(raw_result.get("results"), list):
            return raw_result["results"]

    return []


def _extract_match_score(raw_result: Any, top_jobs: List[Dict[str, Any]]) -> Optional[float]:
    """
    Пытаемся получить match_score:
    - если graph уже вернул match_score → берём его
    - иначе временно используем final_score первой вакансии
    - если нет final_score, пробуем score
    """

    if isinstance(raw_result, dict) and raw_result.get("match_score") is not None:
        return raw_result.get("match_score")

    if top_jobs:
        first = top_jobs[0]

        if isinstance(first, dict):
            if first.get("final_score") is not None:
                return first.get("final_score")

            if first.get("score") is not None:
                return first.get("score")

    return None


def _extract_missing_skills(raw_result: Any) -> List[str]:
    """
    Пытаемся взять missing_skills / skill_gaps из graph output.
    Если их пока нет — возвращаем пустой список.
    """

    if isinstance(raw_result, dict):
        if isinstance(raw_result.get("missing_skills"), list):
            return raw_result["missing_skills"]

        if isinstance(raw_result.get("skill_gaps"), list):
            return raw_result["skill_gaps"]

        market = raw_result.get("market")
        if isinstance(market, dict):
            if isinstance(market.get("missing_skills"), list):
                return market["missing_skills"]
            if isinstance(market.get("skill_gaps"), list):
                return market["skill_gaps"]

    return []


def _extract_recommendations(raw_result: Any, profile: Dict[str, Any], missing_skills: List[str]) -> List[str]:
    """
    Если graph уже вернул recommendations — используем их.
    Иначе собираем простые fallback-рекомендации.
    """

    if isinstance(raw_result, dict) and isinstance(raw_result.get("recommendations"), list):
        return raw_result["recommendations"]

    recommendations: List[str] = []

    if missing_skills:
        recommendations.append(
            f"Добавь в резюме или подтяни навыки: {', '.join(missing_skills[:3])}"
        )

    skills = profile.get("skills", [])
    if isinstance(skills, list) and len(skills) < 5:
        recommendations.append("Укажи больше релевантных навыков в резюме")

    if not profile.get("experience_years"):
        recommendations.append("Добавь информацию о коммерческом опыте")

    if not recommendations:
        recommendations.append("Резюме выглядит достаточно полным, можно переходить к таргетированной адаптации под вакансии")

    return recommendations


def call_langgraph_pipeline(profile: Dict[str, Any]) -> Any:
    """
    ВРЕМЕННАЯ ТОЧКА ВЫЗОВА GRAPH / PIPELINE.

    СЕЙЧАС:
    - тут можно оставить mock
    - или подставить реальный вызов, если сокомандник даст working import

    ПОТОМ:
    - именно эту функцию ты заменишь на реальный вызов graph / run_pipeline
    """

    # =========================================
    # ВРЕМЕННАЯ ЗАГЛУШКА ПОД ТЕКУЩИЙ ОТВЕТ СОКОМАНДНИКА
    # =========================================
    return [
        {
            "title": "Junior ML Engineer",
            "company": "SberAI",
            "skills": ["Python", "PyTorch", "SQL"],
            "city": "Moscow",
            "salary_from": 150000,
            "salary_to": 200000,
            "url": "https://career.sber.ru/vacancies/sberai-jr-ml",
            "score": 0.8211961,
            "final_score": 0.79271766,
        },
        {
            "title": "NLP Researcher",
            "company": "OpenAI Partner",
            "skills": ["Python", "LLM", "LangChain"],
            "city": "San Francisco",
            "salary_from": 5500,
            "salary_to": 8500,
            "url": "https://openaipartner.ai/jobs/nlp-senior-researcher",
            "score": 0.81449485,
            "final_score": 0.68869691,
        },
        {
            "title": "Computer Vision Engineer",
            "company": "DeepVision",
            "skills": ["OpenCV", "C++", "PyTorch"],
            "city": "Belgrade",
            "salary_from": 3000,
            "salary_to": 4500,
            "url": "https://deepvision.rs/career/cv-engineer",
            "score": 0.8213141,
            "final_score": 0.59278846,
        },
    ]


def run_analysis(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Главная функция для FastAPI.

    Принимает:
    - structured candidate profile

    Возвращает:
    - стабильный dict для frontend
    """

    try:
        candidate = _build_candidate_block(profile)

        raw_result = call_langgraph_pipeline(profile)

        top_jobs = _normalize_top_jobs(raw_result)
        match_score = _extract_match_score(raw_result, top_jobs)
        missing_skills = _extract_missing_skills(raw_result)
        recommendations = _extract_recommendations(raw_result, profile, missing_skills)

        return {
            "ok": True,
            "candidate": candidate,
            "match_score": match_score,
            "missing_skills": missing_skills,
            "top_jobs": top_jobs,
            "recommendations": recommendations,
            "error": None,
        }

    except Exception as e:
        return {
            "ok": False,
            "candidate": _build_candidate_block(profile),
            "match_score": None,
            "missing_skills": [],
            "top_jobs": [],
            "recommendations": [],
            "error": str(e),
        }