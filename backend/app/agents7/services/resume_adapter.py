"""
Сервис адаптации резюме под вакансии.

ИСПРАВЛЕНИЯ:
- adapt_resume_to_vacancy: сигнатура согласована с вызовом из main.py.
- extract_resume_data_from_state: принимает 1 аргумент (current_state), как вызывается в main.py.
- Добавлен fallback если нет vacancy_context.
"""

import logging
from typing import Dict, Any, Optional, List

from ..tools import vacancy_search_tool
from ..utils.skill_normalizer import normalize_skills

logger = logging.getLogger(__name__)


async def adapt_resume_to_vacancy(
    message: str,
    candidate_resume: Optional[str] = None,
    vacancy_context: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Адаптирует резюме под требования вакансии.

    Args:
        message: Запрос пользователя
        candidate_resume: Текст резюме (может быть None)
        vacancy_context: Контекст вакансии с полями vacancy_payload и declared_skills

    Returns:
        str: Форматированный ответ
    """
    logger.info(f"✏️ [resume_adapter] message='{message[:50]}...'")

    try:
        declared_skills = []
        vacancy_payload = {}

        if vacancy_context:
            declared_skills = vacancy_context.get("declared_skills", [])
            vacancy_payload = vacancy_context.get("vacancy_payload", {})

        if not vacancy_payload:
            return (
                "⚠️ Для адаптации резюме нужна вакансия. "
                "Сначала найдите вакансии командой вроде «Найди вакансии ML Engineer в Москве», "
                "а потом запросите адаптацию."
            )

        result = await vacancy_search_tool.tailor_resume(
            candidate_resume=candidate_resume or "",
            vacancy_payload=vacancy_payload,
            declared_skills=normalize_skills(declared_skills) if declared_skills else [],
        )

        if result.get("status") == "adapted":
            return format_resume_response(result)
        elif result.get("error"):
            return f"⚠️ Не удалось проанализировать резюме: {result['error']}"
        else:
            recs = result.get("recommendations", ["✅ Готово"])
            return recs[0] if recs else "✅ Готово"

    except Exception as e:
        logger.exception(f"❌ [resume_adapter] Ошибка: {e}")
        return f"⚠️ Ошибка адаптации: {str(e)}"


def format_resume_response(result: Dict[str, Any]) -> str:
    """Форматирует результат tailor_resume в текстовый ответ."""
    title = result.get("vacancy_title", "вакансии")
    company = result.get("vacancy_company", "")
    header = f"{title} @ {company}" if company else title

    text = f"✏️ Рекомендации для \"{header}\":\n\n"
    text += f"✅ Совпадение: {result.get('match_percentage')}%\n"

    if result.get("missing_skills"):
        keywords = ", ".join(f'"{s}"' for s in result["missing_skills"][:5])
        text += f"🔑 Добавь: {keywords}\n"

    if result.get("matched_skills"):
        matched = ", ".join(result["matched_skills"][:5])
        text += f"✅ Совпадают: {matched}\n"

    text += "\n💡 Используй глаголы действия: «разработал», «внедрил», «оптимизировал»\n"
    text += f"📊 После правок: ~{result.get('projected_match')}%"

    if result.get("vacancy_url"):
        text += f"\n🔗 {result['vacancy_url']}"

    return text


def should_trigger_resume_adaptation(message: str) -> bool:
    """Проверяет, запрашивает ли пользователь адаптацию резюме."""
    if not message:
        return False

    keywords = [
        "адаптируй резюме", "улучши резюме", "что добавить в резюме",
        "подготовь резюме", "помоги с резюме", "оптимизируй резюме",
        "сделай резюме лучше", "какие навыки добавить", "чего не хватает в резюме",
        "проверь резюме", "оцени резюме", "усиль резюме", "резюме под вакансию",
        "адаптировать резюме", "адаптация резюме", "улучшение резюме",
        "улучши моё резюме", "улучши мое резюме", "под вакансию", "подготовить резюме",
    ]

    return any(kw in message.lower() for kw in keywords)


def extract_resume_data_from_state(current_state: Dict) -> Dict[str, Any]:
    """
    Извлекает данные для адаптации резюме из state.

    ИСПРАВЛЕНИЕ: принимает 1 аргумент (current_state), как вызывается в main.py.

    Returns:
        Dict с ключами: resume_text, vacancy_context (declared_skills + vacancy_payload)
    """
    # Текст резюме
    resume_text = current_state.get("candidate_resume") or current_state.get("user_input") or ""

    # Навыки кандидата
    declared_skills = []
    candidate = current_state.get("candidate")
    if candidate:
        if isinstance(candidate, dict):
            declared_skills = candidate.get("skills", [])
        elif hasattr(candidate, "skills"):
            declared_skills = getattr(candidate, "skills", [])

    # Если навыки из current_skills
    if not declared_skills:
        declared_skills = current_state.get("current_skills", [])

    # Вакансия (берём первую из top_vacancies)
    vacancy_payload = {}
    market_context = current_state.get("market_context") or {}
    if isinstance(market_context, dict) and market_context.get("top_vacancies"):
        vacancy_payload = market_context["top_vacancies"][0].get("payload", {})

    # Или из market (Pydantic)
    if not vacancy_payload:
        market = current_state.get("market")
        if market:
            top = None
            if isinstance(market, dict):
                top = market.get("top_vacancies", [])
            elif hasattr(market, "top_vacancies"):
                top = getattr(market, "top_vacancies", [])
            if top:
                vacancy_payload = top[0].get("payload", {})

    return {
        "resume_text": resume_text,
        "vacancy_context": {
            "declared_skills": declared_skills,
            "vacancy_payload": vacancy_payload,
        },
    }
