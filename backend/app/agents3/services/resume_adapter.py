"""
Сервис адаптации резюме под вакансии.
Вынесен из main.py для чистоты кода и тестируемости.
"""

import logging
from typing import Dict, Any, Optional, List

from ..tools import vacancy_search_tool
from ..utils.skill_normalizer import normalize_skills

logger = logging.getLogger(__name__)


async def adapt_resume_to_vacancy(
    candidate_resume: str,
    declared_skills: List[str],
    vacancy_payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Адаптирует резюме под требования вакансии.
    
    Args:
        candidate_resume: Текст резюме (может быть пустым)
        declared_skills: Навыки кандидата (из резюме или state)
        vacancy_payload: Данные вакансии из Qdrant
    
    Returns:
        Dict с готовым ответом для пользователя
    """
    logger.info(f"✏️ [resume_adapter] Адаптация резюме: skills={len(declared_skills)}, vacancy={vacancy_payload.get('title')}")
    
    try:
        # Вызываем основную логику из vacancy_search_tool
        result = await vacancy_search_tool.tailor_resume(
            candidate_resume=candidate_resume,
            vacancy_payload=vacancy_payload,
            declared_skills=normalize_skills(declared_skills) if declared_skills else []
        )
        
        # Формируем красивый ответ
        if result.get("status") == "adapted":
            return format_resume_response(result)
        else:
            return {
                "response": result.get("recommendations", ["✅ Готово"])[0],
                "status": result.get("status")
            }
    
    except Exception as e:
        logger.exception(f"❌ [resume_adapter] Ошибка: {e}")
        return {
            "response": f"⚠️ Не удалось проанализировать резюме: {str(e)}",
            "status": "error"
        }


def format_resume_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Форматирует результат tailor_resume в ответ для пользователя.
    """
    title = result.get("vacancy_title", "вакансии")
    company = result.get("vacancy_company", "")
    header = f"{title} @ {company}" if company else title
    
    response_text = f"✏️ Рекомендации для \"{header}\":\n\n"
    response_text += f"✅ Совпадение: {result.get('match_percentage')}%\n"
    
    if result.get("missing_skills"):
        keywords = ", ".join(f'"{s}"' for s in result["missing_skills"][:5])
        response_text += f"🔑 Добавь: {keywords}\n"
    
    response_text += "💡 Используй глаголы действия: «разработал», «внедрил», «оптимизировал»\n"
    response_text += f"📊 После правок: ~{result.get('projected_match')}%"
    
    if result.get("vacancy_url"):
        response_text += f"\n🔗 {result['vacancy_url']}"
    
    return {
        "response": response_text,
        "status": "adapted",
        "match_percentage": result.get("match_percentage"),
        "missing_skills": result.get("missing_skills", [])
    }


def should_trigger_resume_adaptation(message: str) -> bool:
    """
    Проверяет, является ли запрос запросом на адаптацию резюме.
    """
    if not message:
        return False
    
    #РАСШИРЕННЫЙ СПИСОК
    keywords = [
        "адаптируй резюме", "улучши резюме", "что добавить в резюме",
        "подготовь резюме", "помоги с резюме", "оптимизируй резюме",
        "сделай резюме лучше", "какие навыки добавить", "чего не хватает в резюме",
        "проверь резюме", "оцени резюме", "усиль резюме", "резюме под вакансию",
        # 🔥 ДОБАВЬ ЭТИ ВАРИАЦИИ:
        "адаптировать резюме", "адаптация резюме", "улучшение резюме",
        "улучши моё резюме", "улучши мое резюме", "под вакансию", "подготовить резюме"
    ]
    
    return any(kw in message.lower() for kw in keywords)


def extract_resume_data_from_state(current_state: Dict, input_state: Dict, candidate_data: Optional[Dict]) -> tuple:
    """
    Извлекает данные для адаптации резюме из state.
    
    Returns:
        (declared_skills, vacancy_payload)
    """
    # Навыки
    declared_skills = []
    if candidate_data and candidate_data.get("skills"):
        declared_skills = candidate_data["skills"]
    elif current_state.get("candidate", {}).get("skills"):
        declared_skills = current_state["candidate"]["skills"]
    
    # Вакансия
    vacancy_payload = {}
    market = current_state.get("market", {})
    if market.get("top_vacancies"):
        vacancy_payload = market["top_vacancies"][0].get("payload", {})
    elif input_state.get("market") and input_state["market"].get("top_vacancies"):
        vacancy_payload = input_state["market"]["top_vacancies"][0].get("payload", {})
    
    return declared_skills, vacancy_payload