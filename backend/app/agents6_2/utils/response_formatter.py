"""
Модуль для форматирования ответов ассистента.

Выносит логику извлечения и обогащения ответов из main.py.
"""

import logging
from typing import Dict, Any, Optional, List
from langchain_core.messages import AIMessage

logger = logging.getLogger(__name__)


def extract_assistant_response(result: Dict[str, Any]) -> str:
    """
    Безопасно извлекает текст ответа ассистента из результата графа.
    
    Args:
        result: Словарь с результатом выполнения графа
    
    Returns:
        str: Текст ответа или сообщение об ошибке
    """
    messages = result.get("messages", [])
    if not messages:
        logger.warning("⚠️ Пустой список сообщений")
        return "Нет ответа"
    
    # Ищем последнее сообщение от ассистента
    for msg in reversed(messages):
        try:
            if isinstance(msg, AIMessage):
                if msg.content and msg.content.strip():
                    return msg.content.strip()
            elif isinstance(msg, dict):
                if msg.get("role") == "assistant" and msg.get("content"):
                    return str(msg["content"]).strip()
        except Exception as e:
            logger.warning(f"⚠️ Пропущено сообщение при извлечении: {e}")
            continue
    
    # Фолбэк: берём последнее сообщение любого типа
    last_msg = messages[-1]
    if isinstance(last_msg, dict):
        content = last_msg.get("content", "")
        if content:
            return str(content).strip()
    elif hasattr(last_msg, "content"):
        content = getattr(last_msg, "content", "")
        if content:
            return str(content).strip()
    
    return "Извините, не удалось сформировать ответ. Попробуйте ещё раз."


def format_vacancy_item(vacancy: Dict[str, Any], index: int) -> str:
    """
    Форматирует одну вакансию в читаемую строку.
    
    Args:
        vacancy: Данные вакансии из top_vacancies
        index: Порядковый номер в списке
    
    Returns:
        str: Отформатированная строка с информацией о вакансии
    """
    payload = vacancy.get("payload", {})
    
    title = payload.get("title", "Без названия")
    company = payload.get("company", "Не указано")
    city = payload.get("city", "")
    salary_from = payload.get("salary_from")
    salary_to = payload.get("salary_to")
    work_format = payload.get("work_format", "")
    url = payload.get("url", "")
    score = vacancy.get("score", 0)
    
    # Формируем строку зарплаты
    if salary_from and salary_to:
        salary_str = f"{salary_from:,} – {salary_to:,} ₽"
    elif salary_from:
        salary_str = f"от {salary_from:,} ₽"
    elif salary_to:
        salary_str = f"до {salary_to:,} ₽"
    else:
        salary_str = "З/п не указана"
    
    # Формируем строку локации
    location = f"{city}" if city else ""
    if work_format:
        location += f" • {work_format}" if location else work_format
    
    # Собираем строку
    line = f"{index}. **{title}** @ {company}"
    if location:
        line += f" | {location}"
    line += f" | {salary_str}"
    if url:
        line += f"\n   🔗 {url}"
    line += f" | 🔍 score: {score:.3f}"
    
    return line


def append_vacancies_list(response: str, market_context: Optional[Dict]) -> str:
    """
    Добавляет форматированный список найденных вакансий к ответу.
    
    Args:
        response: Исходный текст ответа от LLM
        market_context: Контекст рынка с top_vacancies
    
    Returns:
        str: Ответ + список вакансий (если есть)
    """
    if not market_context:
        return response
    
    vacancies = market_context.get("top_vacancies", [])
    if not vacancies:
        return response
    
    lines = ["\n\n📋 **Найдено вакансий**:"]
    
    for i, vac in enumerate(vacancies, 1):
        lines.append(format_vacancy_item(vac, i))
    
    return response + "\n".join(lines)


def build_api_response(
    response_text: str,
    thread_id: str,
    market_context: Optional[Dict] = None,
    include_vacancies_struct: bool = False
) -> Dict[str, Any]:
    """
    Строит финальный ответ для API.
    
    Args:
        response_text: Текст ответа от ассистента
        thread_id: Идентификатор сессии
        market_context: Контекст рынка (опционально)
        include_vacancies_struct: Если True — добавляет вакансии отдельным полем
    
    Returns:
        Dict: Готовый ответ для отправки клиенту
    """
    result = {
        "response": response_text,
        "thread_id": thread_id
    }
    
    # Если нужно — добавляем структурированный список вакансий
    if include_vacancies_struct and market_context:
        vacancies = market_context.get("top_vacancies", [])
        result["vacancies"] = [
            {
                "title": v["payload"].get("title"),
                "company": v["payload"].get("company"),
                "city": v["payload"].get("city"),
                "salary_from": v["payload"].get("salary_from"),
                "salary_to": v["payload"].get("salary_to"),
                "work_format": v["payload"].get("work_format"),
                "url": v["payload"].get("url"),
                "score": round(v.get("score", 0), 3)
            }
            for v in vacancies
        ]
    
    return result