import os
import json
import asyncio
import logging
from typing import Dict, Any, List, Optional, Union

from langchain_groq import ChatGroq
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage

# ✅ Импортируем единый AgentState из state.py (Pydantic BaseModel)
from .state import AgentState, MarketContext
from .tools import vacancy_search_tool

# 🔥 Импортируем нормализацию навыков (использует skills_map.py: SKILL_SYNONYMS + SKILL_IMPLIES)
from .utils.skill_normalizer import normalize_skills, extract_skills_from_text, merge_skills

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

groq_api_key = os.getenv("GROQ_API_KEY")
model_name = os.getenv("GROQ_MODEL", "llama3-70b-8192")

if not groq_api_key:
    raise ValueError(
        "GROQ_API_KEY not found in environment variables. "
        "Please set it in your .env file or environment."
    )

logger.info(f"🔄 Инициализация LLM: {model_name}")
llm = ChatGroq(groq_api_key=groq_api_key, model_name=model_name)


# =============================================================================
# ИНСТРУМЕНТЫ (TOOLS)
# =============================================================================

@tool
def analyze_market_context(query: str, location: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Анализирует рынок вакансий по поисковому запросу и локации.
    
    Args:
        query: Поисковый запрос (например, "Python разработчик", "Data Scientist")
        location: Словарь с фильтрами локации {'city': 'Москва', 'country': 'Россия'}
    
    Returns:
        Dict с полями: top_vacancies, salary_median, salary_top_10, market_range, skill_g, match_score
    """
    logger.info(f"🔍 [TOOL] analyze_market_context вызван: query='{query}', location={location}")
    try:
        result = asyncio.run(vacancy_search_tool.search_vacancies(query, location))
        if isinstance(result, dict) and "error" in result:
            logger.error(f"❌ [TOOL] Ошибка поиска: {result['error']}")
        else:
            vacancies_count = len(result.get("top_vacancies", []))
            logger.info(f"✅ [TOOL] Найдено вакансий: {vacancies_count}, медиана: {result.get('salary_median')}")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Критическая ошибка в analyze_market_context: {e}")
        return {"error": f"Ошибка анализа рынка: {str(e)}", "top_vacancies": [], "salary_median": 0}


@tool
def calculate_skills_gap(current_skills: List[str], market_context: Dict) -> List[str]:
    """Вычисляет разрыв между навыками пользователя и требованиями рынка."""
    logger.info(f"🧠 [TOOL] calculate_skills_gap: текущие навыки={current_skills}")
    try:
        # Парсим market_skills
        skill_g_raw = market_context.get("skill_g", [])
        if isinstance(skill_g_raw, str):
            market_skills = [s.strip() for s in skill_g_raw.split(",") if s.strip()]
        elif isinstance(skill_g_raw, list):
            market_skills = skill_g_raw
        else:
            market_skills = []
        
        # 🔥 НОРМАЛИЗАЦИЯ ОБОИХ СПИСКОВ (применяет синонимы + импликации)
        candidate_normalized = set(s.lower() for s in normalize_skills(current_skills))
        market_normalized = set(s.lower() for s in normalize_skills(market_skills))
        
        # Сравниваем в lowercase
        gaps = [skill for skill in market_skills if skill.lower().strip() not in candidate_normalized]
        
        logger.info(f"✅ [TOOL] Найдено разрывов навыков: {len(gaps)} -> {gaps}")
        return gaps[:5]
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка в calculate_skills_gap: {e}")
        return []

@tool
def get_salary_insights(market_context: Dict) -> Dict[str, Any]:
    """
    Получает детальную информацию о зарплатах из рыночного контекста.
    
    Args:
        market_context: Результат анализа рынка из analyze_market_context
    
    Returns:
        Dict с полями: median, top_10, range, currency
    """
    logger.info(f"💰 [TOOL] get_salary_insights вызван")
    try:
        result = {
            "median": market_context.get("salary_median", 0),
            "top_10": market_context.get("salary_top_10", 0),
            "range": market_context.get("market_range", [0, 0]),
            "currency": "RUB"
        }
        logger.info(f"✅ [TOOL] Зарплатная статистика: медиана={result['median']}, диапазон={result['range']}")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка в get_salary_insights: {e}")
        return {"median": 0, "top_10": 0, "range": [0, 0], "currency": "RUB", "error": str(e)}
    
@tool
def generate_roadmap_tool(
    current_skills: List[str],
    market_context: Dict,
    target_role: Optional[str] = None,
    timeframe_months: int = 3
) -> Dict[str, Any]:
    """
    Генерирует план развития навыков на основе анализа рынка.
    """
    logger.info(f"🗺️ [TOOL] generate_roadmap: skills={current_skills}, role={target_role}")
    try:
        # 🔥 НОРМАЛИЗАЦИЯ НАВЫКОВ КАНДИДАТА ПЕРЕД ПЕРЕДАЧЕЙ В ИНСТРУМЕНТ
        current_skills = normalize_skills(current_skills)
        logger.info(f"✅ Нормализованные навыки для roadmap: {current_skills}")
        
        result = asyncio.run(
            vacancy_search_tool.generate_roadmap(
                current_skills=current_skills,
                market_context=market_context,
                target_role=target_role,
                timeframe_months=timeframe_months
            )
        )
        logger.info(f"✅ [TOOL] Roadmap сгенерирован: {len(result.get('skill_priorities', []))} навыков")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка в generate_roadmap: {e}")
        return {"error": str(e), "skill_priorities": []}

@tool
def tailor_resume_tool(
    candidate_resume: Optional[str] = None,
    vacancy_payload: Optional[Dict[str, Any]] = None,
    declared_skills: Optional[List[str]] = None  # ← НОВЫЙ ПАРАМЕТР
) -> Dict[str, Any]:
    """
    Адаптирует резюме под требования вакансии.
    
    📋 АРГУМЕНТЫ (СТРОГО):
    - candidate_resume: str — текст резюме (или "")
    - vacancy_payload: dict — объект вакансии с полями: title, company, requirements/skills, url
    - declared_skills: list[str] — список навыков пользователя (МАССИВ, не строка!)
    
    ✅ ПРИМЕР ВЫЗОВА:
    {
      "candidate_resume": "Разрабатывал модели на Python",
      "vacancy_payload": {"title": "ML Engineer", "requirements": ["Python", "PyTorch"]},
      "declared_skills": ["Python", "SQL"]
    }
    
    ❌ ОШИБКИ:
    - declared_skills: "Python, SQL" ← НЕ строка, а массив ["Python", "SQL"]
    - vacancy_payload: "требования..." ← НЕ строка, а объект {...}
    """
    logger.info(f"✏️ [TOOL] tailor_resume_tool: declared_skills={declared_skills}")
    try:
        result = asyncio.run(
            vacancy_search_tool.tailor_resume(
                candidate_resume=candidate_resume,
                vacancy_payload=vacancy_payload,
                declared_skills=declared_skills  # ← Передаём
            )
        )
        logger.info(f"✅ [TOOL] tailor_resume: match={result.get('match_percentage')}%")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка: {e}")
        return {"status": "error", "message": str(e), "recommendations": []}


# =============================================================================
# УЗЛЫ ГРАФА (NODES) — ИСПРАВЛЕНО ДЛЯ PYDANTIC
# =============================================================================

def assistant_node(state: AgentState) -> Dict[str, Any]:
    """
    Основной узел агента-ассистента.
    
    Returns:
        Dict с обновлениями состояния (LangGraph merge)
    """
    # ✅ Pydantic: используем getattr вместо .get()
    query = getattr(state, 'query', None)
    messages = getattr(state, 'messages', [])

    # 🔥 ИЗВЛЕЧЕНИЕ НАВЫКОВ ИЗ ЗАПРОСА ПОЛЬЗОВАТЕЛЯ (чат)
    if query:
        extracted_skills = extract_skills_from_text(query)
        if extracted_skills:
            logger.info(f"🔍 Извлечены навыки из запроса: {extracted_skills}")
            
            # 🔥 ОБЪЕДИНЯЕМ с существующими навыками кандидата (если есть)
            candidate = getattr(state, 'candidate', None)
            if candidate:
                existing_skills = getattr(candidate, 'skills', []) or []
                merged_skills = merge_skills(existing_skills, extracted_skills)
                # 🔥 Обновляем candidate.skills (если candidate — Pydantic модель)
                if hasattr(candidate, 'skills'):
                    candidate.skills = merged_skills
                logger.info(f"📦 Объединённые навыки: {merged_skills}")
    
    # 🔥 Также объединяем с resume_skills (если парсили резюме)
    resume_skills = getattr(state, 'resume_skills', [])
    candidate = getattr(state, 'candidate', None)
    if candidate and resume_skills:
        existing_skills = getattr(candidate, 'skills', []) or []
        merged_skills = merge_skills(existing_skills, resume_skills)
        if hasattr(candidate, 'skills'):
            candidate.skills = merged_skills
        logger.info(f"📦 Навыки после объединения с резюме: {merged_skills}")
    
    logger.info(f"🎯 [assistant_node] Начало. Query: {query}, Сообщений: {len(messages)}")
    
    system_prompt = """
    Ты — карьерный ассистент, помогающий специалистам анализировать рынок труда.
    Твои возможности:
    1. Анализировать вакансии по запросу и локации
    2. Сравнивать навыки пользователя с требованиями рынка
    3. Давать рекомендации по развитию и зарплатным ожиданиям

    ПРАВИЛА:
    - Если пользователь указал свои навыки (например, "знаю Python, SQL"), НЕ рекомендуй их снова
    - Используй результаты инструментов: если calculate_skills_gap вернул список — это и есть рекомендации
    - Не добавляй навыки, которые уже есть у пользователя (даже если они популярны)
    - Если skill_gap пуст — скажи, что навыков достаточно для старта

    

    ДОСТУП К ДАННЫМ ВАКАНСИЙ:
        Вакансии находятся в market_context.top_vacancies[].payload:
        - title: vacancy["payload"]["title"]
        - company: vacancy["payload"]["company"]
        - city/country: vacancy["payload"]["city"], vacancy["payload"]["country"]
        - salary: vacancy["payload"]["salary_from"], vacancy["payload"]["salary_to"]
        - skills/requirements: vacancy["payload"]["skills"] или ["requirements"]
        - url: vacancy["payload"]["url"]

        ФОРМАТ ВЫВОДА (ТОП-5):
        1. {payload.title}
        {payload.company}
        {payload.city}, {payload.country}
        {salary_from} — {salary_to} {currency}
        Стек: {", ".join(payload.skills[:5])}
        {payload.url}


    ПРАВИЛА ИСПОЛЬЗОВАНИЯ ИНСТРУМЕНТОВ:
    - Если пользователь просит "адаптируй резюме", "улучши резюме", "что добавить в резюме" → ВЫЗОВИ tailor_resume_tool
    - Если пользователь спрашивает "что учить", "план развития" → ВЫЗОВИ generate_roadmap_tool
    - Если пользователь ищет вакансии → ВЫЗОВИ analyze_market_context
    - После получения результата от инструмента — ОТВЕЧАЙ НА ОСНОВЕ ЕГО ДАННЫХ, не выдумывай

    ФОРМАТ ОТВЕТА для поиска вакансий:
    1. если вариантов не нашлось, то коротко ответь, что вариантов нет и предложи изменить фильтр поиска.

    ФОРМАТ ОТВЕТА ДЛЯ tailor_resume:
    Инструкции по шагам:
    1. Выдели ТОП-5 ключевых навыков и требований из вакансии (Keywords).
    2. Проведи аудит резюме: что из требований вакансии уже есть у кандидата, а что сформулировано слабо.
    3. Перепиши блок "Достижения", сделав его коротким (3-4 строки) и сфокусированным на решении задач бизнеса из описания вакансии.
    4. Переформулируй задачи в достижения, используя формулу Google: "Сделал [X], что измеряется [Y], путем внедрения [Z]". 
    Пример: "Ускорил инференс модели на 30% за счет квантования в TensorRT".
    5. Добавь недостающие ключевые слова (Keywords) из вакансии в раздел Skills, если они соответствуют реальному стеку кандидата.
    
    Отвечай на русском, по делу, структурированно. После получения ToolMessage сформулируй ответ НА ОСНОВЕ этих данных.
    
    
    """

    system_message = SystemMessage(content=system_prompt)
    
    # Конвертация сообщений в объекты LangChain
    all_messages = [system_message]
    for m in messages:
        if isinstance(m, BaseMessage):
            all_messages.append(m)
        elif isinstance(m, dict):
            role = m.get("role", "")
            content = m.get("content", "")
            if role == "human":
                all_messages.append(HumanMessage(content=content))
            elif role == "assistant":
                all_messages.append(AIMessage(content=content))
            elif role == "system":
                all_messages.append(SystemMessage(content=content))
    
    logger.info(f"📦 [assistant_node] Подготовлено {len(all_messages)} сообщений для LLM")
    
    llm_with_tools = llm.bind_tools([
        analyze_market_context,
        calculate_skills_gap,
        get_salary_insights,
        generate_roadmap_tool,
        tailor_resume_tool,
    ])
    
    try:
        logger.info(f"🤖 [LLM] Вызов модели...")
        response = llm_with_tools.invoke(all_messages)
        
        # 🔍 Отладочный вывод
        content_preview = response.content[:200] if response.content else 'EMPTY'
        logger.info(f"📝 [LLM] Content: '{content_preview}...'")
        logger.info(f"🔧 [LLM] Tool calls: {response.tool_calls if hasattr(response, 'tool_calls') else 'N/A'}")
        
        # 🆕 Фолбэк: если ответ пустой, но есть результаты — формируем вручную
        if not response.content:
            market_context = getattr(state, 'market_context', None)
            if market_context:
                mc = market_context if isinstance(market_context, dict) else market_context.model_dump() if hasattr(market_context, 'model_dump') else {}
                vacancies = len(mc.get("top_vacancies", []))
                median = mc.get("salary_median", 0)
                fallback_content = f"🔍 По вашему запросу найдено {vacancies} вакансий. Медианная зарплата: {median:,} ₽."
                skills_gap = getattr(state, 'skills_gap', None)
                if skills_gap:
                    fallback_content += f"\n\n📚 Рекомендуемые навыки: {', '.join(skills_gap[:3])}"
                response = AIMessage(content=fallback_content)
                logger.info(f"✅ [assistant_node] Сгенерирован фолбэк-ответ")
        
        # ✅ Возвращаем dict с обновлениями (LangGraph merge)
        updated_messages = messages + [response]
        logger.info(f"✅ [assistant_node] Завершено. Всего сообщений: {len(updated_messages)}")
        
        return {"messages": updated_messages}
        
    except Exception as e:
        logger.exception(f"❌ [assistant_node] Критическая ошибка: {e}")
        error_message = AIMessage(content=f"Произошла ошибка: {str(e)}. Попробуйте ещё раз.")
        return {"messages": messages + [error_message]}


def tools_node(state: AgentState) -> Dict[str, Any]:
    """
    Узел выполнения инструментов.
    Извлекает tool_calls из последнего сообщения и выполняет соответствующие инструменты.
    
    Returns:
        Dict с обновлениями состояния
    """
    # ✅ Pydantic: getattr вместо .get()
    messages = getattr(state, 'messages', [])
    
    logger.info(f"🛠️ [tools_node] Начало. State fields: query={getattr(state, 'query', None)}, location={getattr(state, 'location', None)}")
    
    updates = {}
    tool_responses = []
    
    last_message = messages[-1] if messages else None
    
    if last_message and hasattr(last_message, "tool_calls") and last_message.tool_calls:
        logger.info(f"🔧 [tools_node] Обработка {len(last_message.tool_calls)} tool_calls")
        
        for tool_call in last_message.tool_calls:
            tool_call_id = tool_call.get("id")
            tool_name = tool_call.get("name")
            tool_args = tool_call.get("args", {})
            
            logger.info(f"📦 [tools_node] Выполняю {tool_name} с аргументами: {tool_args}")
            
            try:
                result = None
                
                if tool_name == "analyze_market_context":
                    query = tool_args.get("query")
                    location = tool_args.get("location")
                    if query:
                        result = analyze_market_context_func(query, location)
                        updates["market_context"] = result
                        logger.info(f"✅ [tools_node] analyze_market_context завершён")
                
                elif tool_name == "calculate_skills_gap":
                    current_skills = tool_args.get("current_skills", [])
                    # Берём market_context из аргументов или из состояния
                    mc_arg = tool_args.get("market_context")
                    mc_state = getattr(state, 'market_context', None)
                    market_context = mc_arg or (mc_state.model_dump() if hasattr(mc_state, 'model_dump') else mc_state)
                    
                    if current_skills and market_context:
                        result = calculate_skills_gap_func(current_skills, market_context)
                        updates["skills_gap"] = result
                        logger.info(f"✅ [tools_node] skills_gap сохранён: {result}")
                
                elif tool_name == "get_salary_insights":
                    mc_arg = tool_args.get("market_context")
                    mc_state = getattr(state, 'market_context', None)
                    market_context = mc_arg or (mc_state.model_dump() if hasattr(mc_state, 'model_dump') else mc_state)
                    if market_context:
                        result = get_salary_insights_func(market_context)
                        updates["market_context"] = {**getattr(state, 'market_context', {}), **result} if hasattr(state, 'market_context') else result
                        logger.info(f"✅ [tools_node] get_salary_insights завершён")

                elif tool_name == "tailor_resume_tool":
                    candidate = getattr(state, 'candidate', None)
                    
                    # 🔥 Берём навыки ИЗ STATE
                    declared_skills = []
                    if candidate and candidate.skills:
                        declared_skills = candidate.skills
                    resume_skills = getattr(state, 'resume_skills', []) or []
                    all_declared = list(set(declared_skills + resume_skills))
                    
                    vacancy_payload = tool_args.get("vacancy_payload", {})
                    candidate_resume = tool_args.get("candidate_resume", "")
                    
                    if all_declared or candidate_resume or vacancy_payload:
                        result = tailor_resume_tool_func(
                            candidate_resume=candidate_resume,
                            vacancy_payload=vacancy_payload,
                            declared_skills=all_declared  # ← Передаём!
                        )
                        updates["custom_resume"] = result
                        logger.info(f"✅ [tools_node] tailor_resume: match={result.get('match_percentage')}%")
                
                # 🆕 Создаём ToolMessage для передачи результата обратно в диалог
                if tool_call_id:
                    content = json.dumps(result, ensure_ascii=False) if result is not None else "No result"
                    tool_responses.append(
                        ToolMessage(content=content, tool_call_id=tool_call_id, name=tool_name)
                    )
                    logger.info(f"📤 [tools_node] Добавлен ToolMessage для {tool_name}")
                    
            except Exception as e:
                logger.exception(f"❌ [tools_node] Ошибка выполнения {tool_name}: {e}")
                if tool_call_id:
                    tool_responses.append(
                        ToolMessage(content=f"Error: {str(e)}", tool_call_id=tool_call_id, name=tool_name)
                    )
    else:
        logger.info(f"⚠️ [tools_node] Нет tool_calls в последнем сообщении")
    
    # 🆕 Добавляем ToolMessage в историю
    updated_messages = messages + tool_responses if tool_responses else messages
    
    # ✅ Возвращаем dict с обновлениями
    if updates:
        updates["messages"] = updated_messages
        logger.info(f"✅ [tools_node] Завершено. Обновления: {list(updates.keys())}, ToolMessages: {len(tool_responses)}")
        return updates
    
    return {"messages": updated_messages}


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def analyze_market_context_func(query: str, location: Optional[Dict] = None) -> Dict[str, Any]:
    """Синхронная обёртка для асинхронного поиска вакансий."""
    logger.info(f"🔄 [FUNC] analyze_market_context_func: query='{query}'")
    try:
        if vacancy_search_tool is None:
            logger.error("❌ [FUNC] vacancy_search_tool не инициализирован!")
            return {"error": "Инструмент поиска не доступен", "top_vacancies": []}
        result = asyncio.run(vacancy_search_tool.search_vacancies(query, location))
        logger.info(f"✅ [FUNC] Поиск завершён, результат: {type(result)}")
        return result
    except RuntimeError as e:
        if "already running" in str(e):
            logger.warning("⚠️ [FUNC] Event loop уже запущен, применяем nest_asyncio")
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(vacancy_search_tool.search_vacancies(query, location))
        raise
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"error": f"Ошибка поиска: {str(e)}", "top_vacancies": [], "salary_median": 0}


def calculate_skills_gap_func(current_skills: List[str], market_context: Dict) -> List[str]:
    """Синхронная версия расчёта разрыва навыков."""
    logger.info(f"🔄 [FUNC] calculate_skills_gap_func: {len(current_skills)} навыков")
    try:
        # Парсим market_skills
        skill_g_raw = market_context.get("skill_g", [])
        if isinstance(skill_g_raw, str):
            market_skills = [s.strip() for s in skill_g_raw.split(",") if s.strip()]
        elif isinstance(skill_g_raw, list):
            market_skills = skill_g_raw
        else:
            market_skills = []
        
        # 🔥 НОРМАЛИЗАЦИЯ ОБОИХ СПИСКОВ
        from .utils.skill_normalizer import normalize_skills
        candidate_normalized = set(s.lower() for s in normalize_skills(current_skills))
        market_normalized = set(s.lower() for s in normalize_skills(market_skills))
        
        gaps = [skill for skill in market_skills if skill.lower().strip() not in candidate_normalized]
        # 🔥 ЛОГИРОВАНИЕ ДЛЯ ОТЛАДКИ
        logger.info(f"✅ candidate_normalized: {candidate_normalized}")
        logger.info(f"✅ market_normalized: {market_normalized}")
        logger.info(f"✅ gaps (то, чего НЕ хватает): {gaps}")


        return gaps[:5]
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return []

def get_salary_insights_func(market_context: Dict) -> Dict[str, Any]:
    """Синхронная обёртка для get_salary_insights."""
    logger.info(f"🔄 [FUNC] get_salary_insights_func вызван")
    try:
        return {
            "median": market_context.get("salary_median", 0),
            "top_10": market_context.get("salary_top_10", 0),
            "range": market_context.get("market_range", [0, 0]),
            "currency": "RUB"
        }
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"median": 0, "top_10": 0, "range": [0, 0], "currency": "RUB", "error": str(e)}

def tailor_resume_tool_func(
    candidate_resume: str,
    vacancy_payload: Dict[str, Any]
) -> Dict[str, Any]:
    """Синхронная обёртка для tailor_resume."""
    logger.info(f"🔄 [FUNC] tailor_resume_tool_func вызван")
    try:
        return asyncio.run(
            vacancy_search_tool.tailor_resume(
                candidate_resume=candidate_resume,
                vacancy_payload=vacancy_payload
            )
        )
    except RuntimeError as e:
        if "already running" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(
                vacancy_search_tool.tailor_resume(
                    candidate_resume=candidate_resume,
                    vacancy_payload=vacancy_payload
                )
            )
        raise
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"error": str(e), "recommendations": []}

# =============================================================================
# ЭКСПОРТЫ
# =============================================================================

__all__ = [
    "assistant_node",
    "tools_node",
    "analyze_market_context",
    "calculate_skills_gap",
    "get_salary_insights",
    "generate_roadmap_tool",
    "tailor_resume_tool",
    "llm",
    "vacancy_search_tool",
    "analyze_market_context_func",
    "calculate_skills_gap_func",
    "get_salary_insights_func",
    "tailor_resume_tool_func",
]

# 🔥 ДОБАВЬ ЭТИ СТРОКИ (для дуальной конфигурации моделей):
__all__.extend(["get_llm_client", "GROQ_MODEL_FAST", "GROQ_MODEL_SMART"])

# ============================================================================
# ДУАЛЬНАЯ КОНФИГУРАЦИЯ МОДЕЛЕЙ (для масштабирования и контроля затрат)
# ============================================================================

import os
from langchain_groq import ChatGroq

# Базовая модель (8B) — для большинства задач
GROQ_MODEL_FAST = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
# Умная модель (70B) — только для сложных кейсов или fallback
GROQ_MODEL_SMART = os.getenv("GROQ_SMART_MODEL", "llama-3.3-70b-versatile")

def get_llm_client(model_preset: str = "fast") -> ChatGroq:
    """
    Возвращает LLM-клиент с нужной моделью.
    
    Args:
        model_preset: "fast" (8B, дёшево) или "smart" (70B, надёжно)
    
    Returns:
        ChatGroq instance с нужной моделью
    """
    model_name = GROQ_MODEL_FAST if model_preset == "fast" else GROQ_MODEL_SMART
    return ChatGroq(
        groq_api_key=os.getenv("GROQ_API_KEY"),
        model_name=model_name,
        temperature=0.1,
        max_tokens=2000  # Лимит на ответ для контроля затрат
    )