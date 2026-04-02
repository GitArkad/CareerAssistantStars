"""
Узлы (nodes) LangGraph-графа: assistant_node и tools_node.

ИСПРАВЛЕНИЯ:
1. asyncio.run() → _run_async_safe() (безопасный запуск внутри FastAPI event loop)
2. tools_node: ДОБАВЛЕНА обработка generate_roadmap_tool (раньше игнорировался)
3. tailor_resume_tool_func: добавлен параметр declared_skills
4. assistant_node: candidate возвращается как dict (не Pydantic-объект)
5. Защита от зацикливания через consecutive_tool_calls
6. Согласован ключ skill_gaps / skill_g → используется skill_gaps
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, List, Optional

from langchain_groq import ChatGroq
from langchain_core.tools import tool
from langchain_core.messages import (
    HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage,
)

from .state import AgentState, CandidateProfile, MarketContext
from .tools import vacancy_search_tool
from .utils.skill_normalizer import (
    normalize_skills, extract_skills_from_text, merge_skills,
)
from .utils.normalizers import get_city_aliases, get_country_aliases, normalize_city, normalize_country

# ═══════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ LLM
# ═══════════════════════════════════════════════════════════════════════

groq_api_key = os.getenv("GROQ_API_KEY")
model_name = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not groq_api_key:
    raise ValueError("GROQ_API_KEY not found. Установите в .env или окружении.")

logger.info(f"🔄 Инициализация LLM: {model_name}")
llm = ChatGroq(groq_api_key=groq_api_key, model_name=model_name)

# ═══════════════════════════════════════════════════════════════════════
# ДУАЛЬНАЯ КОНФИГУРАЦИЯ МОДЕЛЕЙ
# ═══════════════════════════════════════════════════════════════════════

GROQ_MODEL_FAST = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MODEL_SMART = os.getenv("GROQ_SMART_MODEL", "llama-3.3-70b-versatile")


def get_llm_client(model_preset: str = "fast") -> ChatGroq:
    """Возвращает LLM-клиент: 'fast' (8B) или 'smart' (70B)."""
    chosen = GROQ_MODEL_FAST if model_preset == "fast" else GROQ_MODEL_SMART
    return ChatGroq(
        groq_api_key=os.getenv("GROQ_API_KEY"),
        model_name=chosen,
        temperature=0.1,
        max_tokens=2000,
    )


# ═══════════════════════════════════════════════════════════════════════
# БЕЗОПАСНЫЙ ЗАПУСК ASYNC ИЗ SYNC-КОНТЕКСТА
# ═══════════════════════════════════════════════════════════════════════

def _run_async_safe(coro):
    """
    Безопасный запуск корутины из синхронного кода.
    Если event loop уже запущен (FastAPI) — применяет nest_asyncio.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)


def _format_salary_rub(vacancy: Dict[str, Any]) -> str:
    salary_from = vacancy.get("salary_from_rub")
    salary_to = vacancy.get("salary_to_rub")

    if salary_from is None and salary_to is None:
        salary_from = vacancy.get("salary_from")
        salary_to = vacancy.get("salary_to")
        currency = vacancy.get("currency")
        if currency and currency != "RUB" and (salary_from is not None or salary_to is not None):
            return "з/п указана в локальной валюте"

    if salary_from is not None and salary_to is not None:
        return f"{salary_from:,}–{salary_to:,} ₽".replace(",", " ")
    if salary_from is not None:
        return f"от {salary_from:,} ₽".replace(",", " ")
    if salary_to is not None:
        return f"до {salary_to:,} ₽".replace(",", " ")
    return "з/п не указана"


def _has_impossible_location(query: Optional[str]) -> bool:
    if not query:
        return False

    query_lower = query.lower()
    impossible_markers = [
        "на луне",
        "на луну",
        "луна",
        "на марсе",
        "на марс",
        "марс",
        "в космосе",
        "космос",
    ]
    return any(marker in query_lower for marker in impossible_markers)


# ═══════════════════════════════════════════════════════════════════════
# ИНСТРУМЕНТЫ (TOOLS) — декларации для bind_tools
# ═══════════════════════════════════════════════════════════════════════

@tool
def analyze_market_context(query: Optional[str] = None, location: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Анализирует рынок вакансий по поисковому запросу и локации.

    Args:
        query: Поисковый запрос (например, "Python разработчик", "Data Scientist")
        location: Словарь с фильтрами локации {'city': 'Москва', 'country': 'Россия'}

    Returns:
        Dict с top_vacancies, salary_median, salary_top_10, market_range, skill_gaps, match_score
    """
    query = query or "вакансии"
    logger.info(f"🔍 [TOOL] analyze_market_context: query='{query}', location={location}")
    try:
        result = _run_async_safe(vacancy_search_tool.search_vacancies(query, location))
        if isinstance(result, dict) and "error" in result:
            logger.error(f"❌ [TOOL] Ошибка поиска: {result['error']}")
        else:
            count = len(result.get("top_vacancies", []))
            logger.info(f"✅ [TOOL] Найдено вакансий: {count}, медиана: {result.get('salary_median')}")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка в analyze_market_context: {e}")
        return {"error": str(e), "top_vacancies": [], "salary_median": 0}


@tool
def calculate_skills_gap(current_skills: List[str], market_context: Dict) -> List[str]:
    """
    Вычисляет разрыв между навыками пользователя и требованиями рынка.

    Args:
        current_skills: Текущие навыки кандидата
        market_context: Результат analyze_market_context
    """
    logger.info(f"🧠 [TOOL] calculate_skills_gap: навыков={len(current_skills)}")
    try:
        # Берём skill_gaps из market_context
        market_skills_raw = market_context.get("skill_gaps", [])
        if isinstance(market_skills_raw, str):
            market_skills = [s.strip() for s in market_skills_raw.split(",") if s.strip()]
        elif isinstance(market_skills_raw, list):
            market_skills = market_skills_raw
        else:
            market_skills = []

        candidate_normalized = set(s.lower() for s in normalize_skills(current_skills))
        gaps = [s for s in market_skills if s.lower().strip() not in candidate_normalized]

        logger.info(f"✅ [TOOL] Разрывов: {len(gaps)} → {gaps}")
        return gaps[:5]
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка: {e}")
        return []


@tool
def get_salary_insights(market_context: Dict) -> Dict[str, Any]:
    """
    Получает информацию о зарплатах из рыночного контекста.

    Args:
        market_context: Результат analyze_market_context
    """
    logger.info(f"💰 [TOOL] get_salary_insights")
    try:
        return {
            "median": market_context.get("salary_median", 0),
            "top_10": market_context.get("salary_top_10", 0),
            "range": market_context.get("market_range", [0, 0]),
            "currency": "RUB",
        }
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка: {e}")
        return {"median": 0, "top_10": 0, "range": [0, 0], "currency": "RUB", "error": str(e)}


@tool
def generate_roadmap_tool(
    current_skills: List[str],
    market_context: Dict,
    target_role: Optional[str] = None,
    timeframe_months: int = 3,
) -> Dict[str, Any]:
    """
    Генерирует план развития навыков на основе анализа рынка.

    Args:
        current_skills: Текущие навыки кандидата
        market_context: Результат analyze_market_context
        target_role: Целевая роль (например, "ML Engineer")
        timeframe_months: Горизонт планирования в месяцах
    """
    logger.info(f"🗺️ [TOOL] generate_roadmap: skills={current_skills}, role={target_role}")
    try:
        current_skills = normalize_skills(current_skills)
        result = _run_async_safe(
            vacancy_search_tool.generate_roadmap(
                current_skills=current_skills,
                market_context=market_context,
                target_role=target_role,
                timeframe_months=timeframe_months,
            )
        )
        logger.info(f"✅ [TOOL] Roadmap: {len(result.get('skill_priorities', []))} навыков")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка: {e}")
        return {"error": str(e), "skill_priorities": []}


@tool
def tailor_resume_tool(
    candidate_resume: Optional[str] = None,
    vacancy_payload: Optional[Dict[str, Any]] = None,
    declared_skills: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Адаптирует резюме под требования вакансии.

    Args:
        candidate_resume: Текст резюме (или "")
        vacancy_payload: Объект вакансии с полями title, company, requirements/skills, url
        declared_skills: Список навыков пользователя (МАССИВ, не строка!)

    Пример вызова:
    {
      "candidate_resume": "Разрабатывал модели на Python",
      "vacancy_payload": {"title": "ML Engineer", "requirements": ["Python", "PyTorch"]},
      "declared_skills": ["Python", "SQL"]
    }
    """
    logger.info(f"✏️ [TOOL] tailor_resume_tool: skills={declared_skills}")
    try:
        result = _run_async_safe(
            vacancy_search_tool.tailor_resume(
                candidate_resume=candidate_resume,
                vacancy_payload=vacancy_payload or {},
                declared_skills=declared_skills,
            )
        )
        logger.info(f"✅ [TOOL] tailor_resume: match={result.get('match_percentage')}%")
        return result
    except Exception as e:
        logger.exception(f"❌ [TOOL] Ошибка: {e}")
        return {"status": "error", "message": str(e), "recommendations": []}


# ═══════════════════════════════════════════════════════════════════════
# СПИСОК ИНСТРУМЕНТОВ ДЛЯ bind_tools
# ═══════════════════════════════════════════════════════════════════════

ALL_TOOLS = [
    analyze_market_context,
    calculate_skills_gap,
    get_salary_insights,
    generate_roadmap_tool,
    tailor_resume_tool,
]


# ═══════════════════════════════════════════════════════════════════════
# ASSISTANT NODE
# ═══════════════════════════════════════════════════════════════════════

def assistant_node(state: AgentState) -> Dict[str, Any]:
    """
    Основной узел — вызывает LLM с инструментами.
    Возвращает dict с обновлениями (LangGraph merge).
    """
    query = getattr(state, "query", None)
    messages = getattr(state, "messages", [])
    current_iter = getattr(state, "iteration_count", 0)
    candidate = getattr(state, "candidate", None)

    # ── Bypass LLM: готовый ответ уже сформирован в tools_node ──
    agent_response = getattr(state, "agent_response", None)
    if agent_response and current_iter > 0:
        logger.info(f"⚡ [assistant_node] Bypass LLM — используем agent_response")
        return {
            "messages": [AIMessage(content=agent_response)],
            "iteration_count": current_iter + 1,
            "agent_response": None,
        }

    # ── Защита от зацикливания ──
    if current_iter >= getattr(state, "max_iterations", 5):
        return {
            "messages": [AIMessage(content="Я проверил данные. Вот лучшие результаты, которые удалось найти.")],
            "iteration_count": current_iter + 1,
        }

    # ── Извлечение навыков из запроса пользователя ──
    candidate_dict = None
    if candidate:
        candidate_dict = candidate.model_dump() if hasattr(candidate, "model_dump") else dict(candidate)

    if query:
        extracted = extract_skills_from_text(query)
        if extracted:
            logger.info(f"🔍 Навыки из запроса: {extracted}")
            if candidate_dict:
                existing = candidate_dict.get("skills", []) or []
                merged = merge_skills(existing, extracted)
                candidate_dict["skills"] = merged
                logger.info(f"📦 Объединённые навыки: {merged}")
            else:
                candidate_dict = {"skills": extracted}

    # ── Объединяем с resume_skills ──
    resume_skills = getattr(state, "resume_skills", [])
    if candidate_dict and resume_skills:
        existing = candidate_dict.get("skills", []) or []
        merged = merge_skills(existing, resume_skills)
        candidate_dict["skills"] = merged

    logger.info(f"🎯 [assistant_node] Query: {query}, Сообщений: {len(messages)}, Итерация: {current_iter}")

    # ── Контекст для промпта ──
    context_parts = []
    mc = getattr(state, "market_context", None)
    if mc and isinstance(mc, dict) and mc.get("top_vacancies"):
        context_parts.append(f"Результаты поиска вакансий уже есть (найдено {len(mc['top_vacancies'])}). Используй их.")

    sg = getattr(state, "skills_gap", None)
    if sg:
        context_parts.append(f"Skills gap: {', '.join(sg[:5])}")

    candidate_skills_str = ""
    if candidate_dict and candidate_dict.get("skills"):
        candidate_skills_str = f"Навыки кандидата: {', '.join(candidate_dict['skills'])}"

    extra_context = "\n".join(context_parts) if context_parts else ""

#     system_prompt = f"""
#     Ты — карьерный ассистент, помогающий специалистам анализировать рынок труда.

#     Твои возможности:
#     1. Анализировать вакансии по запросу и локации (analyze_market_context)
#     2. Рассчитывать разрыв навыков (calculate_skills_gap)
#     3. Информация о зарплатах (get_salary_insights)
#     4. Генерировать план развития (generate_roadmap_tool)
#     5. Адаптировать резюме под вакансию (tailor_resume_tool)
#     ПРАВИЛА ИЗВЛЕЧЕНИЯ ЛОКАЦИИ:
#     - Если пользователь указал ТОЛЬКО страну (например, "в России"), передавай в аргумент location ТОЛЬКО страну: {'country': 'Россия'}. 
#     - Никогда не додумывай город, если он не упомянут в сообщении. 
#     - Если город не указан, поле 'city' в словаре location должно отсутствовать.

#     {candidate_skills_str}
#     {extra_context}

#     ПРАВИЛА:
#     - Если пользователь указал навыки — НЕ рекомендуй их снова.
#     - Используй ТОЛЬКО результаты инструментов. НЕ ВЫДУМЫВАЙ вакансии, зарплаты, компании.
#     - После получения ToolMessage сформулируй ответ НА ОСНОВЕ реальных данных.
#     - Если skill_gap пуст — скажи, что навыков достаточно.
#     - НЕ ВЫДУМЫВАЙ, ТОЛЬКО РЕАЛЬНЫЕ ДАННЫЕ.

#     ДОСТУП К ДАННЫМ ВАКАНСИЙ:
#         Вакансии находятся в market_context.top_vacancies[].payload:
#         - title, company, city, country
#         - salary_from, salary_to
#         - skills или requirements
#         - url

#         ФОРМАТ ВЫВОДА ВАКАНСИЙ (ТОП-5):
#         1. **[title]**
#         Компания: [company]
#         Локация: [city], [country]
#         Зарплата: [salary_from] — [salary_to] ₽
#         Стек: [skills]
#         Ссылка: [url]

#     ПРАВИЛА ВЫЗОВА ИНСТРУМЕНТОВ:
#     - "адаптируй резюме", "улучши резюме" → tailor_resume_tool
#     - "что учить", "план развития", "roadmap" → generate_roadmap_tool
#     - поиск вакансий → analyze_market_context
#     - После получения данных — ОТВЕЧАЙ НА ОСНОВЕ ДАННЫХ, не выдумывай.

#     ФОРМАТ ОТВЕТА для tailor_resume_tool:
#     1. ТОП-5 ключевых навыков из вакансии.
#     2. Аудит: что есть, что слабо.
#     3. Блок «Достижения» по формуле Google: «Сделал [X], что измеряется [Y], путём внедрения [Z]».
#     4. Недостающие ключевые слова.

#     Отвечай на русском, по делу, структурированно.
# """
    system_prompt = f"""
### РОЛЬ
Ты — Карьерный AI-Ассистент.
Твоя задача: определить намерение пользователя, выбрать наиболее подходящий ОДИН инструмент и выдать полезный структурированный ответ на русском языке.

### КОНТЕКСТ ПОЛЬЗОВАТЕЛЯ
- Текущие навыки: {candidate_skills_str}
- Доп. контекст: {extra_context}

### ГЛАВНЫЙ ПРИНЦИП
Отвечай только на основе данных из состояния и результатов инструментов.
Если данных недостаточно, прямо скажи об этом.
Ничего не придумывай.

### ПРАВИЛА ВЫБОРА ДЕЙСТВИЯ
Выбирай только одно действие на запрос:

1. Поиск вакансий, анализ рынка, поиск работы
→ analyze_market_context

2. План обучения, roadmap, что учить дальше
→ generate_roadmap_tool
Но:
- если в контексте уже есть выбранная вакансия, строй план по выбранной вакансии
- если выбранной вакансии нет, но есть top vacancies, строй план по ним
- если вакансий нет, не выдумывай данные, сообщи что сначала нужен поиск вакансий

3. Улучшение или адаптация резюме
→ tailor_resume_tool

4. Анализ недостающих навыков
→ calculate_skills_gap

5. Зарплатная аналитика
→ get_salary_insights

6. Подготовка к интервью
→ если в состоянии уже есть сценарий интервью, продолжай его
→ если нет данных для интервью, сообщи что нужна вакансия или контекст роли
Не выдумывай отдельный tool, если он не был реально предоставлен системой.

### КРИТИЧЕСКИЕ ОГРАНИЧЕНИЯ
- Не придумывай вакансии, компании, зарплаты, страны, города или навыки.
- Используй только то, что пришло из ToolMessage и state.
- Если инструмент вернул пустой результат, честно скажи, что данных не найдено.
- Не рекомендуй изучать навыки, которые уже есть у пользователя в "Текущих навыках".
- Не вызывай второй инструмент, если задача уже может быть завершена на основе текущего state или результата первого инструмента.
- Если город или страна не были явно указаны пользователем, не добавляй их в аргументы инструмента.

### ПРИОРИТЕТ ИСТОЧНИКОВ ДАННЫХ
Используй данные в таком порядке:
1. ToolMessage / результат инструмента
2. State / extra_context
3. Никаких догадок сверх этого

### ФОРМАТЫ ОТВЕТА

#### 1. ВАКАНСИИ
Если вакансии найдены:
Найдено вакансий: {{N}}

Для каждой вакансии, максимум 5:
{{title}}
Компания: {{company}}
Локация: {{city}}, {{country}}
Зарплата: {{formatted_salary}}
Стек: {{skills}}
Ссылка: {{url}}

Если есть агрегаты:
Медиана з/п по рынку: {{salary_median}} ₽

Если вакансий нет:
Вакансий не найдено. Попробуйте изменить запрос, убрать часть фильтров или расширить локацию.

#### 2. ROADMAP
Если есть данные по вакансии или топ-вакансиям:
Цель: {{target_role}}

Приоритет навыков:
- {{skill}}: встречается в {{market_demand}} вакансиях, срок освоения ~{{estimated_weeks}} нед.
- {{skill}}: встречается в {{market_demand}} вакансиях, срок освоения ~{{estimated_weeks}} нед.

Если есть прогноз:
Прогноз по зарплате: {{from_salary}} → {{to_salary}} ₽

Итог:
- Что учить сначала
- Что учить следующим этапом
- На какой тип ролей это поможет выйти

Если данных нет:
Невозможно построить план обучения без вакансий. Сначала выполните поиск вакансий или выберите конкретную вакансию.

#### 3. RESUME
Целевая позиция: {{title}}

- Что уже хорошо совпадает
- Что нужно добавить
- Какие ключевые слова стоит включить
- Как усилить формулировки опыта
- Краткий совет по summary

Если данных по вакансии нет:
Для адаптации резюме нужна выбранная или найденная вакансия.

#### 4. INTERVIEW
- На что обратить внимание по стеку
- Какие темы наиболее вероятны
- Сильные стороны кандидата
- Зоны риска
- Краткий совет по подготовке

#### 5. SKILL GAP
Если данные есть:
Недостающие навыки:
- {{skill_1}}
- {{skill_2}}
- {{skill_3}}

Если разрывов нет:
Ваш стек в целом соответствует найденным вакансиям.

Если вакансий нет:
Недостаточно данных для анализа skill gap.

#### 6. SALARY
- Медиана по рынку: {{median}} ₽
- Верхняя граница по найденным вакансиям: {{top_10}} ₽
- Диапазон: {{range_from}} — {{range_to}} ₽

Если данных нет:
Недостаточно данных для зарплатной аналитики.

### СТИЛЬ
- Русский язык
- Кратко, по делу, структурированно
- Без воды
- Без вымысла
- Ответ должен быть удобен для копирования в заметки
    """
    system_message = SystemMessage(content=system_prompt)

    # ── Конвертация сообщений ──
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

    logger.info(f"📦 [assistant_node] {len(all_messages)} сообщений для LLM")

    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    try:
        response = llm_with_tools.invoke(all_messages)

        content_preview = response.content[:200] if response.content else "EMPTY"
        logger.info(f"📝 [LLM] Content: '{content_preview}...'")
        logger.info(f"🔧 [LLM] Tool calls: {response.tool_calls if hasattr(response, 'tool_calls') else 'N/A'}")

        # Фолбэк: пустой ответ, но есть данные
        if not response.content and not (hasattr(response, "tool_calls") and response.tool_calls):
            if mc:
                vacancies = len(mc.get("top_vacancies", []))
                median = mc.get("salary_median", 0)
                fallback = f"🔍 По вашему запросу найдено {vacancies} вакансий. Медианная зарплата: {median:,} ₽."
                if sg:
                    fallback += f"\n\n📚 Рекомендуемые навыки: {', '.join(sg[:3])}"
                response = AIMessage(content=fallback)

        # ── Обновление consecutive_tool_calls ──
        has_tool_calls = hasattr(response, "tool_calls") and response.tool_calls
        prev_consecutive = getattr(state, "consecutive_tool_calls", 0)

        updates = {
            "messages": [response],
            "iteration_count": current_iter + 1,
            "consecutive_tool_calls": (prev_consecutive + 1) if has_tool_calls else 0,
        }

        # Сохраняем обновлённого кандидата
        if candidate_dict:
            updates["candidate"] = CandidateProfile(**candidate_dict)

        return updates

    except Exception as e:
        logger.exception(f"❌ [assistant_node] Ошибка: {e}")
        error_msg = AIMessage(content=f"Произошла ошибка: {str(e)}. Попробуйте ещё раз.")
        return {
            "messages": [error_msg],
            "iteration_count": current_iter + 1,
        }


# ═══════════════════════════════════════════════════════════════════════
# TOOLS NODE
# ═══════════════════════════════════════════════════════════════════════

def tools_node(state: AgentState) -> Dict[str, Any]:
    """
    Выполняет tool_calls из последнего сообщения LLM.
    Возвращает dict с обновлениями состояния.

    ИСПРАВЛЕНИЯ:
    - Добавлена обработка generate_roadmap_tool (раньше отсутствовала!)
    - tailor_resume_tool_func принимает declared_skills
    - Результаты сохраняются в правильные поля state
    """
    messages = getattr(state, "messages", [])

    logger.info(f"🛠️ [tools_node] Начало. query={getattr(state, 'query', None)}")

    updates: Dict[str, Any] = {}
    tool_responses: List[ToolMessage] = []

    last_message = messages[-1] if messages else None

    if not (last_message and hasattr(last_message, "tool_calls") and last_message.tool_calls):
        logger.info("⚠️ [tools_node] Нет tool_calls")
        return {"messages": []}

    logger.info(f"🔧 [tools_node] {len(last_message.tool_calls)} tool_calls")

    for tool_call in last_message.tool_calls:
        tool_call_id = tool_call.get("id")
        tool_name = tool_call.get("name")
        tool_args = tool_call.get("args", {})

        logger.info(f"📦 [tools_node] {tool_name}: {tool_args}")

        try:
            result = None

            # ──────────────────────────────────────────
            # 1. ПОИСК ВАКАНСИЙ
            # ──────────────────────────────────────────
            if tool_name == "analyze_market_context":
                query = tool_args.get("query") or getattr(state, "query", "") or "вакансии"
                location = tool_args.get("location") or getattr(state, "location", None)
                if _has_impossible_location(query):
                    updates["market_context"] = {
                        "match_score": 0.0,
                        "skill_gaps": [],
                        "top_vacancies": [],
                        "salary_median": 0,
                        "salary_top_10": 0,
                        "market_range": [0, 0],
                    }
                    updates["top_vacancies"] = []
                    updates["agent_response"] = (
                        "К сожалению, по вашему запросу в указанной локации вакансий не найдено. "
                        "Попробуйте изменить локацию на реальный город или страну."
                    )
                    result = {
                        "status": "no_vacancies",
                        "message": updates["agent_response"],
                        "top_vacancies": [],
                    }
                    logger.info("🚫 [tools_node] Обнаружена невозможная локация в запросе: %s", query)
                else:
                    # Проверяем: реально ли пользователь упоминал этот город/страну
                    user_query = (getattr(state, "query", "") or "").lower()
                    if location and isinstance(location, dict):
                        city = location.get("city")
                        country = location.get("country")

                        def alias_in_query(aliases: list, query: str) -> bool:
                            for alias in aliases:
                                if alias in query:
                                    return True
                                # Проверяем корень (без последних 1-2 букв) для склонений
                                if len(alias) > 4 and alias[:-1] in query:
                                    return True
                                if len(alias) > 5 and alias[:-2] in query:
                                    return True
                            return False

                        city_mentioned = city and alias_in_query(
                            get_city_aliases(normalize_city(city)), user_query
                        )
                        country_mentioned = country and alias_in_query(
                            get_country_aliases(normalize_country(country)), user_query
                        )
                        clean_location = {}
                        if city_mentioned:
                            clean_location["city"] = city
                        if country_mentioned:
                            clean_location["country"] = country
                        location = clean_location if clean_location else None
                        if location != tool_args.get("location"):
                            logger.info(f"🔍 [tools_node] Локация скорректирована: {tool_args.get('location')} → {location}")
                    if query:
                        result = analyze_market_context_func(query, location)
                        updates["market_context"] = result
                        vacancies = result.get("top_vacancies", []) if result else []
                        if vacancies:
                            updates["top_vacancies"] = vacancies
                        else:
                            updates["top_vacancies"] = []
                        logger.info(f"✅ [tools_node] analyze_market_context: {len(vacancies)} вакансий")
                        # Формируем читаемый ответ и сохраняем для bypass LLM
                        if vacancies:
                            lines = [f"Найдено вакансий: {len(vacancies)}\n"]
                            for i, v in enumerate(vacancies, 1):
                                salary = _format_salary_rub(v)
                                skills_str = ", ".join(v.get('skills') or []) or "не указаны"
                                lines.append(f"**{i}. {v.get('title','?')}**")
                                lines.append(f"Компания: {v.get('company','?')} | Локация: {v.get('city','?')}")
                                lines.append(f"Зарплата: {salary}")
                                lines.append(f"Стек: {skills_str}")
                                url = v.get('url','')
                                if url:
                                    lines.append(f"Ссылка: {url}")
                                lines.append("")
                            mc_data = updates.get("market_context", {})
                            if isinstance(mc_data, dict) and mc_data.get("salary_median"):
                                lines.append(f"Медиана з/п по рынку: {mc_data['salary_median']:,} ₽".replace(",", " "))
                            formatted = "\n".join(lines)
                            updates["agent_response"] = formatted
                            result = f"НАЙДЕНО ВАКАНСИЙ: {len(vacancies)}\n{formatted}"
                        else:
                            error_text = result.get("error") if isinstance(result, dict) else None
                            if error_text:
                                updates["agent_response"] = (
                                    f"Не удалось выполнить поиск вакансий: {error_text}"
                                )
                            else:
                                updates["agent_response"] = (
                                    "К сожалению, по вашему запросу в данной локации вакансий не найдено. "
                                    "Попробуйте расширить поиск, изменить роль или убрать фильтр по городу."
                                )
                            result = {
                                "status": "no_vacancies",
                                "message": updates["agent_response"],
                                "top_vacancies": [],
                            }

            # ──────────────────────────────────────────
            # 2. РАСЧЁТ SKILL GAP
            # ──────────────────────────────────────────
            elif tool_name == "calculate_skills_gap":
                
                current_skills = tool_args.get("current_skills", [])
                # Берём market_context из аргументов или из state
                mc_arg = tool_args.get("market_context")
                mc_state = getattr(state, "market_context", None)
                market_ctx = mc_arg or mc_state
                if isinstance(market_ctx, dict) is False and hasattr(market_ctx, "model_dump"):
                    market_ctx = market_ctx.model_dump()

                if current_skills and market_ctx:
                    result = calculate_skills_gap_func(current_skills, market_ctx)
                    updates["skills_gap"] = result
                    updates["missing_skills"] = result
                    logger.info(f"✅ [tools_node] skills_gap: {result}")

            # ──────────────────────────────────────────
            # 3. ЗАРПЛАТНАЯ АНАЛИТИКА
            # ──────────────────────────────────────────
            elif tool_name == "get_salary_insights":
                mc_arg = tool_args.get("market_context")
                mc_state = getattr(state, "market_context", None)
                market_ctx = mc_arg or mc_state
                if isinstance(market_ctx, dict) is False and hasattr(market_ctx, "model_dump"):
                    market_ctx = market_ctx.model_dump()

                if market_ctx:
                    result = get_salary_insights_func(market_ctx)
                    logger.info(f"✅ [tools_node] salary_insights: {result}")

            # ──────────────────────────────────────────
            # 4. ГЕНЕРАЦИЯ ROADMAP (БЫЛО ПРОПУЩЕНО!)
            # ──────────────────────────────────────────
            elif tool_name == "generate_roadmap_tool":
                current_skills = tool_args.get("current_skills", [])
                # Приоритет: 1) updates (параллельный analyze_market_context) 2) state 3) args LLM
                mc_updates = updates.get("market_context")
                mc_state = getattr(state, "market_context", None)
                if isinstance(mc_updates, dict) and mc_updates.get("top_vacancies"):
                    market_ctx = mc_updates
                    logger.info(f"✅ [tools_node] roadmap: используем market_context из updates ({len(mc_updates.get('top_vacancies', []))} вакансий)")
                elif isinstance(mc_state, dict) and mc_state.get("top_vacancies"):
                    market_ctx = mc_state
                    logger.info(f"✅ [tools_node] roadmap: используем market_context из state ({len(mc_state.get('top_vacancies', []))} вакансий)")
                else:
                    market_ctx = tool_args.get("market_context") or {}
                    logger.warning(f"⚠️ [tools_node] roadmap: market_context пуст везде, используем из args LLM")
                target_role = tool_args.get("target_role")
                timeframe = tool_args.get("timeframe_months", 3)

                # Дополняем навыки из state
                state_skills = getattr(state, "current_skills", [])
                candidate = getattr(state, "candidate", None)
                cand_skills = []
                if candidate:
                    cand_skills = getattr(candidate, "skills", []) or []

                all_skills = list(set(current_skills + state_skills + cand_skills))

                if all_skills and market_ctx:
                    result = generate_roadmap_func(
                        current_skills=all_skills,
                        market_context=market_ctx,
                        target_role=target_role,
                        timeframe_months=timeframe,
                    )
                    updates["roadmap"] = result
                    logger.info(f"✅ [tools_node] roadmap: {len(result.get('skill_priorities', []))} навыков")

                    # ⚡ Bypass LLM — формируем ответ прямо здесь
                    priorities = result.get("skill_priorities", [])
                    if priorities:
                        lines = ["**План развития навыков:**\n"]
                        for p in priorities[:5]:
                            skill = p.get("skill", "")
                            demand = p.get("market_demand", 0)
                            weeks = p.get("estimated_weeks", "?")
                            salary_impact = p.get("avg_salary_impact")
                            roles = ", ".join(p.get("seen_in_roles", [])[:2])
                            line = f"• **{skill}** — встречается в {demand} вак., ~{weeks} нед."
                            if salary_impact:
                                line += f", ср. з/п {salary_impact:,}".replace(",", " ") + " ₽"
                            if roles:
                                line += f" ({roles})"
                            lines.append(line)

                        salary_range = result.get("expected_salary_range", [])
                        if salary_range and len(salary_range) == 2 and salary_range[0]:
                            lines.append(
                                f"\nПрогноз з/п: {salary_range[0]:,} → {salary_range[1]:,} ₽".replace(",", " ")
                            )
                        growth = result.get("growth_explanation")
                        if growth:
                            lines.append(f"📈 {growth}")

                        updates["agent_response"] = "\n".join(lines)
                else:
                    result = {"error": "Нет навыков или market_context для roadmap", "skill_priorities": []}

            # ──────────────────────────────────────────
            # 5. АДАПТАЦИЯ РЕЗЮМЕ
            # ──────────────────────────────────────────
            elif tool_name == "tailor_resume_tool":
                candidate = getattr(state, "candidate", None)

                # Навыки из state
                declared_skills = []
                if candidate and hasattr(candidate, "skills") and candidate.skills:
                    declared_skills = list(candidate.skills)

                resume_skills = getattr(state, "resume_skills", []) or []
                all_declared = list(set(declared_skills + resume_skills))

                vacancy_payload = tool_args.get("vacancy_payload", {})
                candidate_resume = tool_args.get("candidate_resume") or getattr(state, "candidate_resume", "")

                # Если LLM не передал vacancy_payload, берём из state
                if not vacancy_payload:
                    mc_state = getattr(state, "market_context", None)
                    if mc_state and mc_state.get("top_vacancies"):
                        first_vacancy = mc_state["top_vacancies"][0]
                        if isinstance(first_vacancy, dict):
                            vacancy_payload = first_vacancy.get("payload", first_vacancy)

                result = tailor_resume_tool_func(
                    candidate_resume=candidate_resume or "",
                    vacancy_payload=vacancy_payload,
                    declared_skills=all_declared,
                )
                updates["custom_resume"] = result
                logger.info(f"✅ [tools_node] tailor_resume: match={result.get('match_percentage')}%")

            else:
                logger.warning(f"⚠️ [tools_node] Неизвестный инструмент: {tool_name}")
                result = {"error": f"Неизвестный инструмент: {tool_name}"}

            # Создаём ToolMessage
            if tool_call_id:
                content = json.dumps(result, ensure_ascii=False, default=str) if result is not None else '{"error": "No result"}'
                tool_responses.append(
                    ToolMessage(content=content, tool_call_id=tool_call_id, name=tool_name)
                )

        except Exception as e:
            logger.exception(f"❌ [tools_node] Ошибка {tool_name}: {e}")
            if tool_call_id:
                tool_responses.append(
                    ToolMessage(
                        content=json.dumps({"error": str(e)}, ensure_ascii=False),
                        tool_call_id=tool_call_id,
                        name=tool_name,
                    )
                )

    # Добавляем ToolMessages
    updates["messages"] = tool_responses
    logger.info(f"✅ [tools_node] Готово. Обновления: {list(updates.keys())}, ToolMessages: {len(tool_responses)}")
    return updates


# ═══════════════════════════════════════════════════════════════════════
# СИНХРОННЫЕ ОБЁРТКИ (для tools_node)
# ═══════════════════════════════════════════════════════════════════════

def analyze_market_context_func(query: str, location: Optional[Dict] = None) -> Dict[str, Any]:
    """Синхронная обёртка для search_vacancies."""
    logger.info(f"🔄 [FUNC] analyze_market_context_func: query='{query}'")
    try:
        if vacancy_search_tool is None:
            return {"error": "Инструмент поиска не доступен", "top_vacancies": []}
        return _run_async_safe(vacancy_search_tool.search_vacancies(query, location))
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"error": str(e), "top_vacancies": [], "salary_median": 0}


def calculate_skills_gap_func(current_skills: List[str], market_context: Dict) -> List[str]:
    """Синхронная версия расчёта skill gap."""
    logger.info(f"🔄 [FUNC] calculate_skills_gap_func: {len(current_skills)} навыков")
    try:
        # Если market_context пришёл строкой (LLM передал ToolMessage текст) — берём из state
        if not isinstance(market_context, dict):
            logger.warning(f"⚠️ market_context не dict, тип: {type(market_context)}")
            return []
        # Собираем навыки из вакансий
        market_skills = []
        for vac in market_context.get("top_vacancies", []):
            skills_raw = vac.get("skills") or vac.get("requirements") or []
            if isinstance(skills_raw, str):
                skills_raw = [s.strip() for s in skills_raw.split(",") if s.strip()]
            if isinstance(skills_raw, list):
                market_skills.extend(skills_raw)

        # Также из skill_gaps если есть
        skill_gaps_field = market_context.get("skill_gaps", [])
        if isinstance(skill_gaps_field, list):
            market_skills.extend(skill_gaps_field)

        candidate_normalized = set(s.lower() for s in normalize_skills(current_skills))
        market_normalized = normalize_skills(list(set(market_skills)))

        gaps = [s for s in market_normalized if s.lower() not in candidate_normalized]

        logger.info(f"✅ candidate: {candidate_normalized}")
        logger.info(f"✅ market: {set(s.lower() for s in market_normalized)}")
        logger.info(f"✅ gaps: {gaps}")

        return gaps[:5]
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return []


def get_salary_insights_func(market_context: Dict) -> Dict[str, Any]:
    """Синхронная обёртка для salary insights."""
    try:
        return {
            "median": market_context.get("salary_median", 0),
            "top_10": market_context.get("salary_top_10", 0),
            "range": market_context.get("market_range", [0, 0]),
            "currency": "RUB",
        }
    except Exception as e:
        return {"median": 0, "top_10": 0, "range": [0, 0], "currency": "RUB", "error": str(e)}


def generate_roadmap_func(
    current_skills: List[str],
    market_context: Dict,
    target_role: Optional[str] = None,
    timeframe_months: int = 3,
) -> Dict[str, Any]:
    """Синхронная обёртка для generate_roadmap."""
    logger.info(f"🔄 [FUNC] generate_roadmap_func: {len(current_skills)} навыков, role={target_role}")
    try:
        return _run_async_safe(
            vacancy_search_tool.generate_roadmap(
                current_skills=current_skills,
                market_context=market_context,
                target_role=target_role,
                timeframe_months=timeframe_months,
            )
        )
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"error": str(e), "skill_priorities": []}


def tailor_resume_tool_func(
    candidate_resume: str,
    vacancy_payload: Dict[str, Any],
    declared_skills: Optional[List[str]] = None,  # ← ИСПРАВЛЕНО: добавлен параметр
) -> Dict[str, Any]:
    """Синхронная обёртка для tailor_resume."""
    logger.info(f"🔄 [FUNC] tailor_resume_tool_func: skills={declared_skills}")
    try:
        return _run_async_safe(
            vacancy_search_tool.tailor_resume(
                candidate_resume=candidate_resume,
                vacancy_payload=vacancy_payload,
                declared_skills=declared_skills,
            )
        )
    except Exception as e:
        logger.exception(f"❌ [FUNC] Ошибка: {e}")
        return {"error": str(e), "recommendations": []}


# ═══════════════════════════════════════════════════════════════════════
# ЭКСПОРТЫ
# ═══════════════════════════════════════════════════════════════════════

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
    "generate_roadmap_func",
    "tailor_resume_tool_func",
    "get_llm_client",
    "GROQ_MODEL_FAST",
    "GROQ_MODEL_SMART",
]
