def assistant_node(state: AgentState) -> Dict[str, Any]:
    """
    Основной узел — вызывает LLM с инструментами.
    Возвращает dict с обновлениями (LangGraph merge).
    """

    query = getattr(state, "query", None)
    messages = getattr(state, "messages", [])
    current_iter = getattr(state, "iteration_count", 0)
    candidate = getattr(state, "candidate", None)

    # ── Защита от зацикливания ──
    if current_iter >= getattr(state, "max_iterations", 5):
        return {
            "messages": [
                AIMessage(content="Я проверил данные. Вот лучшие результаты, которые удалось найти.")
            ],
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

    # ── Контекст ──
    context_parts = []
    mc = getattr(state, "market_context", None)
    if mc and isinstance(mc, dict) and mc.get("top_vacancies"):
        context_parts.append(
            f"Результаты поиска вакансий уже есть (найдено {len(mc['top_vacancies'])}). Используй их."
        )

    sg = getattr(state, "skills_gap", None)
    if sg:
        context_parts.append(f"Skills gap: {', '.join(sg[:5])}")

    candidate_skills_str = ""
    if candidate_dict and candidate_dict.get("skills"):
        candidate_skills_str = f"Навыки кандидата: {', '.join(candidate_dict['skills'])}"

    extra_context = "\n".join(context_parts) if context_parts else ""

    # ── SYSTEM PROMPT ──
    system_prompt = f"""
Ты — карьерный ассистент, помогающий специалистам анализировать рынок труда.

Твои возможности:
1. Анализировать вакансии по запросу и локации (analyze_market_context)
2. Рассчитывать разрыв навыков (calculate_skills_gap)
3. Информация о зарплатах (get_salary_insights)
4. Генерировать план развития (generate_roadmap_tool)
5. Адаптировать резюме под вакансию (tailor_resume_tool)

ПРАВИЛА ИЗВЛЕЧЕНИЯ ЛОКАЦИИ:
- Никогда не додумывай город, если он не упомянут
- Если город не указан — не добавляй поле city

{candidate_skills_str}
{extra_context}

ПРАВИЛА:
- Если это приветствие → ответь приветствием
- Если недостаточно данных → попроси уточнение
- Не рекомендуй уже существующие навыки
- Используй ТОЛЬКО данные инструментов
- НЕ ВЫДУМЫВАЙ данные

ДОСТУП К ДАННЫМ:
market_context.top_vacancies[].payload:
- title, company, city, country
- salary_from, salary_to
- skills / requirements
- url

ФОРМАТ ВАКАНСИЙ (ТОП-5):
1. **[title]**
Компания: [company]
Локация: [city], [country]
Зарплата: [salary_from] — [salary_to] ₽
Стек: [skills]
Ссылка: [url]

Отвечай кратко и по делу.
"""

# ПРАВИЛА ЛОКАЦИИ:
#     Если город не упомянут — НЕ добавляй ключ "city" в словарь.
#     Если страна не упомянута — НЕ добавляй ключ "country".
#     Если локации нет совсем — передавай location=None.

#     system_prompt = f"""
# РОЛЬ: Ты — Карьерный AI-Ассистент. Твоя работа: классифицировать запрос, вызвать инструмент и выдать ответ СТРОГО на основе полученных данных.

# КОНТЕКСТ ПОЛЬЗОВАТЕЛЯ:
#     Текущие навыки: {candidate_skills_str}
#     Доп. контекст: {extra_context}

# ЖЕСТКИЕ ОГРАНИЧЕНИЯ (ПРОТОКОЛ "NO-HALLUCINATION"):
#     ЗАПРЕТ НА ВЫМЫСЕЛ: Запрещено придумывать вакансии, зарплаты, компании, навыки, города. Если инструмент вернул пустой результат — прямо ответь, что данных нет.
#     ПРИОРИТЕТ ДАННЫХ: Используй только ToolMessage. Игнорируй свои внутренние знания о рынке, если они противоречат выдаче инструментов.
#     ФИЛЬТР НАВЫКОВ: Если пользователь уже владеет навыком (см. Текущие навыки) — никогда не включай его в рекомендации по обучению.

# АЛГОРИТМ ВЫБОРА ИНСТРУМЕНТОВ:
#     Поиск/анализ работы/ищу работу или вакансию → analyze_market_context
#     "Что учить", "roadmap", "план развития", "план обучения" → generate_roadmap_tool
#     "Адаптируй/улучши резюме" → tailor_resume_tool
#     Анализ дефицита навыков → calculate_skills_gap
#     Зарплатные ожидания → get_salary_insights

# ПРАВИЛА ВЫЗОВА ИНСТРУМЕНТОВ:
# - calculate_skills_gap: передавай ТОЛЬКО список названий навыков (из поля skill_gaps или извлеченных из вакансий). 

# РЕГЛАМЕНТ ОТВЕТОВ:
#     При выводе вакансий (Макс. 5):
#         [Название должности]
#         Компания: [company] | Локация: [city], [country]
#         Зарплата: [salary_from] — [salary_to] ₽
#         Стек: [skills]
#         Ссылка: [url]

# ОБРАБОТКА РЕЗУЛЬТАТОВ:
#     - Если инструмент поиска (analyze_market_context) вернул 0 вакансий: ответь "К сожалению, по вашему запросу в данной локации вакансий не найдено. Попробуйте расширить поиск".
#     - Если вакансии НАЙДЕНЫ, но список недостающих навыков (skill_gap) пуст, выведи список вакансий.
#     - НИКОГДА не говори, что стек соответствует, если вакансий найдено 0.

#     При адаптации резюме (tailor_resume_tool):
#         Выдели ТОП-5 навыков из вакансии.
#         Проведи аудит (что есть / что слабо).
#         Перепиши достижения по формуле Google: "Сделал [X], что измеряется [Y], внедрением [Z]".
#         Список недостающих ключевых слов.

#     При отсутствии разрыва навыков:
#         Если skill_gap пуст, ответь: "Ваш текущий стек полностью соответствует требованиям рынка для найденных вакансий ТОП-5".

# Язык: Русский. Стиль: Лаконичный, профессиональный, структурированный.
#     """

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

    # ─────────────────────────────────────
    # 🧠 РЕЖИМЫ llm
    # ─────────────────────────────────────
    has_data = bool(mc and isinstance(mc, dict) and mc.get("top_vacancies"))
    mode = "ANSWER" if has_data else "DATA"

    logger.info(f"🧠 Режим работы: {mode}")

    # ─────────────────────────────────────
    # 🔧 Управление tools
    # ─────────────────────────────────────
    if mode == "DATA":
        llm_with_tools = llm.bind_tools(ALL_TOOLS)
    else:
        llm_with_tools = llm

    try:
        response = llm_with_tools.invoke(all_messages, config=config)

        content_preview = response.content[:200] if response.content else "EMPTY"
        logger.info(f"📝 [LLM] Content: '{content_preview}...'")
        logger.info(
            f"🔧 [LLM] Tool calls: {response.tool_calls if hasattr(response, 'tool_calls') else 'N/A'}"
        )

        has_tool_calls = hasattr(response, "tool_calls") and response.tool_calls
        prev_consecutive = getattr(state, "consecutive_tool_calls", 0)

        # ─────────────────────────────────────
        # 🛑 GUARD 1: форсим tool если DATA
        # ─────────────────────────────────────
        if mode == "DATA" and not has_tool_calls:
            logger.warning("⚠️ Форсим вызов analyze_market_context")

            forced = AIMessage(
                content="Выполняю поиск вакансий...",
                tool_calls=[
                    {
                        "name": "analyze_market_context",
                        "args": {}
                    }
                ]
            )

            return {
                "messages": [forced],
                "iteration_count": current_iter + 1,
                "consecutive_tool_calls": prev_consecutive + 1,
            }

        # ─────────────────────────────────────
        # 🛑 GUARD 2: блок повторных tool
        # ─────────────────────────────────────
        if mode == "ANSWER" and has_tool_calls:
            logger.warning("⚠️ Блокируем повторный tool")

            safe_response = AIMessage(
                content="Формирую ответ на основе найденных данных..."
            )

            return {
                "messages": [safe_response],
                "iteration_count": current_iter + 1,
                "consecutive_tool_calls": 0,
            }

        # ─────────────────────────────────────
        # 🧠 Fallback
        # ─────────────────────────────────────
        if not response.content and not has_tool_calls:
            if mc:
                vacancies = len(mc.get("top_vacancies", []))
                median = mc.get("salary_median", 0)
                fallback = f"🔍 Найдено {vacancies} вакансий. Медианная зарплата: {median:,} ₽."
                if sg:
                    fallback += f"\n📚 Рекомендуемые навыки: {', '.join(sg[:3])}"
                response = AIMessage(content=fallback)

        # ─────────────────────────────────────
        # 📦 Финальный return
        # ─────────────────────────────────────
        updates = {
            "messages": [response],
            "iteration_count": current_iter + 1,
            "consecutive_tool_calls": (prev_consecutive + 1) if has_tool_calls else 0,
        }

        if candidate_dict:
            updates["candidate"] = CandidateProfile(**candidate_dict)

        return updates

    except Exception as e:
        logger.exception(f"❌ [assistant_node] Ошибка: {e}")

        return {
            "messages": [
                AIMessage(content=f"Произошла ошибка: {str(e)}. Попробуйте ещё раз.")
            ],
            "iteration_count": current_iter + 1,
        }