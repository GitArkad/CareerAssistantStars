# summary_node.py
import os
from dotenv import load_dotenv

from langchain_groq import ChatGroq
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from app.agents.state import AgentState
from app.agents.career_tools import career_tools_list 
from app.agents.services.taxonomy import ROADMAP_TRIGGERS, RESUME_TRIGGERS

load_dotenv()


GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# Инициализация
llm = ChatGroq(model=GROQ_MODEL, temperature=0.2)
llm_with_tools = llm.bind_tools(career_tools_list)


def summary_node(state: AgentState):
    print("\n--- [START] SUMMARY: Processing Messages ---")
    
    messages = state.get("messages", [])
    if "interview" not in state:
        state["interview"] = None

    print("\n=== DEBUG ===")
    for m in messages[-5:]:
        print(type(m).__name__)


    last_user_message = (
        messages[-1].content.lower().strip()
        if messages and isinstance(messages[-1], HumanMessage)
        else ""
    )
    
    # =========================================================
    # 🟡 SHOW SAVED DATA (СНАЧАЛА!)
    # =========================================================

    # roadmap
    if (
        state.get("roadmap")
        and any(trigger in last_user_message for trigger in ROADMAP_TRIGGERS)
    ):
        print("📦 cached roadmap")
        return {
            "messages": [AIMessage(content=state["roadmap"])],
            "interview": state.get("interview")
        }

    # resume
    if (
        state.get("tailored_resume")
        and any(trigger in last_user_message for trigger in RESUME_TRIGGERS)
    ):
        print("📦 cached resume")
        return {
            "messages": [AIMessage(content=state["tailored_resume"])],
            "interview": state.get("interview")
        }

    # =========================================================
    # 🚀 FAST ROUTER (БЕЗ LLM)
    # =========================================================

    if any(trigger in last_user_message for trigger in ROADMAP_TRIGGERS):
        print("⚡ FAST ROUTER: roadmap → tool")
        return {
            "messages": [],
            "tool": "generate_roadmap"
        }
    
    if any(trigger in last_user_message for trigger in RESUME_TRIGGERS):
        print("⚡ FAST ROUTER: resume → tool")
        return {
            "messages": [],
            "tool": "improve_resume"
        }

    # =========================================================
    # 🔥 1. ОБРАБОТКА TOOL RESULT (ГЛАВНОЕ ИЗМЕНЕНИЕ)
    # =========================================================
    if messages and isinstance(messages[-1], ToolMessage):
        print("Tool result detected → финальный ответ")

        tool_name = messages[-1].name

        # =========================
        # 🟢 ROADMAP
        # =========================
        if tool_name == "generate_roadmap":

            candidate = state.get("candidate", {})
            market = state.get("market", {})

            gaps = market.get("skill_gaps", [])
            specialization = candidate.get("specialization", "").lower()
            salary_median = market.get("salary_median", 0)            # P50 (Медиана)
            salary_top_10 = market.get("salary_top_10", 0)             # P90 (Топ рынка)
            market_range = market.get("market_range", [0, 0])
            if market_range == [0, 0]:
                market_range_str = "нет данных"
            else:
                market_range_str = f"{market_range[0]} - {market_range[1]}"

            if not gaps:
                if "ml" in specialization or "data" in specialization:
                    gaps = ["ml fundamentals", "model evaluation", "feature engineering"]
                elif "backend" in specialization:
                    gaps = ["system design", "databases", "api design"]
                elif "frontend" in specialization:
                    gaps = ["javascript", "react", "css architecture"]
                else:
                    gaps = ["computer science fundamentals", "algorithms", "system design"]
            experience = candidate.get("experience_years")
            top_vacancies = market.get("top_vacancies", [])[:5]
            vacancies_text = "\n".join([
                f"- {v['title']} ({v['company']}): {', '.join(v.get('skills', []))}"
                for v in top_vacancies
            ])

            prompt = f"""
            Ты пишешь только на русском.

            Дай roadmap (максимум 3 шага).

            Кандидат: {specialization}, {experience} лет  
            Skills: {", ".join(candidate.get('skills', [])[:5])}  
            Gaps: {", ".join(gaps)}

            РЫНОК:
            P50: {salary_median}
            P90: {salary_top_10}
            Range: {market_range_str}

            Формат:
            1. ...
            2. ...
            3. ...

            Для каждого шага:
            - что изучить
            - как применить

            В конце всего:
            ЗП сейчас → ЗП после (используй только P50 и P90)

            Кратко, без лишнего.
            """

            response = llm.invoke([SystemMessage(content=prompt)])

            # return {"messages": [response]}
            return {
                "messages": [AIMessage(content=response.content)],
                "roadmap": response.content
            }

        # =========================
        # 🔵 IMPROVE RESUME (TOP-1)
        # =========================
        if tool_name == "improve_resume":

            candidate = state.get("candidate", {})
            market = state.get("market", {})

            vacancies = market.get("top_vacancies", [])

            if not vacancies:
                return {
                    "messages": [AIMessage(content="Не нашёл вакансий для адаптации резюме.")]
                }

            best_vacancy = vacancies[0]
            skills = best_vacancy.get('skills') or []

            prompt = f"""
            Перепиши резюме под вакансию.

            Кандидат:
            {candidate.get('experience_years')} лет
            Skills: {", ".join(candidate.get('skills', [])[:5])}

            Вакансия:
            {best_vacancy.get('title')}
            Skills: {", ".join(skills[:5])}

            Сделай:
            - краткое summary
            - релевантный стек
            - достижения (bullet points)
            """

            response = llm.invoke([SystemMessage(content=prompt)])

            # return {"messages": [response]}
            # last_tool_message = state["messages"][-1]

            return {
                "messages": [AIMessage(content=response.content)],
                "tailored_resume": response.content
            }

        # =========================
        # 🟣 INTERVIEW
        # =========================
        if tool_name == "start_mock_interview":

            candidate = state.get("candidate", {})
            market = state.get("market", {})

            gaps = market.get("skill_gaps", [])
            specialization = candidate.get("specialization", "")

            if not gaps:
                vacancies = market.get("top_vacancies", [])

                if vacancies:
                    # берём skills из вакансий
                    all_skills = []
                    for v in vacancies:
                        all_skills.extend(v.get("skills", []))

                    # уникальные
                    gaps = list(set(all_skills))[:5]

                else:
                    spec = (candidate.get("specialization") or "").lower()

                    if "ml" in spec or "data" in spec:
                        gaps = ["ml system design", "feature engineering", "model evaluation"]

                    elif "backend" in spec:
                        gaps = ["system design", "databases", "api design"]

                    else:
                        gaps = ["algorithms", "system design", "problem solving"]

            # ✅ создаём состояние интервью
            interview = {
                "active": True,
                "step": 1,
                "gaps": gaps
            }

            prompt = f"""
            Ты интервьюер.

            Задай 1 вопрос по теме:
            {", ".join(gaps[:3])}

            Без объяснений.
            """

            response = llm.invoke([SystemMessage(content=prompt)])

            return {
                "messages": [response],
                "interview": interview
            }

    # =========================================================
    # 🟢 2. ПЕРВЫЙ ЗАПУСК (НЕ ТРОГАЕМ)
    # =========================================================
    has_ai_reply = any(isinstance(m, AIMessage) for m in messages)

    if not has_ai_reply:
        candidate = state.get("candidate", {})
        market = state.get("market", {})

        report = f"""
            ✅ **Резюме проанализировано!**
            Кандидат: {candidate.get('name', 'Кандидат')} | Опыт: {candidate.get('experience_years', 0)} лет.
            Match Score: {market.get('match_score', 0)}%

            🔍 **Топ вакансии:**
        """

        for v in market.get('top_vacancies', [])[:3]:
            report += f"- {v['title']} в {v['company']} ({v['match_score']}%)\n"

        return {
            "summary": report,
            "messages": [AIMessage(content=report)]
        }

    # =========================================================
    # 🟣 INTERVIEW FLOW
    # =========================================================
    interview = state.get("interview")

    if interview and interview.get("active"):

        candidate = state.get("candidate", {})
        market = state.get("market", {})
        gaps = interview.get("gaps", [])

        last_user_message = ""

        for m in reversed(messages):
            if isinstance(m, HumanMessage):
                last_user_message = m.content
                break

        # 🛑 остановка интервью
        if "стоп" in last_user_message.lower() or "хватит" in last_user_message.lower():
            interview["active"] = False
            return {
                "messages": [AIMessage(content="Окей, остановили интервью 👍")],
                "interview": interview   # 🔥 не забудь вернуть
            }

        prompt = f"""
        Ты интервьюер.

        Ответ кандидата:
        {last_user_message}
        Если предыдущий ответ кандидата слабый, то задай уточняющий вопрос, если же нормальный, то задай следующий вопрос по теме:
        {", ".join(gaps[:3])}

        1 вопрос. Без объяснений.
        """

        response = llm.invoke([SystemMessage(content=prompt)])

        # увеличиваем шаг
        interview["step"] += 1

        # 🛑 ограничение (например 5 вопросов)
        if interview["step"] >= 5:
            interview["active"] = False
            return {
                "messages": [AIMessage(content="Интервью завершено. Хочешь разбор?")],
                "interview": interview
            }

        return {
            "messages": [response],
            "interview": interview
        }


    # =========================================================
    # 🔵 3. ОБЫЧНЫЙ ДИАЛОГ
    # =========================================================
    candidate = state.get("candidate", {})
    market = state.get("market", {})

    short_context = f"""
    {candidate.get('specialization')} ({candidate.get('experience_years')}y)
    Skills: {", ".join(candidate.get('skills', [])[:5])}
    Match: {market.get('match_score')}%
    Gaps: {", ".join(market.get('skill_gaps', [])[:5])}
    """
    
    system_prompt = SystemMessage(content=f"""
    Ты карьерный ассистент.

    Контекст:
    {short_context}

    Правила:
    - roadmap → generate_roadmap
    - резюме → improve_resume
    - интервью → start_mock_interview
    - если интервью активно → продолжай

    Не пиши лишнего.
    """)

    response = llm_with_tools.invoke([system_prompt] + messages[-5:])

    return {
        "messages": [response],
        "interview": state.get("interview")
    }
