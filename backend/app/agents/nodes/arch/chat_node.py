# chat_node.py
import os
from dotenv import load_dotenv

from langchain_groq import ChatGroq
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from app.agents.state import AgentState
from app.agents.career_tools import career_tools_list
from app.agents.services.taxonomy import ROADMAP_TRIGGERS, RESUME_TRIGGERS

load_dotenv()

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

llm = ChatGroq(model=GROQ_MODEL, temperature=0.2)
llm_with_tools = llm.bind_tools(career_tools_list)


def chat_node(state: AgentState):
    print("\n--- [CHAT NODE] ---")

    messages = state.get("messages", [])
    candidate = state.get("candidate", {})
    market = state.get("market", {})
    interview = state.get("interview")

    # 🔥 НЕ ДАЕМ CHAT ПЕРЕБИТЬ SUMMARY
    if state.get("just_summarized"):
        return {
            "messages": [],
            "interview": interview,
            "just_summarized": False,
            "iterations": state.get("iterations", 0) + 1
        }

    iterations = state.get("iterations", 0)
    tool_calls = state.get("tool_calls", 0)
    last_tool = state.get("last_tool")

    def step(update: dict):
        return {**update, "iterations": iterations + 1}

    def ai(text):
        return AIMessage(content=text)

    last_user = (
        messages[-1].content.lower().strip()
        if messages and isinstance(messages[-1], HumanMessage)
        else ""
    )

    def match_trigger(text, triggers):
        return any(t in text for t in triggers)

    # LIMITS
    if iterations > 10:
        return step({"messages": [ai("Достигнут лимит шагов, остановка")], "interview": None})
    if tool_calls > 3:
        return step({"messages": [ai("Слишком много вызовов инструментов")]})

    # GREETING / OFFTOPIC
    if last_user:
        if last_user in {"привет", "hello", "hi"}:
            return step({"messages": [ai("Привет. Я карьерный ассистент.")]})
        if any(t in last_user for t in ["погода", "новости", "курс", "доллар", "евро", "как дела", "что делаешь"]):
            return step({"messages": [ai("Я помогаю с карьерой и резюме.")]})

    # ROUTER: сначала триггеры
    if last_user:
        if match_trigger(last_user, RESUME_TRIGGERS):
            return step({"messages": [], "tool": "improve_resume"})
        if match_trigger(last_user, ROADMAP_TRIGGERS):
            return step({"messages": [], "tool": "generate_roadmap"})

    # CACHE
    if state.get("tailored_resume") and match_trigger(last_user, RESUME_TRIGGERS):
        return step({"messages": [ai(state["tailored_resume"])]})
    if state.get("roadmap") and match_trigger(last_user, ROADMAP_TRIGGERS):
        return step({"messages": [ai(state["roadmap"])]})

    # TOOL RESULT
    if messages and isinstance(messages[-1], ToolMessage):
        tool = messages[-1].name
        if tool == last_tool:
            return step({"messages": [ai("Инструмент уже был вызван")]})

        # ===== ROADMAP =====
        if tool == "generate_roadmap":
            top3 = market.get("top_vacancies", [])[:3]
            gaps_set = set()
            for v in top3:
                gaps_set.update(v.get("skills", []))
            candidate_skills = set(candidate.get("skills", []))
            gaps_to_learn = list(gaps_set - candidate_skills)[:3]

            current_match = market.get("match_score", 0)
            p50 = market.get("salary_median", 0)
            p90 = market.get("salary_top_10", 0)

            prompt = f"""
Ты карьерный аналитик.

Кандидат:
Skills: {', '.join(candidate.get('skills', [])[:10])}
Match: {current_match}%

Топ 3 вакансии:
{', '.join([v['title'] for v in top3])}

Недостающие навыки для повышения Match и зарплаты:
{', '.join(gaps_to_learn)}

Формат:
1. навык
2. навык
3. навык

ИТОГ:
Match: {current_match}% → X%
Зарплата: {p50} → Y
"""

            res = llm.invoke([SystemMessage(content=prompt)])
            return step({
                "messages": [ai(res.content)],
                "roadmap": res.content,
                "tool_calls": tool_calls + 1,
                "last_tool": tool
            })

        # ===== IMPROVE RESUME (TOP-1) =====
        if tool == "improve_resume":
            top1 = (market.get("top_vacancies") or [None])[0]
            if not top1:
                return step({"messages": [ai("Нет вакансий для адаптации резюме")]})

            prompt = f"""
Ты HR.

Адаптируй резюме под вакансию.

Кандидат:
Skills: {', '.join(candidate.get('skills', [])[:10])}

Вакансия:
{top1['title']} в {top1['company']}
Требуемые навыки: {', '.join(top1.get('skills', [])[:5])}

Правила:
- кратко (4-6 строк)
- усили релевантный опыт
- добавь ключевые слова вакансии
- без воды
"""

            res = llm.invoke([SystemMessage(content=prompt)])
            return step({
                "messages": [ai(res.content)],
                "tailored_resume": res.content,
                "tool_calls": tool_calls + 1,
                "last_tool": tool
            })

        # ===== INTERVIEW =====
        if tool == "start_mock_interview":
            top1 = (market.get("top_vacancies") or [None])[0]
            gaps = top1.get("skills", [])[:5] if top1 else market.get("skill_gaps", [])[:5]
            interview = {"active": True, "step": 1, "gaps": gaps}

            prompt = f"""
Ты интервьюер.

Задай 1 вопрос по теме:
{', '.join(gaps[:3])}

Только вопрос.
"""
            res = llm.invoke([SystemMessage(content=prompt)])
            return step({
                "messages": [ai(res.content)],
                "interview": interview,
                "tool_calls": tool_calls + 1,
                "last_tool": tool
            })

    # ===== INTERVIEW FLOW =====
    if interview and interview.get("active"):
        last_answer = next((m.content for m in reversed(messages) if isinstance(m, HumanMessage)), "")
        if not last_answer.strip() or any(x in last_answer.lower() for x in ["стоп", "закончено"]):
            interview["active"] = False
            return step({"messages": [ai("Интервью завершено")], "interview": interview})

        # следующий вопрос
        top1 = (market.get("top_vacancies") or [None])[0]
        gaps = top1.get("skills", [])[:5] if top1 else market.get("skill_gaps", [])[:5]

        prompt = f"""
Кандидат ответил:
{last_answer}

Если ответ слабый, задай уточняющий вопрос. Если нормальный, переходи к следующему вопрос.
Следующий вопрос (только вопрос, без объяснений) по теме:
{', '.join(gaps[:3])}
"""
        res = llm.invoke([SystemMessage(content=prompt)])
        interview["step"] += 1
        if interview["step"] > 5:
            interview["active"] = False
            return step({"messages": [ai("Интервью завершено")], "interview": interview})

        return step({"messages": [res], "interview": interview})

    # ===== CHAT / SUMMARY =====
    summary_text = f"✅ Резюме проанализировано!\nКандидат: {candidate.get('name')}\nСпециализация: {candidate.get('specialization')}\nОпыт: {candidate.get('experience_years')} лет\nMatch Score: {market.get('match_score')}%\n\n🧠 Навыки:\n{', '.join(candidate.get('skills', [])[:10])}\n\n⚠️ Пробелы:\n{', '.join(market.get('skill_gaps', [])[:5]) or 'не выявлены'}\n\n🔍 Топ вакансии:\n"

    for v in market.get("top_vacancies", [])[:3]:
        salary = f"{v.get('salary_from', '?')}-{v.get('salary_to', '?')}" if v.get('salary_from') else "не указана"
        link = v.get("url", "")
        summary_text += f"- {v['title']} в {v['company']} ({v.get('match_score', 0)}%) | ЗП: {salary} {f'| {link}' if link else ''}\n"

    return step({"messages": [AIMessage(content=summary_text)], "interview": interview})