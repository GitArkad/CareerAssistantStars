import os, json
from copy import deepcopy
from langchain_groq import ChatGroq
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from app.agents.state import AgentState
from app.agents.tools import career_tools_list
from app.agents.services.qdrant_service import search_vacancies
from dotenv import load_dotenv

load_dotenv()

llm = ChatGroq(model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"), temperature=0.2)

def agent_node(state: AgentState):
    print("\n--- [AGENT] ---")
    
    if state.get("just_processed_tool"):
        return {"messages": [], "just_processed_tool": False}
    
    messages = state.get("messages", [])
    candidate = state.get("candidate", {})
    market = state.get("market", {})
    interview = deepcopy(state.get("interview")) if state.get("interview") else {}
    last_user_msg = next((m for m in reversed(messages) if isinstance(m, HumanMessage)), None)
    last_user_text = last_user_msg.content.lower().strip() if last_user_msg else ""
    total_iterations = state.get("total_iterations", 0)
    
    if total_iterations >= 20:
        return {"messages": [AIMessage(content="⚠️ Лимит итераций (20)")], "total_iterations": total_iterations}
    
    if interview.get("active"):
        return _handle_interview(messages, interview, last_user_text)
    
    if messages and isinstance(messages[-1], ToolMessage):
        result = _handle_tool_result(state, messages[-1].name, messages[-1].content)
        result["just_processed_tool"] = True
        result["interview"] = interview
        result["total_iterations"] = total_iterations
        return result
    
    if state.get("roadmap") and any(t in last_user_text for t in ["роадмап", "план", "что учить"]):
        return {"messages": [AIMessage(content=state["roadmap"])], "total_iterations": total_iterations}
    if state.get("tailored_resume") and any(t in last_user_text for t in ["резюме", "улучши"]):
        return {"messages": [AIMessage(content=state["tailored_resume"])], "total_iterations": total_iterations}
    
    llm_with_tools = llm.bind_tools(career_tools_list)
    system_prompt = SystemMessage(content=f"""
    Ты карьерный ассистент. Отвечай КРАТКО (3-5 предложений).

    Контекст:
    - Кандидат: {candidate.get('specialization', 'N/A')}, {candidate.get('experience_years', 0)} лет
    - Навыки: {", ".join(candidate.get('skills', [])[:5])}
    - Пробелы: {", ".join(market.get('skill_gaps', [])[:3]) or 'нет'}
    - ЗП: {market.get('salary_median', 0)} ₽ (медиана)

    ПРАВИЛА:
    1. НЕ генерируй списки "топ инструменты", "топ библиотеки" и т.д.
    2. НЕ раскрывай таксономию навыков
    3. Если резюме уже проанализировано — НЕ генерируй отчёт заново
    4. Для roadmap/resume/interview вызывай инструменты
    ИНСТРУМЕНТЫ: generate_roadmap, improve_resume, start_interview, search_vacancies, update_candidate
    Формат: "✅ [краткий ответ]. 
    Подсказка: роадмап | резюме | интервью"
    Отвечай кратко на русском.
    """)

    
    try:
        response = llm_with_tools.invoke([system_prompt] + messages[-5:])
        return {"messages": [response], "interview": interview, "just_processed_tool": False, "total_iterations": total_iterations}
    except Exception as e:
        return {"messages": [AIMessage(content=f"Ошибка: {e}")], "interview": interview, "total_iterations": total_iterations}

def _handle_tool_result(state, tool_name, tool_content):
    candidate = state.get("candidate", {})
    market = state.get("market", {})
    llm = ChatGroq(model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"))
    
    if tool_name == "search_vacancies":
        params = json.loads(tool_content.split(":", 1)[1].replace("'", '"'))
        new_market = search_vacancies(specialization=candidate.get("specialization"), skills=candidate.get("skills", []),
                                       city=params.get("city") or candidate.get("city"), salary_min=params.get("salary_min"), limit=5)
        report = f"🔍 Найдено вакансий: {len(new_market['top_vacancies'])}\n"
        for v in new_market['top_vacancies'][:5]:
            report += f"- {v['title']} в {v['company']} | ЗП: {v.get('salary_from', '?')}-{v.get('salary_to', '?')}₽\n"
        return {"messages": [AIMessage(content=report)], "market": new_market, "just_processed_tool": True}
    
    elif tool_name == "update_candidate":
        updates = json.loads(tool_content.split(":", 1)[1].replace("'", '"'))
        new_candidate = deepcopy(candidate)
        if updates.get("skills_to_add"):
            new_candidate["skills"] = list(set(new_candidate.get("skills", [])) | set(s.lower() for s in updates["skills_to_add"]))
        report = f"✅ Профиль обновлен! Навыков: {len(new_candidate['skills'])}\n💡 Скажите 'обновить вакансии'"
        return {"messages": [AIMessage(content=report)], "candidate": new_candidate, "market": None, "just_processed_tool": True}
    
    elif tool_name == "generate_roadmap":
        gaps = market.get("skill_gaps", [])[:3] or ["системный дизайн"]
        prompt = f"Roadmap для {candidate.get('specialization')}. Пробелы: {', '.join(gaps)}. ЗП: {market.get('salary_median')} → {market.get('salary_top_10')} ₽. Формат: 1-3 шаги + ИТОГ."
        response = llm.invoke([SystemMessage(content=prompt)])
        return {"messages": [AIMessage(content=response.content)], "roadmap": response.content}
    
    elif tool_name == "improve_resume":
        top1 = (market.get("top_vacancies") or [None])[0]
        if not top1: return {"messages": [AIMessage(content="Нет вакансий")]}
        prompt = f"Адаптируй резюме под {top1['title']} в {top1['company']}. Навыки: {', '.join(candidate.get('skills', [])[:5])}. Summary 4-5 строк."
        response = llm.invoke([SystemMessage(content=prompt)])
        return {"messages": [AIMessage(content=response.content)], "tailored_resume": response.content}
    
    elif tool_name == "start_interview":
        gaps = market.get("skill_gaps", [])[:5] or ["системный дизайн"]
        prompt = f"Задай первый вопрос интервью по: {', '.join(gaps[:3])}. Только вопрос."
        response = llm.invoke([SystemMessage(content=prompt)])
        return {"messages": [AIMessage(content=response.content)], "interview": {"active": True, "step": 1, "gaps": gaps}}
    
    return {"messages": [AIMessage(content="Запрос обработан")]}

def _handle_interview(messages, interview, last_answer):
    if any(x in last_answer.lower() for x in ["стоп", "хватит", "закончено"]):
        interview["active"] = False
        return {"messages": [AIMessage(content="Интервью завершено. Удачи! 🎯")], "interview": interview}
    step = interview.get("step", 1)
    if step >= 5:
        interview["active"] = False
        return {"messages": [AIMessage(content="Интервью завершено (5 вопросов).")], "interview": interview}
    gaps = interview.get("gaps", [])
    llm = ChatGroq(model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"))
    prompt = f"Ответ: '{last_answer}'. Задай вопрос #{step+1} по: {', '.join(gaps[:3])}. Только вопрос."
    response = llm.invoke([SystemMessage(content=prompt)])
    return {"messages": [AIMessage(content=response.content)], "interview": {**interview, "step": step + 1}}