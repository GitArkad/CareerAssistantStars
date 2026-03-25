import os
from langchain_groq import ChatGroq
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from app.agents.state import AgentState
from app.agents.career_tools import career_tools_list 

# Инициализация
# llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0.2)
llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0.2)
llm_with_tools = llm.bind_tools(career_tools_list)

def summary_node(state: AgentState):
    print("\n--- [START] SUMMARY: Processing Messages ---")
    
    messages = state.get("messages", [])
    
    # ПРОВЕРКА НА ЦИКЛ: Если последнее сообщение уже от AI (и без вызова тулз), выходим.
    if messages and isinstance(messages[-1], AIMessage) and not messages[-1].tool_calls:
        return {}

    # Проверяем, был ли уже ответ от ассистента в истории
    has_ai_reply = any(isinstance(m, AIMessage) for m in messages)
    
    # 1. ПЕРВЫЙ ЗАПУСК (Формируем отчет)
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

        # Возвращаем AIMessage в список сообщений
        return {
            "summary": report,
            "messages": [AIMessage(content=report)]
        }

    # 2. ДИАЛОГ В ЧАТЕ
    system_prompt = SystemMessage(content=f"""
    Ты экспертный карьерный ассистент. 
    Данные кандидата: {state.get('candidate')}
    Данные рынка: {state.get('market')}
    
    Если пользователь просит 'роадмап' — ОБЯЗАТЕЛЬНО вызывай инструмент generate_roadmap.
    """)
    
    # Вызываем модель
    response = llm_with_tools.invoke([system_prompt] + messages)

    return {
        "messages": [response]
    }