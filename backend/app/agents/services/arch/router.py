# router.py
def router_node(state: AgentState):
    """
    Анализирует последнее сообщение пользователя и решает: 
    вызвать инструмент или просто ответить.
    """
    last_msg = state["messages"][-1].content.lower()
    
    # Передаем управление LLM, чтобы она сама выбрала инструмент
    # (Это и есть 'Agentic' часть)
    response = llm_with_tools.invoke(state["messages"])
    
    if response.tool_calls:
        # Если LLM решила вызвать инструмент
        tool_name = response.tool_calls[0]['name']
        if tool_name == "generate_roadmap": return "execute_roadmap"
        if tool_name == "improve_resume": return "execute_resume"
        if tool_name == "start_mock_interview": return "execute_interview"
    
    return "end"