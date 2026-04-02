def route_by_mode(state: AgentState) -> AgentState:
    """
    Обновляет state на основе user_input для выбора режима и фильтров
    """
    user_input = state.get("user_input", "").lower() if state.get("user_input") else ""

    # простая логика выбора режима
    if "резюме" in user_input:
        state["stage"] = "resume"
    elif "обуч" in user_input:
        state["stage"] = "roadmap"
    elif "интерв" in user_input:
        state["stage"] = "interview"
    else:
        state["stage"] = "search"

    # фильтры
    if "berlin" in user_input:
        if state["candidate"] is None:
            state["candidate"] = {}
        state["candidate"]["city"] = "Berlin"
        state["candidate"]["relocation"] = False

    # добавление навыков
    for skill in ["python", "docker", "sql"]:
        if skill in user_input:
            if state["candidate"] is None:
                state["candidate"] = {}
            state["candidate"].setdefault("skills", []).append(skill)

    return state