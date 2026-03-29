from app.agents2.tools.career_agent import CareerAgent
from app.agents2.services.resume_parser import parse_pdf


def router_node(state):
    """
    Router node:
    - парсит PDF (если есть)
    - определяет action
    - передаёт ВСЁ в agent.route()
    """

    agent = CareerAgent()

    message = (state.get("message") or "").lower()

    # -----------------------------
    # 1. FILE → parse resume
    # -----------------------------
    if state.get("raw_file_content") and not state.get("candidate"):
        parsed = parse_pdf(state["raw_file_content"], True)

        if isinstance(parsed, str):
            state["candidate"] = {
                "skills": parsed.lower().split()[:10],
                "city": None,
                "relocation": True
            }
        else:
            state["candidate"] = parsed

        state["last_action"] = "PDF распарсен"

    # -----------------------------
    # 2. DIGIT INPUT (НЕ ТРОГАЕМ ACTION)
    # -----------------------------
    if message.isdigit():
        print("🔢 DIGIT INPUT → skip action override")
        return agent.route(state)

    # -----------------------------
    # 3. COMMANDS
    # -----------------------------
    if "search" in message:
        state["action"] = "search"

    elif "roadmap" in message:
        state["action"] = "roadmap"

    elif "resume" in message:
        state["action"] = "resume"

    elif "interview" in message:
        state["action"] = "interview"

    # -----------------------------
    # 4. AUTO SEARCH
    # -----------------------------
    elif state.get("candidate") and not state.get("top_vacancies"):
        print(f"ROUT_NODE 4.1  state['action'] - {state["action"]}")
        state["action"] = "search"
        print(f"ROUT_NODE 4.2  state['action'] - {state["action"]}")

    # -----------------------------
    # 5. DEFAULT
    # -----------------------------
    else:
        print(f"ROUT_NODE 5.1  state['action'] - {state["action"]}")
        state["action"] = state.get("action", "search")
        print(f"ROUT_NODE 5.2  state['action'] - {state["action"]}")


    # -----------------------------
    # 6. MAIN FLOW
    # -----------------------------
    return agent.route(state)

