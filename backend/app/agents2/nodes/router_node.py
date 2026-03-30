from app.agents2.tools.career_agent import CareerAgent
from app.agents2.services.resume_parser import parse_pdf


def router_node(state):
    """
    Router node:
    - парсит PDF (если есть)
    - определяет action
    - передаёт ВСЁ в agent.route()
    """
    if not isinstance(state, dict):
        state = {}

    state.setdefault("action", "search")
    state.setdefault("stage", "idle")
    state.setdefault("history", [])
    state.setdefault("top_vacancies", [])
    
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
    if "search" in message or "найди" in message or "поиск" in message or "ваканс" in message:
        state["action"] = "search"

    elif "roadmap" in message or "дорожн" in message or "план" in message:
        state["action"] = "roadmap"

    elif "resume" in message or "резюме" in message or "cv" in message or "проанализируй" in message:
        state["action"] = "resume"

    elif "interview" in message or "собесед" in message:
        state["action"] = "interview"

    # -----------------------------
    # 4. AUTO SEARCH
    # -----------------------------
    elif (
        state.get("candidate")
        and not state.get("top_vacancies")
        and state.get("action") not in ["resume", "roadmap", "interview"]
    ):
        print(f"ROUT_NODE 4.1  state['action'] - {state.get('action')}")
        state["action"] = "search"
        print(f"ROUT_NODE 4.2  state['action'] - {state.get('action')}")
    # -----------------------------
    # 5. DEFAULT
    # -----------------------------
    else:
        print(f"ROUT_NODE 5.1  state['action'] - {state.get('action')}")
        state["action"] = state.get("action", "search")
        print(f"ROUT_NODE 5.2  state['action'] - {state.get('action')}")


    # -----------------------------
    # 6. MAIN FLOW
    # -----------------------------
    return agent.route(state)

