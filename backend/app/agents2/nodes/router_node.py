from app.agents2.tools.career_agent import CareerAgent
from app.agents2.services.resume_parser import parse_pdf

def router_node(state):
    """
    Router node:
    - если есть файл, парсим PDF
    - запускаем CareerAgent.route
    """
    agent = CareerAgent()

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
    # 2. MAIN AGENT
    # -----------------------------
    state = agent.route(state)

    return state