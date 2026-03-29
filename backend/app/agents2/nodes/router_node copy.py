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


# from app.agents2.tools.career_agent import CareerAgent
# from app.agents2.services.resume_parser import parse_pdf


# def router_node(state):
#     """
#     Router node:
#     - если есть файл → парсим PDF
#     - определяем action из команды
#     - запускаем CareerAgent
#     """

#     agent = CareerAgent()
#     message = (state.get("message") or "").lower()

#     # -------------------------------------------------
#     # 🔥 0. ЖЁСТКИЙ BYPASS: выбор вакансии по номеру
#     # -------------------------------------------------
#     if state.get("stage") == "waiting_vacancy_choice":
#         print("🔥 BYPASS → handle_vacancy_choice")
#         return agent.handle_vacancy_choice(state)

#     # -------------------------------------------------
#     # 🔥 1. ЕСЛИ ВВЕДЕНО ЧИСЛО — НЕ ТРОГАЕМ ACTION
#     # -------------------------------------------------
#     if message.isdigit():
#         print("🔢 DIGIT INPUT → skip action override")
#         return agent.route(state)

#     # -------------------------------------------------
#     # 2. FILE → parse resume
#     # -------------------------------------------------
#     if state.get("raw_file_content") and not state.get("candidate"):
#         parsed = parse_pdf(state["raw_file_content"], True)

#         if isinstance(parsed, str):
#             state["candidate"] = {
#                 "skills": parsed.lower().split()[:10],
#                 "city": None,
#                 "relocation": True
#             }
#         else:
#             state["candidate"] = parsed

#         state["last_action"] = "PDF распарсен"

#     # -------------------------------------------------
#     # 3. COMMANDS (из чата)
#     # -------------------------------------------------
#     if "search" in message:
#         state["action"] = "search"

#     elif "roadmap" in message:
#         state["action"] = "roadmap"

#     elif "resume" in message:
#         state["action"] = "resume"

#     elif "interview" in message:
#         state["action"] = "interview"

#     # -------------------------------------------------
#     # 4. AUTO SEARCH (если есть кандидат)
#     # -------------------------------------------------
#     elif state.get("candidate") and not state.get("top_vacancies"):
#         print("🤖 AUTO SEARCH TRIGGERED")
#         state["action"] = "search"

#     # -------------------------------------------------
#     # 5. DEFAULT (НЕ ПЕРЕТИРАЕМ action!)
#     # -------------------------------------------------
#     elif "action" not in state:
#         state["action"] = "search"

#     # -------------------------------------------------
#     # 6. MAIN AGENT
#     # -------------------------------------------------
#     state = agent.route(state)

#     return state

# from app.agents2.tools.career_agent import CareerAgent
# from app.agents2.services.resume_parser import parse_pdf


# def router_node(state):
#     """
#     Router node:
#     - если есть файл → парсим PDF
#     - определяем action из команды
#     - запускаем CareerAgent
#     """

#     agent = CareerAgent()

#     if state.get("stage") == "waiting_vacancy_choice":
#         return agent.route(state)
    
#     message = (state.get("message") or "").lower()

#     # -----------------------------
#     # 1. FILE → parse resume
#     # -----------------------------
#     if state.get("raw_file_content") and not state.get("candidate"):
#         parsed = parse_pdf(state["raw_file_content"], True)

#         if isinstance(parsed, str):
#             state["candidate"] = {
#                 "skills": parsed.lower().split()[:10],
#                 "city": None,
#                 "relocation": True
#             }
#         else:
#             state["candidate"] = parsed

#         state["last_action"] = "PDF распарсен"

#     # -----------------------------
#     # 2. COMMANDS (из чата)
#     # -----------------------------
#     if "search" in message:
#         state["action"] = "search"

#     elif "roadmap" in message:
#         state["action"] = "roadmap"

#     elif "resume" in message:
#         state["action"] = "resume"

#     elif "interview" in message:
#         state["action"] = "interview"

#     # -----------------------------
#     # 3. AUTO SEARCH (если есть кандидат)
#     # -----------------------------
#     elif state.get("candidate") and not state.get("top_vacancies"):
#         state["action"] = "search"

#     # -----------------------------
#     # 4. DEFAULT
#     # -----------------------------
#     else:
#         state["action"] = state.get("action", "search")

#     # -----------------------------
#     # 5. MAIN AGENT
#     # -----------------------------
#     state = agent.route(state)

#     return state
