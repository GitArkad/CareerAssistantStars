from app.agents.state import AgentState
from app.agents.services.pdf_parser import parse_resume
from app.agents.services.qdrant_service import search_vacancies
from app.agents.services.market_analysis import analyze_market
from app.agents.services.llm import llm_invoke
from app.agents.services.intent_parser import detect_intent

from app.agents.services.new_tools import roadmap, resume_builder, mini_interview


def router_node(state: AgentState) -> AgentState:

    # =========================================================================
    # 1. PARSE
    # =========================================================================
    if not state.get("candidate"):
        if state.get("raw_file_content") or state.get("user_input"):
            state["candidate"] = parse_resume(state)

    if not state.get("candidate"):
        return state

    # =========================================================================
    # 2. INTENT
    # =========================================================================
    if not state.get("intent"):
        state["intent"] = detect_intent(state.get("user_input", ""))

    intent = state["intent"]
    last_action = state.get("last_action")

    # =========================================================================
    # 3. SEARCH
    # =========================================================================
    if intent in ["search", "market", "roadmap", "resume", "interview"]:

        if not state.get("top_vacancies") or intent == "search":
            if last_action != "search":

                query = llm_invoke(state)

                state["top_vacancies"] = search_vacancies(
                    query=query,
                    city=state["candidate"].get("city"),
                    relocation=state["candidate"].get("relocation", True),
                    limit=5
                )

                state["last_action"] = "search"
                return state

    # =========================================================================
    # 4. MARKET
    # =========================================================================
    if intent == "market":
        if last_action != "market":

            state["market"] = analyze_market(
                state["candidate"],
                state["top_vacancies"]
            )

            state["last_action"] = "market"

        return state

    # =========================================================================
    # 5. ROADMAP
    # =========================================================================
    if intent == "roadmap":
        if last_action != "roadmap":

            state["roadmap"] = roadmap.generate_roadmap(
                state["candidate"],
                state["top_vacancies"]
            )

            state["last_action"] = "roadmap"

        return state

    # =========================================================================
    # 6. RESUME
    # =========================================================================
    if intent == "resume":
        if last_action != "resume":

            state["custom_resume"] = resume_builder.build_resume(
                state["candidate"],
                state["top_vacancies"][0]
            )

            state["last_action"] = "resume"

        return state

    # =========================================================================
    # 7. INTERVIEW
    # =========================================================================
    if intent == "interview":
        if last_action != "interview":

            state["mini_interview"] = mini_interview.conduct_mini_interview(
                state["candidate"],
                state["top_vacancies"][0]
            )

            state["last_action"] = "interview"

        return state

    return state