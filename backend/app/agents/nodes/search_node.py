from app.agents.state import AgentState
from app.agents.services.qdrant_service import search_vacancies


def search_node(state: AgentState) -> AgentState:
    print(">>> SEARCH NODE START")

    candidate = state.get("candidate", {})

    skills = candidate.get("skills", [])
    specialization = candidate.get("specialization", "")
    city = candidate.get("city")

    # формируем запрос
    query = f"{specialization} {' '.join(skills)}"

    print(">>> QUERY:", query)
    print(">>> CITY:", city)

    vacancies = search_vacancies(query, city)

    print(">>> FOUND:", len(vacancies))

    state["market"] = {
        "top_vacancies": vacancies,
        "match_score": 0.0,
        "skill_gaps": [],
        "salary_median": 0,
        "salary_top_10": 0,
        "market_range": [0, 0],
    }

    state["next_step"] = "end"
    return state