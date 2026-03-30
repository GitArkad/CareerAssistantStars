from app.agents.services.qdrant_service import search_for_candidate


def search_vacancies(candidate):
    return search_for_candidate(candidate)