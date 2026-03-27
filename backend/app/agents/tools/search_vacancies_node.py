# app/agents/tools/search_vacancies_node.py
from langgraph import Node
from app.agents.services.state import AgentState

class SearchVacanciesNode(Node):
    def __init__(self, retriever):
        """
        retriever должен быть передан извне (Dependency Injection)
        """
        self.retriever = retriever

    def run(self, state: AgentState):
        candidate = state.candidate

        # Ищем вакансии в Qdrant через RAG
        vacancies = self.retriever.retrieve(candidate, top_k=20)
        state.market.top_vacancies = vacancies

        # Определяем пропущенные навыки
        candidate_skills = set(candidate.skills)
        top_skills = set(skill for v in vacancies for skill in v.get("skills", []))
        state.market.skill_gaps = list(top_skills - candidate_skills)

        # Зарплатная аналитика
        salaries = [v.get("salary", 0) for v in vacancies if v.get("salary")]
        if salaries:
            salaries_sorted = sorted(salaries)
            state.market.salary_median = int(salaries_sorted[len(salaries_sorted)//2])
            state.market.salary_top_10 = int(salaries_sorted[-max(1, len(salaries_sorted)//10)])
            state.market.market_range = [min(salaries), max(salaries)]

        return state