# app/agents/tools/generate_summary_node.py
from langgraph import Node
from app.agents.services.state import AgentState

class GenerateSummaryNode(Node):
    def run(self, state: AgentState):
        summary = f"Кандидат: {state.candidate.name}\nТоп вакансий:\n"
        for vac in state.market["top_vacancies"]:
            summary += f"- {vac['title']} ({vac['company']}) [{vac['city']}] - {vac.get('salary',0)}\n"
        summary += f"Skill gaps: {', '.join(state.market['skill_gaps'])}\n"
        summary += f"Зарплатная медиана: {state.market['salary_median']}, топ 10%: {state.market['salary_top_10']}\n"
        state.summary = summary
        return state