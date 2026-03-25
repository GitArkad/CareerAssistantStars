from langgraph.graph import StateGraph, END
from app.agents.state import AgentState
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.analysis_node import analysis_node
from app.agents.nodes.summary_node import summary_node
from app.agents.nodes.roadmap_node import roadmap_node

def create_career_graph():
    workflow = StateGraph(AgentState)

    # Добавляем узлы
    workflow.add_node("ingestion", ingestion_node) # парсинг резюме/сообщения
    workflow.add_node("analysis", analysis_node) # анализ рынка вакансий на основе БД
    workflow.add_node("summary", summary_node) # краткое саммари
    workflow.add_node("roadmap", roadmap_node)  # дорожная карта

    # Условный переход
    def route_after_summary(state: AgentState):
        # Эта логика может проверять последнее сообщение пользователя
        # Если в стейте зафиксировано согласие:
        if state.get("next_step") == "roadmap":
            return "roadmap"
        return END    

    # Настраиваем переходы
    workflow.set_entry_point("ingestion")
    workflow.add_edge("ingestion", "analysis")
    workflow.add_edge("analysis", "summary")
    # workflow.add_edge("summary", END)

    workflow.add_conditional_edges(
        "summary",
        route_after_summary,
        {
            "roadmap": "roadmap",
            "end": END
        }
    )

    # Компилируем граф
    return workflow.compile()

career_app = create_career_graph()