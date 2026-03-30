from langgraph.graph import StateGraph, END
from app.agents.state import AgentState
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.analysis_node import analysis_node
from app.agents.nodes.summary_node import summary_node
from app.agents.nodes.roadmap_node import roadmap_node

def create_career_graph():
    workflow = StateGraph(AgentState)

    # 1. Регистрируем узлы
    workflow.add_node("ingestion", ingestion_node)
    workflow.add_node("analysis", analysis_node)
    workflow.add_node("summary", summary_node)
    workflow.add_node("roadmap", roadmap_node)

    # 2. Логика условного перехода
    def route_after_summary(state: AgentState):
        # Проверяем флаг в стейте, который поставил summary_node или UI
        step = state.get("next_step")
        if step == "roadmap":
            return "roadmap"
        return "exit" # Возвращаем строковый ключ

    # 3. Настройка связей (Edges)
    workflow.set_entry_point("ingestion")
    workflow.add_edge("ingestion", "analysis")
    workflow.add_edge("analysis", "summary")

    # Условные переходы из summary
    workflow.add_conditional_edges(
        "summary",
        route_after_summary,
        {
            "roadmap": "roadmap",
            "exit": END  # Теперь ключ "exit" ведет в системный END
        }
    )

    # ВАЖНО: Добавляем выход из roadmap
    workflow.add_edge("roadmap", END)

    return workflow.compile()

career_app = create_career_graph()