from langgraph.checkpoint.memory import MemorySaver # Импортируем чекпоинтер
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition

from app.agents.state import AgentState
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.analysis_node import analysis_node
from app.agents.nodes.summary_node import summary_node
from app.agents.career_tools import career_tools_list

def create_career_graph():
    workflow = StateGraph(AgentState)

    # Регистрируем узлы
    workflow.add_node("ingestion", ingestion_node)
    workflow.add_node("analysis", analysis_node)
    workflow.add_node("summary", summary_node)
    workflow.add_node("tools", ToolNode(career_tools_list))

    # Настраиваем связи
    workflow.set_entry_point("ingestion")
    workflow.add_edge("ingestion", "analysis")
    workflow.add_edge("analysis", "summary")

    # Условные переходы (теперь без словаря, так надежнее)
    workflow.add_conditional_edges(
        "summary",
        tools_condition,
        {
            "tools": "tools",  # Если модель хочет вызвать инструмент
            "__end__": END     # Если модель просто ответила текстом (ВАЖНО!)
        }
    )

    # Обратная связь из тулз в чат
    workflow.add_edge("tools", END)

    # Инициализируем память
    memory = MemorySaver()

    # Компилируем граф С ПАМЯТЬЮ
    return workflow.compile(checkpointer=memory)

# Создаем глобальный объект приложения
career_app = create_career_graph()