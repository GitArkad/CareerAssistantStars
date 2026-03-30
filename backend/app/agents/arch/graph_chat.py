# graph.py

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition

from app.agents.state import AgentState
from app.agents.nodes.router_node import router_node
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.analysis_node import analysis_node
from app.agents.nodes.summary_node import summary_node
from app.agents.nodes.chat_node import chat_node
from app.agents.career_tools import career_tools_list


def create_career_graph():
    workflow = StateGraph(AgentState)

    # НОДЫ
    workflow.add_node("router", router_node)
    workflow.add_node("ingestion", ingestion_node)
    workflow.add_node("analysis", analysis_node)
    workflow.add_node("summary", summary_node)
    workflow.add_node("chat", chat_node)
    workflow.add_node("tools", ToolNode(career_tools_list))

    # ENTRY POINT
    workflow.set_entry_point("router")

    # ROUTER LOGIC (фикс)
    workflow.add_conditional_edges(
        "router",
        lambda state: state.get("stage", "chat"),
        {
            "chat": "chat",
            "ingestion": "ingestion",   # ← исправлено
        }
    )

    # ANALYSIS FLOW
    workflow.add_edge("ingestion", "analysis")
    workflow.add_edge("analysis", "summary")
    workflow.add_edge("summary", "chat")

    # TOOLS
    workflow.add_conditional_edges(
        "chat",
        tools_condition,
        {
            "tools": "tools",
            "__end__": END
        }
    )

    # возврат из tools обратно в chat
    workflow.add_edge("tools", "chat")

    # MEMORY
    memory = MemorySaver()

    return workflow.compile(checkpointer=memory)


# Глобальный объект
career_app = create_career_graph()