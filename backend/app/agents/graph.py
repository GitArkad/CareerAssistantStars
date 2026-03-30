from langgraph.graph import StateGraph, END

from app.agents.state import AgentState
from app.agents.nodes.router_node import router_node


def create_career_graph():
    graph = StateGraph(AgentState)

    graph.add_node("router", router_node)

    graph.set_entry_point("router")
    graph.add_edge("router", END)

    return graph.compile()