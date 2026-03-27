from langgraph.graph import StateGraph, END
from app.agents2.state import AgentState
from app.agents2.nodes.router_node import router_node


def create_career_graph():
    graph = StateGraph(AgentState)

    # единственная нода — роутер
    graph.add_node("router", router_node)

    graph.set_entry_point("router")

    # всегда завершаем после одного прохода
    graph.add_edge("router", END)

    return graph.compile()