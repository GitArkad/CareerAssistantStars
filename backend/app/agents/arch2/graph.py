from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition
from app.agents.state import AgentState
from app.agents.nodes.router_node import router_node
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.agent_node import agent_node
from app.agents.tools import career_tools_list

def create_career_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("router", router_node)
    workflow.add_node("ingestion", ingestion_node)
    workflow.add_node("agent", agent_node)
    workflow.add_node("tools", ToolNode(career_tools_list))
    workflow.set_entry_point("router")
    workflow.add_conditional_edges(
        "router", 
        lambda state: state.get("stage", "agent"), 
        {"ingestion": "ingestion", "agent": "agent"}
        )
    workflow.add_edge("ingestion", "agent")
    workflow.add_conditional_edges(
        "agent", tools_condition, 
        {"tools": "tools", "__end__": END}
        )
    workflow.add_edge("tools", "agent")
    return workflow.compile(checkpointer=MemorySaver())

career_app = create_career_graph()