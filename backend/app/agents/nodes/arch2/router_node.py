from langchain_core.messages import HumanMessage
from app.agents.state import AgentState

def router_node(state: AgentState):
    print("\n--- [ROUTER] ---")
    
    total_iterations = state.get("total_iterations", 0)
    if total_iterations >= 20:
        return {"stage": "agent", "error": "Лимит итераций (20)", "total_iterations": total_iterations + 1}
    
    messages = state.get("messages", [])
    last_user = messages[-1].content.lower().strip() if messages and isinstance(messages[-1], HumanMessage) else ""
    
    if state.get("raw_file_content") and not state.get("candidate", {}).get("skills"):
        attempts = state.get("ingestion_attempts", 0)
        if attempts >= 3:
            return {"stage": "agent", "error": "Не удалось обработать резюме (3 попытки)", 
                    "ingestion_attempts": attempts + 1, "total_iterations": total_iterations + 1}
        return {"stage": "ingestion", "ingestion_attempts": attempts + 1, "total_iterations": total_iterations + 1}
    
    if any(x in last_user for x in ["загрузить", "загрузи", "анализ резюме"]) and not state.get("candidate", {}).get("skills"):
        return {"stage": "ingestion", "ingestion_attempts": state.get("ingestion_attempts", 0) + 1, 
                "total_iterations": total_iterations + 1}
    
    return {"stage": "agent", "total_iterations": total_iterations + 1}