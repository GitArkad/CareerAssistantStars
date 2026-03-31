from langchain_core.messages import HumanMessage
from app.agents.state import AgentState


def router_node(state: AgentState):
    print("\n--- [ROUTER NODE] ---")

    messages = state.get("messages", [])

    last_user = (
        messages[-1].content.lower().strip()
        if messages and isinstance(messages[-1], HumanMessage)
        else ""
    )

    # 1. РЕЗЮМЕ → ingestion
    if "загрузить резюме" in last_user:
        return {"stage": "ingestion"}

    # 2. ROADMAP → tool
    if any(t in last_user for t in ["roadmap", "план", "что учить"]):
        return {"stage": "chat", "intent": "roadmap"}

    # 3. RESUME IMPROVE → tool
    if any(t in last_user for t in ["резюме", "улучши"]):
        return {"stage": "chat", "intent": "resume"}

    # 4. INTERVIEW
    if any(t in last_user for t in ["интервью", "собеседование"]):
        return {"stage": "chat", "intent": "interview"}

    # 5. ВСЁ ОСТАЛЬНОЕ → chat
    return {"stage": "chat"}