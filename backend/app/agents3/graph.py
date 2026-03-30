from typing import Literal
from langgraph.graph import StateGraph, END

# ✅ Импортируем AgentState из state.py (Pydantic BaseModel)
from .state import AgentState
from .nodes import assistant_node, tools_node


# =============================================================================
# МАРШРУТИЗАЦИЯ — ИСПРАВЛЕНО ДЛЯ PYDANTIC
# =============================================================================

def route_after_assistant(state: AgentState) -> Literal["tools", "__end__"]:
    """
    Определяет следующий узел после assistant_node.
    
    Если есть tool_calls — идём в tools, иначе — завершаем.
    """
    # ✅ Pydantic: используем getattr вместо .get()
    messages = getattr(state, 'messages', [])
    
    if not messages:
        return END
    
    last_message = messages[-1]
    
    # Проверяем наличие tool_calls (объект LangChain)
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    
    # Проверяем, не является ли сообщение результатом выполнения инструмента
    if hasattr(last_message, "type") and getattr(last_message, "type", None) == "tool":
        return "assistant"
    
    return END


# =============================================================================
# СБОРКА ГРАФА
# =============================================================================

def build_graph() -> StateGraph:
    """Создаёт и компилирует граф агента."""
    
    workflow = StateGraph(AgentState)
    
    # Регистрируем узлы
    workflow.add_node("assistant", assistant_node)
    workflow.add_node("tools", tools_node)
    
    # Точка входа
    workflow.set_entry_point("assistant")
    
    # Условные рёбра после assistant
    workflow.add_conditional_edges(
        "assistant",
        route_after_assistant,
        {
            "tools": "tools",
            END: END,
        }
    )
    
    # После выполнения инструментов возвращаемся к assistant для формирования финального ответа
    workflow.add_edge("tools", "assistant")
    
    # Компилируем граф
    return workflow.compile()


# Экспорт функции для импорта в main.py
get_agent = build_graph