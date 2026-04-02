"""
Сборка LangGraph-графа агента.

ИСПРАВЛЕНИЯ:
- route_after_assistant: безопасный доступ через getattr.
- Добавлена проверка consecutive_tool_calls для защиты от зацикливания.
"""

from typing import Literal
from langgraph.graph import StateGraph, END

from .state import AgentState
from .nodes import assistant_node, tools_node


def route_after_assistant(state: AgentState) -> Literal["tools", "__end__"]:
    """Определяет следующий узел после assistant_node."""

    # 1. Защита от зацикливания по итерациям
    iteration_count = getattr(state, "iteration_count", 0)
    max_iterations = getattr(state, "max_iterations", 5)
    if iteration_count >= max_iterations:
        return END

    # 2. Защита от зацикливания по consecutive tool calls
    consecutive = getattr(state, "consecutive_tool_calls", 0)
    if consecutive >= 3:
        return END

    # 3. Проверка tool_calls в последнем сообщении
    messages = getattr(state, "messages", [])
    if not messages:
        return END

    last_message = messages[-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"

    # 4. Финал — текстовый ответ
    return END


def build_graph() -> StateGraph:
    """Создаёт и компилирует граф агента."""

    workflow = StateGraph(AgentState)

    workflow.add_node("assistant", assistant_node)
    workflow.add_node("tools", tools_node)

    workflow.set_entry_point("assistant")

    workflow.add_conditional_edges(
        "assistant",
        route_after_assistant,
        {
            "tools": "tools",
            END: END,
        },
    )

    workflow.add_edge("tools", "assistant")

    return workflow.compile()


get_agent = build_graph
