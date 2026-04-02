from typing import Optional
import logging

from app.agents2.services.input_processor import InputProcessor
from app.agents2.nodes.router_node import router_node

processor = InputProcessor()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(
    message: Optional[str],
    file_bytes: Optional[bytes],
    state: dict,
) -> dict:
    if not isinstance(state, dict):
        state = {}

    # мягкий сброс при новом файле
    # мягкий сброс при новом файле
    if file_bytes:
        state = {
            "history": state.get("history", []),
            "stage": "idle",
            "top_vacancies": [],
            "action": "search",
        }

    # сообщение
    message = message or ""
    state["message"] = message

    logger.info(f"INPUT: {message}")

    # обработка входа
    state = processor.process(
        message=message,
        file_bytes=file_bytes,
        state=state
    )

    # роутинг
    state.setdefault("action", "search")
    state.setdefault("stage", "idle")
    state.setdefault("history", [])
    state = router_node(state)

    logger.info(f"ACTION: {state.get('action')}")

    # история
    state.setdefault("history", []).append({
        "user": message,
        "assistant": state.get("response"),
        "action": state.get("action")
    })

    # ограничение истории
    state["history"] = state["history"][-10:]

    return state