from fastapi import FastAPI, UploadFile, File, Form
from typing import Optional
import json
import logging

from app.agents2.services.input_processor import InputProcessor
from app.agents2.nodes.router_node import router_node

app = FastAPI()
processor = InputProcessor()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline(message: str, file_bytes: Optional[bytes], state: dict):
    if not isinstance(state, dict):
        state = {}

    state.setdefault("stage", "idle")
    state.setdefault("history", [])
    state.setdefault("top_vacancies", [])
    state.setdefault("action", "search")

    message = (message or "").strip()
    is_digit_input = message.isdigit()

    if file_bytes and not is_digit_input:
        logger.info("📄 [RESET] New file detected. Starting fresh context.")
        state["stage"] = "idle"
        state["top_vacancies"] = []
        state["selected_vacancy"] = None
        state["action"] = "search"
        state["raw_file_content"] = file_bytes
    elif file_bytes and is_digit_input:
        logger.info(f"🔢 [KEEP] Digit '{message}' detected with file. Preserving context.")

    state["message"] = message

    if message and not is_digit_input:
        low_msg = message.lower()
        commands = ["resume", "roadmap", "interview", "search"]
        if state.get("stage") == "waiting_vacancy_choice":
            if not any(cmd in low_msg for cmd in commands):
                state["stage"] = "idle"

    logger.info(f">>> PROCESSING: '{message}' | ACTION: {state['action']} | STAGE: {state['stage']}")

    state = processor.process(
        message=message,
        file_bytes=file_bytes,
        state=state
    )

    state = router_node(state)

    state.pop("raw_file_content", None)

    logger.info(f"<<< RESULT: ACTION={state.get('action')} | STAGE={state.get('stage')}")

    resp = state.get("response")
    summary = f"Found {len(resp)} items" if isinstance(resp, list) else str(resp)[:150]

    state["history"].append({
        "user": message,
        "assistant": summary,
        "action": state.get("action"),
        "stage": state.get("stage")
    })
    state["history"] = state["history"][-10:]

    return state


@app.post("/chat")
async def chat(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    current_state = {}
    if state:
        try:
            current_state = json.loads(state)
        except json.JSONDecodeError:
            logger.error("❌ Invalid state JSON")
            current_state = {}

    file_content = None
    if file:
        file_content = await file.read()

    try:
        updated_state = run_pipeline(
            message=message,
            file_bytes=file_content,
            state=current_state
        )
    except Exception as e:
        logger.exception("💥 Pipeline crashed")
        return {"error": str(e), "status": "failed"}

    return {
        "response": updated_state.get("response"),
        "history": updated_state.get("history", []),
        "action": updated_state.get("action"),
        "stage": updated_state.get("stage"),
    }