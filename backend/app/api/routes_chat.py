from typing import Optional
import json

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from app.services.chat_service import run_pipeline

router = APIRouter()


@router.post("/chat")
async def chat(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    try:
        if state:
            try:
                parsed_state = json.loads(state)
            except json.JSONDecodeError:
                parsed_state = {}
        else:
            parsed_state = {}

        if not isinstance(parsed_state, dict):
            parsed_state = {}

        file_content = await file.read() if file else None

        state_result = run_pipeline(
            message=message,
            file_bytes=file_content,
            state=parsed_state
        )

        return {
            "response": state_result.get("response"),
            "history": state_result.get("history", []),
            "action": state_result.get("action"),
            "stage": state_result.get("stage"),
        }

    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }