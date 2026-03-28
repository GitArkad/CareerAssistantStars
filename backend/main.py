from fastapi import FastAPI, UploadFile, File, Form
from typing import Optional
import json

from app.agents2.tools.career_agent import CareerAgent
from app.agents2.services.input_processor import InputProcessor
from app.agents2.nodes.router_node import router_node

app = FastAPI()

agent = CareerAgent()
processor = InputProcessor()


@app.post("/chat")
async def chat(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    import json

    # -----------------------------
    # 1. восстановление state
    # -----------------------------
    if state:
        try:
            state = json.loads(state)
        except json.JSONDecodeError:
            state = {}
    else:
        state = {}

    # -----------------------------
    # 2. читаем файл
    # -----------------------------
    file_content = None
    if file:
        file_content = await file.read()

    # -----------------------------
    # 3. обработка сообщения и/или файла
    # -----------------------------
    state["message"] = message
    
    state = processor.process(
    message=message,
    file_bytes=file_content,
    state=state
)

    # -----------------------------
    # 4. запускаем агента
    # -----------------------------
    state = router_node(state)

    # -----------------------------
    # 5. история
    # -----------------------------
    state.setdefault("history", []).append({
        "message": message,
        "action": state.get("action")
    })

    return state