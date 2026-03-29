from fastapi import FastAPI, UploadFile, File, Form
from typing import Optional
import json
import logging

from app.agents2.services.input_processor import InputProcessor
from app.agents2.nodes.router_node import router_node

app = FastAPI()
processor = InputProcessor()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------
# PIPELINE
# -----------------------------
def run_pipeline(message: str, file_bytes: Optional[bytes], state: dict):
    """
    Основной цикл обработки. 
    Исправлено: защита от ложного сброса стейта при вводе цифр.
    """
    if not isinstance(state, dict):
        state = {}

    # 0. ИНИЦИАЛИЗАЦИЯ (Гарантия структуры)
    state.setdefault("stage", "idle")
    state.setdefault("history", [])
    state.setdefault("top_vacancies", [])
    state.setdefault("action", "search")

    # Подготовка сообщения
    message = (message or "").strip()
    is_digit_input = message.isdigit()

    # 1. МЯГКИЙ СБРОС (С ЗАЩИТОЙ)
    # Сбрасываем контекст поиска ТОЛЬКО если пришел новый файл 
    # И пользователь НЕ вводит номер вакансии прямо сейчас.
    if file_bytes and not is_digit_input:
        logger.info("📄 [RESET] New file detected. Starting fresh context.")
        state["stage"] = "idle"
        state["top_vacancies"] = []
        state["selected_vacancy"] = None
        state["action"] = "search"
        state["raw_file_content"] = file_bytes
    elif file_bytes and is_digit_input:
        # Если пришел файл вместе с цифрой '1', '2' и т.д. — игнорируем файл, 
        # сохраняем текущий экшн (например, 'resume')
        logger.info(f"🔢 [KEEP] Digit '{message}' detected with file. Preserving context.")
    
    # 2. ОБНОВЛЕНИЕ СООБЩЕНИЯ В СТЕЙТЕ
    state["message"] = message

    # Если пользователь ввел текст вместо цифры во время выбора — сбрасываем стадию
    if message and not is_digit_input:
        low_msg = message.lower()
        commands = ["resume", "roadmap", "interview", "search"]
        if state.get("stage") == "waiting_vacancy_choice":
            if not any(cmd in low_msg for cmd in commands):
                state["stage"] = "idle"

    logger.info(f">>> PROCESSING: '{message}' | ACTION: {state['action']} | STAGE: {state['stage']}")

    # 3. ПРОЦЕССОР (Парсинг PDF)
    state = processor.process(
        message=message,
        file_bytes=file_bytes,
        state=state
    )

    # 4. РОУТИНГ (Router + CareerAgent)
    # Теперь router_node увидит правильный action='resume', а не сброшенный в search
    state = router_node(state)

    # 5. ОЧИСТКА ОТ БАЙТОВ (Фикс UnicodeDecodeError)
    # Удаляем бинарный контент перед сериализацией в JSON
    state.pop("raw_file_content", None)

    # 6. ЛОГИРОВАНИЕ РЕЗУЛЬТАТА
    logger.info(f"<<< RESULT: ACTION={state.get('action')} | STAGE={state.get('stage')}")

    # 7. ИСТОРИЯ
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


# -----------------------------
# FASTAPI ENDPOINT
# -----------------------------
@app.post("/chat")
async def chat(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    # 1. Восстановление стейта (JSON от клиента)
    current_state = {}
    if state:
        try:
            current_state = json.loads(state)
        except json.JSONDecodeError:
            logger.error("❌ Invalid state JSON")
            current_state = {}

    # 2. Чтение файла (если есть)
    file_content = None
    if file:
        file_content = await file.read()

    # 3. Pipeline execution
    try:
        updated_state = run_pipeline(
            message=message,
            file_bytes=file_content,
            state=current_state
        )
    except Exception as e:
        logger.exception("💥 Pipeline crashed")
        return {"error": str(e), "status": "failed"}

    # 4. Возвращаем всё клиенту
    # Клиент ОБЯЗАН прислать 'state' обратно в следующем запросе
    return {
        "response": updated_state.get("response"),
        "history": updated_state.get("history", []),
        "action": updated_state.get("action"),
        "stage": updated_state.get("stage"),
        # "state": updated_state 
    }

# from fastapi import FastAPI, UploadFile, File, Form
# from typing import Optional
# import json
# import logging

# from app.agents2.services.input_processor import InputProcessor
# from app.agents2.nodes.router_node import router_node

# app = FastAPI()
# processor = InputProcessor()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# # -----------------------------
# # PIPELINE
# # -----------------------------
# def run_pipeline(message, file_bytes, state):
#     if not isinstance(state, dict):
#         state = {}

#     # -----------------------------
#     # 0. INIT (🔥 важно для диалога)
#     # -----------------------------
#     state.setdefault("stage", "idle")
#     state.setdefault("history", [])
#     state.setdefault("top_vacancies", [])

#     # -----------------------------
#     # 1. мягкий сброс при новом файле
#     # -----------------------------
#     if file_bytes:
#         state = {
#             "history": state.get("history", []),
#             "stage": "idle"
#         }

#     # -----------------------------
#     # 2. сообщение
#     # -----------------------------
#     message = message or ""
#     state["message"] = message

#     if message and not message.isdigit():
#         if state.get("stage") == "waiting_vacancy_choice":
#             state["stage"] = "idle"

#     logger.info(f"INPUT: {message}")

#     # -----------------------------
#     # 3. обработка входа
#     # -----------------------------
#     state = processor.process(
#         message=message,
#         file_bytes=file_bytes,
#         state=state
#     )

#     # -----------------------------
#     # 4. роутинг (🔥 теперь учитывает stage)
#     # -----------------------------
#     state = router_node(state)

#     logger.info(f"ACTION: {state.get('action')} | STAGE: {state.get('stage')}")

#     # -----------------------------
#     # 5. история (чуть улучшили)
#     # -----------------------------
#     state["history"].append({
#         "user": message,
#         "assistant": state.get("response"),
#         "action": state.get("action"),
#         "stage": state.get("stage")
#     })

#     # анти-раздувание
#     state["history"] = state["history"][-10:]

#     return state


# # -----------------------------
# # FASTAPI ENDPOINT
# # -----------------------------
# @app.post("/chat")
# async def chat(
#     message: Optional[str] = Form(None),
#     file: Optional[UploadFile] = File(None),
#     state: Optional[str] = Form(None),
# ):
#     # -----------------------------
#     # 1. восстановление state
#     # -----------------------------
#     if state:
#         try:
#             state = json.loads(state)
#         except json.JSONDecodeError:
#             state = {}
#     else:
#         state = {}

#     # -----------------------------
#     # 2. читаем файл
#     # -----------------------------
#     file_content = await file.read() if file else None

#     # -----------------------------
#     # 3. pipeline
#     # -----------------------------
#     state = run_pipeline(
#         message=message,
#         file_bytes=file_content,
#         state=state
#     )

#     # -----------------------------
#     # 4. отдаём клиенту
#     # -----------------------------
#     return {
#         "response": state.get("response"),
#         "history": state.get("history", []),
#         "action": state.get("action"),
#         "stage": state.get("stage")  # 👈 ВАЖНО для фронта
#     }

# from fastapi import FastAPI, UploadFile, File, Form
# from typing import Optional
# import json
# import logging

# from app.agents2.services.input_processor import InputProcessor
# from app.agents2.nodes.router_node import router_node

# app = FastAPI()
# processor = InputProcessor()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# # -----------------------------
# # PIPELINE
# # -----------------------------
# def run_pipeline(message, file_bytes, state):
#     if not isinstance(state, dict):
#         state = {}

#     # -----------------------------
#     # 1. мягкий сброс при новом файле
#     # -----------------------------
#     if file_bytes:
#         state = {
#             "history": state.get("history", [])
#         }

#     # -----------------------------
#     # 2. сообщение
#     # -----------------------------
#     message = message or ""
#     state["message"] = message

#     logger.info(f"INPUT: {message}")

#     # -----------------------------
#     # 3. обработка входа
#     # -----------------------------
#     state = processor.process(
#         message=message,
#         file_bytes=file_bytes,
#         state=state
#     )

#     # -----------------------------
#     # 4. роутинг
#     # -----------------------------
#     state = router_node(state)

#     logger.info(f"ACTION: {state.get('action')}")

#     # -----------------------------
#     # 5. история
#     # -----------------------------
#     state.setdefault("history", []).append({
#         "user": message,
#         "assistant": state.get("response"),
#         "action": state.get("action")
#     })

#     # ограничение истории (анти-раздувание)
#     state["history"] = state["history"][-10:]

#     return state


# # -----------------------------
# # FASTAPI ENDPOINT
# # -----------------------------
# @app.post("/chat")
# async def chat(
#     message: Optional[str] = Form(None),
#     file: Optional[UploadFile] = File(None),
#     state: Optional[str] = Form(None),
# ):
#     # -----------------------------
#     # 1. восстановление state
#     # -----------------------------
#     if state:
#         try:
#             state = json.loads(state)
#         except json.JSONDecodeError:
#             state = {}
#     else:
#         state = {}

#     # -----------------------------
#     # 2. читаем файл
#     # -----------------------------
#     file_content = await file.read() if file else None

#     # -----------------------------
#     # 3. pipeline
#     # -----------------------------
#     state = run_pipeline(
#         message=message,
#         file_bytes=file_content,
#         state=state
#     )

#     # -----------------------------
#     # 4. отдаём клиенту
#     # -----------------------------
#     return {
#         "response": state.get("response"),   # последний ответ
#         "history": state.get("history", []), # вся история
#         "action": state.get("action")
#     }


# from fastapi import FastAPI, UploadFile, File, Form
# from typing import Optional
# import json

# from app.agents2.services.input_processor import InputProcessor
# from app.agents2.nodes.router_node import router_node

# app = FastAPI()

# processor = InputProcessor()


# # -----------------------------
# # PIPELINE (твоя логика)
# # -----------------------------
# def run_pipeline(message, file_bytes, state):
#     if not isinstance(state, dict):
#         state = {}

#     # -----------------------------
#     # 1. если пришел новый файл → сброс
#     # -----------------------------
#     if file_bytes:
#         state = {}

#     # -----------------------------
#     # 2. сохраняем сообщение
#     # -----------------------------
#     state["message"] = message

#     # -----------------------------
#     # 3. обработка входа (резюме / текст)
#     # -----------------------------
#     state = processor.process(
#         message=message,
#         file_bytes=file_bytes,
#         state=state
#     )

#     # -----------------------------
#     # 4. роутинг (search / roadmap / resume / interview)
#     # -----------------------------
#     state = router_node(state)

#     # -----------------------------
#     # 5. история
#     # -----------------------------
#     state.setdefault("history", []).append({
#         "message": message,
#         "action": state.get("action")
#     })

#     return state


# # -----------------------------
# # FASTAPI ENTRYPOINT
# # -----------------------------
# @app.post("/chat")
# async def chat(
#     message: Optional[str] = Form(None),
#     file: Optional[UploadFile] = File(None),
#     state: Optional[str] = Form(None),
# ):
#     # -----------------------------
#     # 1. восстановление state
#     # -----------------------------
#     if state:
#         try:
#             state = json.loads(state)
#         except json.JSONDecodeError:
#             state = {}
#     else:
#         state = {}

#     # -----------------------------
#     # 2. читаем файл
#     # -----------------------------
#     file_content = await file.read() if file else None

#     # -----------------------------
#     # 3. вызываем pipeline
#     # -----------------------------
#     result = run_pipeline(
#         message=message,
#         file_bytes=file_content,
#         state=state
#     )

#     # -----------------------------
#     # 4. отдаём результат
#     # -----------------------------
#     return result

# from fastapi import FastAPI, UploadFile, File, Form
# from typing import Optional
# import json

# from app.agents2.tools.career_agent import CareerAgent
# from app.agents2.services.input_processor import InputProcessor
# from app.agents2.nodes.router_node import router_node

# app = FastAPI()

# agent = CareerAgent()
# processor = InputProcessor()


# @app.post("/chat")
# async def chat(
#     message: Optional[str] = Form(None),
#     file: Optional[UploadFile] = File(None),
#     state: Optional[str] = Form(None),
# ):
#     import json

#     # -----------------------------
#     # 1. восстановление state
#     # -----------------------------
#     if state:
#         try:
#             state = json.loads(state)
#         except json.JSONDecodeError:
#             state = {}
#     else:
#         state = {}

#     # -----------------------------
#     # 2. читаем файл
#     # -----------------------------
#     file_content = None
#     if file:
#         file_content = await file.read()

#     # -----------------------------
#     # 3. обработка сообщения и/или файла
#     # -----------------------------
#     state["message"] = message
    
#     state = processor.process(
#     message=message,
#     file_bytes=file_content,
#     state=state
# )

#     # -----------------------------
#     # 4. запускаем агента
#     # -----------------------------
#     state = router_node(state)

#     # -----------------------------
#     # 5. история
#     # -----------------------------
#     state.setdefault("history", []).append({
#         "message": message,
#         "action": state.get("action")
#     })

#     return state