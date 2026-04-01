"""
FastAPI-приложение для AI Career Assistant.

ИСПРАВЛЕНИЯ:
- _build_initial_state: поля согласованы с AgentState (все поля объявлены).
- adapt_resume_to_vacancy: вызов согласован с новой сигнатурой.
- extract_resume_data_from_state: вызывается с 1 аргументом.
- Убрано дублирование логики парсинга PDF.
- Добавлена передача state между запросами через JSON.
"""

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uuid
import json
import logging
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage, ToolMessage

from app.agents6.graph import get_agent
from app.agents6.state import AgentState, CandidateProfile
from app.agents6.resume_parser import parse_resume_from_pdf, parse_resume_from_text
from app.agents6.utils.pdf_parser import parse_pdf
from app.agents6.tools import vacancy_search_tool

from app.agents6.services.resume_adapter import (
    adapt_resume_to_vacancy,
    should_trigger_resume_adaptation,
    extract_resume_data_from_state,
)
from app.agents6.services.interview_service import (
    should_trigger_interview,
    start_interview,
    handle_interview_answer,
    set_llm_clients,
)

from app.agents6.nodes import get_llm_client, GROQ_MODEL_FAST, GROQ_MODEL_SMART

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Career Assistant Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

agent = None


@app.on_event("startup")
def startup():
    global agent
    logger.info("🚀 Инициализация агента...")
    agent = get_agent()

    try:
        fast_llm = get_llm_client("fast")
        smart_llm = get_llm_client("smart") if GROQ_MODEL_SMART != GROQ_MODEL_FAST else None
        set_llm_clients(fast_llm, smart_llm)
        logger.info(f"✅ LLM: fast={GROQ_MODEL_FAST}, smart={GROQ_MODEL_SMART if smart_llm else 'N/A'}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось инициализировать LLM для интервью: {e}")

    logger.info("✅ Агент готов")


# ═══════════════════════════════════════════════════════════════════════
# МОДЕЛИ ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None
    location: Optional[Dict[str, str]] = None


class ChatResponse(BaseModel):
    response: str
    thread_id: str
    interview_state: Optional[Dict[str, Any]] = None


# ═══════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════

def _extract_assistant_response(result: Dict[str, Any]) -> str:
    """Безопасно извлекает текст ответа из результата графа."""
    messages = result.get("messages", [])
    if not messages:
        logger.warning("⚠️ Пустой список сообщений")
        return "Нет ответа"

    # Ищем последний AIMessage
    for msg in reversed(messages):
        try:
            if isinstance(msg, AIMessage):
                if msg.content and msg.content.strip():
                    return msg.content.strip()
            elif isinstance(msg, dict):
                if msg.get("role") == "assistant" and msg.get("content"):
                    return str(msg["content"]).strip()
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при извлечении: {e}")
            continue

    # Фолбэк: последнее сообщение с контентом
    last_msg = messages[-1]
    if hasattr(last_msg, "content") and last_msg.content:
        return str(last_msg.content).strip()

    return "Извините, не удалось сформировать ответ. Попробуйте ещё раз."


def _build_initial_state(
    message: Optional[str],
    candidate_data: Optional[Dict] = None,
    candidate_resume: Optional[str] = None,
    thread_id: Optional[str] = None,
    location: Optional[Dict] = None,
    extra_fields: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Создаёт начальное состояние для agent.ainvoke().

    ИСПРАВЛЕНИЕ: все поля СОГЛАСОВАНЫ с AgentState.
    Возвращаем dict (LangGraph сам валидирует через Pydantic).
    """
    messages = [HumanMessage(content=message)] if message else []

    # Навыки из распарсенного резюме
    current_skills = []
    if candidate_data and isinstance(candidate_data, dict):
        skills = candidate_data.get("skills", [])
        if isinstance(skills, list):
            current_skills = [s for s in skills if isinstance(s, str) and s.strip()]
        elif isinstance(skills, str):
            current_skills = [s.strip() for s in skills.split(",") if s.strip()]

    # Создаём CandidateProfile если есть данные
    candidate = None
    if candidate_data and isinstance(candidate_data, dict):
        try:
            candidate = CandidateProfile(**{
                k: v for k, v in candidate_data.items()
                if k in CandidateProfile.model_fields
            })
        except Exception as e:
            logger.warning(f"⚠️ Не удалось создать CandidateProfile: {e}")

    base_state = {
        "messages": messages,
        "query": message,
        "location": location,
        "candidate": candidate,
        "candidate_resume": candidate_resume,
        "current_skills": current_skills,
        "thread_id": thread_id or str(uuid.uuid4()),
        "iteration_count": 0,
        "max_iterations": 5,
        "steps_taken": 0,
        "max_steps": 10,
        "visited_nodes": [],
        "history": [],
        "consecutive_tool_calls": 0,
        "last_tool_call": None,
    }

    if extra_fields:
        base_state.update(extra_fields)

    return base_state


# ═══════════════════════════════════════════════════════════════════════
# ЭНДПОИНТЫ
# ═══════════════════════════════════════════════════════════════════════

@app.post("/chat")
async def chat_legacy(
    request: Request,
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    """Обработчик чата с гарантированным возвратом ответа."""
    thread_id = None

    try:
        # === 1. Парсинг входных данных ===
        current_state = {}

        if state:
            try:
                current_state = json.loads(state)
                thread_id = current_state.get("thread_id")
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Невалидный state JSON")
                current_state = {}

        if not thread_id:
            thread_id = str(uuid.uuid4())

        config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 10}
        logger.info(f"📥 /chat: message='{(message or '')[:30]}...', thread_id={thread_id}")

        if not message and not file:
            return ChatResponse(
                response="Отправьте сообщение или загрузите резюме.",
                thread_id=thread_id,
            )

        # === 2. Парсинг файла (резюме) ===
        candidate_data, candidate_resume = None, None
        if file:
            try:
                file_bytes = await file.read()
                if file.content_type == "application/pdf":
                    candidate_resume = await parse_pdf(file_bytes)
                    candidate_data = await parse_resume_from_pdf(file_bytes)
                else:
                    candidate_resume = file_bytes.decode("utf-8", errors="ignore")
                    candidate_data = await parse_resume_from_text(candidate_resume)
                logger.info(f"📄 Резюме: {len(candidate_resume or '')} символов, навыки: {candidate_data.get('skills', [])}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка парсинга файла: {e}")

        # === 3. Формируем state ===
        input_state = _build_initial_state(
            message=message,
            candidate_data=candidate_data,
            candidate_resume=candidate_resume,
            thread_id=thread_id,
            location=current_state.get("location"),
        )

        # Копируем данные из previous state
        for key in ["market_context", "interview", "candidate", "skills_gap", "top_vacancies"]:
            if current_state.get(key) and not input_state.get(key):
                input_state[key] = current_state[key]

        # === 4. МИНИ-ИНТЕРВЬЮ ===
        existing_interview = input_state.get("interview") or current_state.get("interview")

        # 4.1 Продолжение активного интервью
        if existing_interview and isinstance(existing_interview, dict) and existing_interview.get("active"):
            logger.info(f"🎤 Интервью: index={existing_interview.get('current_index')}")
            try:
                result = await handle_interview_answer(
                    user_answer=message,
                    interview_state=existing_interview,
                )
                return ChatResponse(
                    response=result["response"],
                    thread_id=thread_id,
                    interview_state=result.get("interview_state"),
                )
            except Exception as e:
                logger.exception(f"⚠️ Ошибка интервью: {e}")
                return ChatResponse(response=f"⚠️ Ошибка: {str(e)[:200]}", thread_id=thread_id)

        # 4.2 Новое интервью
        elif message and should_trigger_interview(message):
            logger.info("🎤 Старт интервью")
            try:
                result = await start_interview(message, current_state, input_state)
                return ChatResponse(
                    response=result["response"],
                    thread_id=thread_id,
                    interview_state=result.get("interview_state"),
                )
            except Exception as e:
                logger.exception(f"⚠️ Ошибка старта интервью: {e}")
                return ChatResponse(response=f"⚠️ {str(e)[:200]}", thread_id=thread_id)

        # === 5. АДАПТАЦИЯ РЕЗЮМЕ ===
        if message and should_trigger_resume_adaptation(message):
            logger.info("✏️ Адаптация резюме")
            try:
                resume_data = extract_resume_data_from_state(current_state)
                result = await adapt_resume_to_vacancy(
                    message=message,
                    candidate_resume=resume_data.get("resume_text"),
                    vacancy_context=resume_data.get("vacancy_context"),
                )
                return ChatResponse(response=result, thread_id=thread_id)
            except Exception as e:
                logger.exception(f"⚠️ Ошибка адаптации: {e}")
                return ChatResponse(response=f"⚠️ {str(e)[:200]}", thread_id=thread_id)

        # === 6. ОБЫЧНЫЙ ПУТЬ: агент ===
        logger.info("🤖 Запуск графа...")
        result_dict: Dict = await agent.ainvoke(input_state, config=config)
        response_message = _extract_assistant_response(result_dict)

        logger.info(f"📤 Ответ: '{response_message[:100]}...'")
        return ChatResponse(response=response_message, thread_id=thread_id)

    except Exception as e:
        logger.exception(f"❌ GLOBAL ERROR: {type(e).__name__}: {e}")
        return ChatResponse(
            response=f"⚠️ Ошибка: {str(e)[:300]}",
            thread_id=thread_id or "unknown",
        )


@app.post("/chat_json", response_model=ChatResponse)
async def chat_json(request: ChatRequest):
    """JSON-endpoint для чата."""
    logger.info(f"📥 /chat_json: message='{request.message[:50]}...', thread_id={request.thread_id}")

    thread_id = request.thread_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 10}

    input_state = _build_initial_state(
        message=request.message,
        thread_id=thread_id,
        location=request.location,
    )

    result_dict = await agent.ainvoke(input_state, config=config)
    response_message = _extract_assistant_response(result_dict)

    logger.info(f"📤 Ответ: '{response_message[:100]}...'")
    return ChatResponse(response=response_message, thread_id=thread_id)


@app.post("/upload_resume")
async def upload_resume(file: UploadFile = File(...)):
    """Загрузка и парсинг резюме."""
    logger.info(f"📥 /upload_resume: {file.filename}, тип: {file.content_type}")

    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 10}

    file_bytes = await file.read()

    try:
        if file.content_type == "application/pdf":
            parsed = await parse_resume_from_pdf(file_bytes)
            text = await parse_pdf(file_bytes)
        else:
            text = file_bytes.decode("utf-8")
            parsed = await parse_resume_from_text(text)

        logger.info(f"✅ Резюме: навыки={parsed.get('skills', [])}")
    except Exception as e:
        logger.exception(f"❌ Парсинг: {e}")
        raise HTTPException(status_code=400, detail=f"Ошибка парсинга: {str(e)}")

    initial_state = _build_initial_state(
        message=None,
        candidate_data=parsed,
        candidate_resume=text,
        thread_id=thread_id,
        extra_fields={
            "user_input": text,
            "file_name": file.filename,
            "raw_file_content": file_bytes if file.content_type == "application/pdf" else None,
        },
    )

    try:
        await agent.aupdate_state(config, initial_state)
        logger.info("✅ State с резюме сохранён")
    except Exception as e:
        logger.exception(f"⚠️ aupdate_state: {e}")

    return {
        "message": "Резюме загружено и спарсено",
        "thread_id": thread_id,
        "parsed_candidate": parsed,
        "preview": text[:200] + "..." if len(text) > 200 else text,
    }


@app.get("/")
async def root():
    return {"status": "AI Career Assistant running", "version": "1.0"}


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "agent_initialized": agent is not None,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
