from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Union
import uuid
import json
import logging
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage, ToolMessage

from .agents3.graph import get_agent
from .agents3.state import AgentState
from .agents3.resume_parser import parse_resume_from_pdf, parse_resume_from_text
from .agents3.utils.pdf_parser import parse_pdf

from app.agents3.tools import vacancy_search_tool

# ============================================================================
# ИМПОРТЫ СЕРВИСОВ И МОДЕЛЕЙ
# ============================================================================
from app.agents3.services.resume_adapter import (
    adapt_resume_to_vacancy,
    should_trigger_resume_adaptation,
    extract_resume_data_from_state
)
from app.agents3.services.interview_service import (
    should_trigger_interview,
    start_interview,
    handle_interview_answer,
    set_llm_clients
)

# Импортируем фабрику LLM из nodes для инициализации сервисов
from app.agents3.nodes import get_llm_client, GROQ_MODEL_FAST, GROQ_MODEL_SMART

# Настройка логирования
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

# Глобальная переменная для агента
agent = None

@app.on_event("startup")
def startup():
    global agent
    logger.info("🚀 Инициализация агента...")
    agent = get_agent()
    
    # 🔥 Инициализируем LLM-клиенты для сервисов интервью
    try:
        fast_llm = get_llm_client("fast")
        smart_llm = get_llm_client("smart") if GROQ_MODEL_SMART != GROQ_MODEL_FAST else None
        set_llm_clients(fast_llm, smart_llm)
        logger.info(f"✅ LLM инициализирован для interview_service: fast={GROQ_MODEL_FAST}, smart={GROQ_MODEL_SMART if smart_llm else 'N/A'}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось инициализировать LLM для интервью: {e}")
    
    logger.info("✅ Агент готов")


# =============================================================================
# МОДЕЛИ ДАННЫХ
# =============================================================================

class ChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None
    location: Optional[Dict[str, str]] = None  # Для фильтра по локации


class ChatResponse(BaseModel):
    response: str
    thread_id: str
    interview_state: Optional[Dict[str, Any]] = None


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def _extract_assistant_response(result: Dict[str, Any]) -> str:
    """
    Безопасно извлекает текст ответа ассистента из результата графа.
    
    Обрабатывает:
    - Объекты LangChain (AIMessage, HumanMessage, ToolMessage)
    - Словари (после сериализации/валидации)
    - Пустые или ошибочные ответы
    
    Returns:
        str: Текст ответа или сообщение об ошибке
    """
    messages = result.get("messages", [])
    if not messages:
        logger.warning("⚠️ [_extract_assistant_response] Пустой список сообщений")
        return "Нет ответа"
    
    # Ищем последнее сообщение от ассистента (в обратном порядке)
    for msg in reversed(messages):
        try:
            # Вариант 1: объект AIMessage от LangChain
            if isinstance(msg, AIMessage):
                if msg.content and msg.content.strip():
                    logger.info(f"✅ Извлечён ответ из AIMessage: '{msg.content[:50]}...'")
                    return msg.content.strip()
            
            # Вариант 2: словарь с ролью "assistant"
            elif isinstance(msg, dict):
                if msg.get("role") == "assistant" and msg.get("content"):
                    content = msg["content"]
                    if content and str(content).strip():
                        logger.info(f"✅ Извлечён ответ из dict: '{str(content)[:50]}...'")
                        return str(content).strip()
            
            # Вариант 3: объект с атрибутами role/content (универсальный)
            elif hasattr(msg, "role") and hasattr(msg, "content"):
                if getattr(msg, "role", None) == "assistant":
                    content = getattr(msg, "content", "")
                    if content and str(content).strip():
                        return str(content).strip()
                        
        except Exception as e:
            logger.warning(f"⚠️ Пропущено сообщение при извлечении: {e}")
            continue
    
    # Фолбэк: берём последнее сообщение любого типа, если есть контент
    last_msg = messages[-1]
    if isinstance(last_msg, dict):
        content = last_msg.get("content", "")
        if content:
            logger.info(f"⚠️ Фолбэк: взят контент из последнего dict: '{str(content)[:50]}...'")
            return str(content).strip()
    elif hasattr(last_msg, "content"):
        content = getattr(last_msg, "content", "")
        if content:
            logger.info(f"⚠️ Фолбэк: взят контент из последнего объекта: '{str(content)[:50]}...'")
            return str(content).strip()
    
    logger.warning("❌ Не удалось извлечь ответ из сообщений")
    return "Извините, не удалось сформировать ответ. Попробуйте ещё раз."


def _build_initial_state(
    message: Optional[str],
    candidate_data: Optional[Dict] = None,
    candidate_resume: Optional[str] = None,
    thread_id: Optional[str] = None,
    location: Optional[Dict] = None,
    extra_fields: Optional[Dict] = None
) -> AgentState:
    """
    Создаёт начальное состояние агента с обязательными полями.
    
    Ключевое: добавляет "query" для триггера tools_node.
    """
    messages = [HumanMessage(content=message)] if message else []
    
    # Извлекаем навыки из распарсенного резюме для skills_gap
    current_skills = []
    if candidate_data and isinstance(candidate_data, dict):
        skills = candidate_data.get("skills", [])
        if isinstance(skills, list):
            current_skills = [s for s in skills if isinstance(s, str) and s.strip()]
        elif isinstance(skills, str):
            current_skills = [s.strip() for s in skills.split(",") if s.strip()]
    
    base_state = {
        "messages": messages,
        "query": message,  # 🔥 КРИТИЧНО: без этого tools_node не запустит анализ рынка
        "location": location,  # Опциональный фильтр по локации
        "candidate": candidate_data,
        "candidate_resume": candidate_resume,
        "current_skills": current_skills,  # Для расчёта skill gap
        "thread_id": thread_id or str(uuid.uuid4()),
        "iteration_count": 0,
        "max_iterations": 5,
        "steps_taken": 0,
        "max_steps": 10,
        "visited_nodes": set(),
        "history": [],
        "consecutive_tool_calls": 0,
        "last_tool_call": None
    }
    
    # Добавляем дополнительные поля, если переданы
    if extra_fields:
        base_state.update(extra_fields)
    
    return base_state  # type: ignore


# =============================================================================
# ЭНДПОИНТЫ
# =============================================================================
@app.post("/chat")
async def chat_legacy(
    request: Request,
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
):
    """Обработчик чата с гарантированным возвратом ответа (никогда не null)."""
    
    # 🔧 ГЛОБАЛЬНЫЙ TRY: ловим ВСЕ ошибки и гарантируем возврат
    try:
        # === 1. Парсинг входных данных ===
        thread_id = None
        current_state = {}
        
        if state:
            try:
                current_state = json.loads(state)
                thread_id = current_state.get("thread_id")
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Не удалось распарсить state: {state[:100]}")
                current_state = {}
        
        if not thread_id:
            thread_id = str(uuid.uuid4())
        
        config = {"configurable": {"thread_id": thread_id}}
        logger.info(f"📥 /chat: message='{message[:30]}...', thread_id={thread_id}")

        # === 2. Парсинг файла (резюме) ===
        candidate_data, candidate_resume = None, None
        if file:
            try:
                file_bytes = await file.read()
                candidate_resume = file_bytes.decode('utf-8', errors='ignore')
                logger.info(f"📄 Получено резюме: {len(candidate_resume)} символов")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка чтения файла: {e}")

        # === 3. Build input_state ===
        input_state = _build_initial_state(
            message=message,
            candidate_data=candidate_data,
            candidate_resume=candidate_resume,
            thread_id=thread_id,
            location=current_state.get("location")
        )
        
        # 🔧 Копируем interview из current_state в input_state
        if current_state.get("interview"):
            input_state["interview"] = current_state["interview"]
            logger.info(f"🎤 interview copied: active={input_state['interview'].get('active')}")

        # === 4. 🔥 МИНИ-ИНТЕРВЬЮ: приоритетная обработка ===
        existing_interview = input_state.get("interview") or current_state.get("interview")
        
        # 4.1 Продолжение активного интервью
        if existing_interview and isinstance(existing_interview, dict) and existing_interview.get("active"):
            logger.info(f"🎤 Processing interview answer: index={existing_interview.get('current_index')}")
            try:
                result = await handle_interview_answer(
                    user_answer=message,
                    interview_state=existing_interview
                )
                return ChatResponse(
                    response=result["response"],
                    thread_id=thread_id,
                    interview_state=result.get("interview_state")
                )
            except Exception as e:
                logger.exception(f"⚠️ Error in handle_interview_answer: {e}")
                return ChatResponse(
                    response=f"⚠️ Ошибка в интервью: {str(e)[:200]}",
                    thread_id=thread_id
                )
        
        # 4.2 Запрос на начало нового интервью
        elif should_trigger_interview(message):
            logger.info("🎤 Starting new interview")
            try:
                result = await start_interview(message, current_state, input_state)
                return ChatResponse(
                    response=result["response"],
                    thread_id=thread_id,
                    interview_state=result.get("interview_state")
                )
            except Exception as e:
                logger.exception(f"⚠️ Error in start_interview: {e}")
                return ChatResponse(
                    response=f"⚠️ Не удалось начать интервью: {str(e)[:200]}",
                    thread_id=thread_id
                )

        # === 5. АДАПТАЦИЯ РЕЗЮМЕ ===
        if should_trigger_resume_adaptation(message):
            logger.info("✏️ Triggering resume adaptation")
            try:
                resume_data = extract_resume_data_from_state(current_state)
                result = await adapt_resume_to_vacancy(
                    message=message,
                    candidate_resume=resume_data.get("resume_text"),
                    vacancy_context=resume_data.get("vacancy_context")
                )
                return ChatResponse(response=result, thread_id=thread_id)
            except Exception as e:
                logger.exception(f"⚠️ Error in resume adaptation: {e}")
                return ChatResponse(
                    response=f"⚠️ Ошибка адаптации резюме: {str(e)[:200]}",
                    thread_id=thread_id
                )

        # === 6. ОБЫЧНЫЙ ПУТЬ: запуск графа агента ===
        logger.info("🤖 Running agent graph...")
        
        # Обновляем состояние агента, если есть данные кандидата
        if candidate_data:
            try:
                await agent.aupdate_state(config, input_state)
            except Exception as e:
                logger.warning(f"⚠️ aupdate_state failed (continuing): {e}")
        
        # Запускаем агент
        result_dict: Dict = await agent.ainvoke(input_state, config=config)
        response_message = _extract_assistant_response(result_dict)
        
        logger.info(f"📤 Response: '{response_message[:100]}...'")
        return ChatResponse(response=response_message, thread_id=thread_id)

    # ========================================================================
    # 🔧 ГЛОБАЛЬНЫЙ EXCEPT: НИКОГДА не возвращать None / null
    # ========================================================================
    except Exception as e:
        logger.exception(f"❌ GLOBAL ERROR in chat_legacy: {type(e).__name__}: {e}")
        return ChatResponse(
            response=f"⚠️ Внутренняя ошибка: {str(e)[:300]}",
            thread_id=thread_id if thread_id else "unknown"
        )


# @app.post("/chat")
# async def chat_legacy(
#     request: Request,
#     message: Optional[str] = Form(None),
#     file: Optional[UploadFile] = File(None),
#     state: Optional[str] = Form(None),
# ):
#     # 🔥 ЖЕСТКОЕ ЛОГИРОВАНИЕ ДЛЯ ОТЛАДКИ
#     logger.info(f"📥 /chat: msg='{message[:30]}...', state_len={len(state) if state else 0}")
    
#     # Парсим state с подробным логом
#     parsed_state = {}
#     if state:
#         try:
#             parsed_state = json.loads(state)
#             logger.info(f"🔍 state parsed, keys: {list(parsed_state.keys())}")
            
#             # Проверяем interview
#             intv = parsed_state.get("interview")
#             if intv:
#                 logger.info(f"🎤 interview key FOUND: active={intv.get('active')}, type={type(intv)}")
#                 if isinstance(intv, dict):
#                     logger.info(f"🎤 interview dict keys: {list(intv.keys())}")
#             else:
#                 logger.info(f"⚠️ interview key NOT FOUND in state")
                
#         except json.JSONDecodeError as e:
#             logger.error(f"❌ JSON parse error: {e}")
#             logger.error(f"📄 Raw state preview: {state[:200]}...")
#         except Exception as e:
#             logger.error(f"❌ Unexpected error parsing state: {e}")
    
#     # Сохраняем parsed_state для дальнейшего использования
#     current_state = parsed_state if parsed_state else current_state
    
#     # Читаем файл резюме, если прикреплён
#     file_bytes = None
#     if file:
#         file_bytes = await file.read()
#         logger.info(f"📄 Загружен файл: {file.filename}, тип: {file.content_type}, размер: {len(file_bytes)} байт")

#     thread_id = current_state.get("thread_id") or str(uuid.uuid4())
#     config = {"configurable": {"thread_id": thread_id}}
#     logger.info(f"🧵 thread_id: {thread_id}")

#     # Парсинг резюме
#     candidate_data, candidate_resume = None, None
#     if file_bytes:
#         try:
#             if file.content_type == "application/pdf":
#                 candidate_data = await parse_resume_from_pdf(file_bytes)
#                 candidate_resume = await parse_pdf(file_bytes)
#             else:
#                 candidate_resume = file_bytes.decode("utf-8")
#                 candidate_data = await parse_resume_from_text(candidate_resume)
#             logger.info(f"✅ Резюме распарсено: навыки={candidate_data.get('skills', []) if candidate_data else None}")
#         except Exception as e:
#             logger.exception(f"❌ Ошибка парсинга резюме: {e}")

#     # ✅ Создаём состояние с обязательным "query"
#     input_state = _build_initial_state(
#         message=message,
#         candidate_data=candidate_data,
#         candidate_resume=candidate_resume,
#         thread_id=thread_id,
#         location=current_state.get("location")
#     )

#     # Обновляем состояние агента, если есть данные кандидата
#     if candidate_data:
#         try:
#             await agent.aupdate_state(config, input_state)
#             logger.info("✅ Состояние агента обновлено")
#         except Exception as e:
#             logger.exception(f"⚠️ Ошибка обновления состояния: {e}")

#     # ========================================================================
#     # 🔥 ПРЯМОЙ ВЫЗОВ АДАПТАЦИИ РЕЗЮМЕ (через сервис)
#     # ========================================================================
#     if should_trigger_resume_adaptation(message):
#         logger.info("✏️ Детектирован запрос на адаптацию резюме — вызов сервиса")
        
#         try:
#             declared_skills, vacancy_payload = extract_resume_data_from_state(
#                 current_state, input_state, candidate_data
#             )
            
#             result = await adapt_resume_to_vacancy(
#                 candidate_resume=candidate_resume or "",
#                 declared_skills=declared_skills,
#                 vacancy_payload=vacancy_payload
#             )
            
#             logger.info(f"✅ Сервис адаптации вернул: {result.get('status')}")
#             return ChatResponse(response=result["response"], thread_id=thread_id)
        
#         except Exception as e:
#             logger.exception(f"⚠️ Ошибка в сервисе адаптации резюме: {e}")
#             # Продолжаем с обычным агентом, если сервис упал
#     # ========================================================================

#     # ========================================================================
#     # 🔥 МИНИ-ИНТЕРВЬЮ: детекция и обработка (через сервис)
#     # ========================================================================
    
#     # 🔥 ЖЁСТКОЕ ЛОГИРОВАНИЕ — что видит сервер ПРЯМО ПЕРЕД проверкой
#     logger.info(f"🎤 === INTERVIEW CHECK START ===")
#     logger.info(f"🎤 message: '{message[:30]}...'")
#     logger.info(f"🎤 input_state has 'interview': {'interview' in input_state}")
#     logger.info(f"🎤 current_state has 'interview': {'interview' in current_state}")
    
#     existing_interview = input_state.get("interview") or current_state.get("interview")
    
#     if existing_interview:
#         logger.info(f"🎤 existing_interview FOUND: type={type(existing_interview)}, active={existing_interview.get('active') if isinstance(existing_interview, dict) else 'N/A'}")
#     else:
#         logger.info(f"⚠️ existing_interview NOT FOUND")
    
#     # 1️⃣ Проверяем активное интервью
#     if existing_interview and isinstance(existing_interview, dict) and existing_interview.get("active"):
#         logger.info(f"🎤 ✅ Processing interview answer: index={existing_interview.get('current_index')}, answer='{message[:20]}...'")
        
#         try:
#             result = await handle_interview_answer(
#                 user_answer=message,
#                 interview_state=existing_interview
#             )
            
#             logger.info(f"✅ Interview response generated: {result['response'][:50]}...")
            
#             return ChatResponse(
#                 response=result["response"],
#                 thread_id=thread_id,
#                 interview_state=result.get("interview_state")
#             )
        
#         except Exception as e:
#             logger.exception(f"⚠️ Error in handle_interview_answer: {e}")
#             return ChatResponse(
#                     response=f"⚠️ Ошибка в интервью: {str(e)[:200]}",
#                     thread_id=thread_id
#                 )
    
#     # 2️⃣ Если не активно — проверяем запрос на новое интервью
#     elif should_trigger_interview(message):
#         logger.info("🎤 Starting new interview")
        
#         try:
#             result = await start_interview(message, current_state, input_state)
            
#             return ChatResponse(
#                 response=result["response"],
#                 thread_id=thread_id,
#                 interview_state=result.get("interview_state")
#             )
        
#         except Exception as e:
#             logger.exception(f"⚠️ Ошибка в сервисе интервью: {e}")
#             # Возвращаем понятную ошибку вместо null
#             return ChatResponse(
#                 response=f"⚠️ Временно не удалось начать интервью: {str(e)[:200]}. Попробуйте ещё раз.",
#                 thread_id=thread_id
#             )
#             # Продолжаем с обычным агентом
#     # ========================================================================

#     # Запускаем граф (обычный путь)
#     logger.info("🤖 Запуск agent.ainvoke...")

#     result_dict: Dict = await agent.ainvoke(input_state, config=config)
#     logger.info(f"✅ Граф завершён, ключи результата: {list(result_dict.keys())}")
    
#     # ✅ Безопасное извлечение ответа
#     response_message = _extract_assistant_response(result_dict)
#     logger.info(f"📤 Ответ пользователю: '{response_message[:100]}...'")
    
#     return ChatResponse(response=response_message, thread_id=thread_id)


@app.post("/chat_json", response_model=ChatResponse)
async def chat_json(request: ChatRequest):
    logger.info(f"📥 /chat_json запрос: message='{request.message[:50]}...', thread_id={request.thread_id}")
    
    thread_id = request.thread_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    # ✅ Создаём состояние с "query" и "location"
    input_state = _build_initial_state(
        message=request.message,
        thread_id=thread_id,
        location=request.location  # Поддержка фильтра по локации
    )
    
    logger.info("🤖 Запуск agent.ainvoke...")
    result_dict = await agent.ainvoke(input_state, config=config)
    
    response_message = _extract_assistant_response(result_dict)
    logger.info(f"📤 Ответ: '{response_message[:100]}...'")
    
    return ChatResponse(response=response_message, thread_id=thread_id)


@app.post("/upload_resume")
async def upload_resume(file: UploadFile = File(...)):
    logger.info(f"📥 /upload_resume: {file.filename}, тип: {file.content_type}")
    
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    file_bytes = await file.read()
    
    try:
        if file.content_type == "application/pdf":
            parsed = await parse_resume_from_pdf(file_bytes)
            text = await parse_pdf(file_bytes)
        else:
            text = file_bytes.decode("utf-8")
            parsed = await parse_resume_from_text(text)
        
        logger.info(f"✅ Резюме распарсено: {parsed.get('skills', []) if parsed else None}")
    except Exception as e:
        logger.exception(f"❌ Ошибка парсинга: {e}")
        raise HTTPException(status_code=400, detail=f"Ошибка парсинга: {str(e)}")
    
    # Создаём состояние с распарсенными данными
    initial_state = _build_initial_state(
        message=None,
        candidate_data=parsed,
        candidate_resume=text,
        thread_id=thread_id,
        extra_fields={
            "user_input": text,
            "file_name": file.filename,
            "raw_file_content": file_bytes if file.content_type == "application/pdf" else None,
        }
    )
    
    try:
        await agent.aupdate_state(config, initial_state)
        logger.info("✅ Состояние с резюме сохранено")
    except Exception as e:
        logger.exception(f"⚠️ Ошибка сохранения состояния: {e}")
    
    return {
        "message": "Резюме загружено и спарсено",
        "thread_id": thread_id,
        "parsed_candidate": parsed,
        "preview": text[:200] + "..." if len(text) > 200 else text
    }


@app.get("/")
async def root():
    return {"status": "AI Career Assistant running", "version": "1.0"}


@app.get("/health")
async def health_check():
    """Эндпоинт для проверки работоспособности."""
    return {
        "status": "ok",
        "agent_initialized": agent is not None,
        "timestamp": str(uuid.uuid4())
    }