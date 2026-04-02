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
load_dotenv(override=True)

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uuid
import json
import logging
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage, ToolMessage

from agents6_2.graph import get_agent
from agents6_2.state import AgentState, CandidateProfile
from agents6_2.resume_parser import parse_resume_from_pdf, parse_resume_from_text
from agents6_2.utils.pdf_parser import parse_pdf
from agents6_2.tools import vacancy_search_tool

from agents6_2.services.resume_adapter import (
    adapt_resume_to_vacancy,
    should_trigger_resume_adaptation,
    extract_resume_data_from_state,
)
from agents6_2.services.interview_service import (
    should_trigger_interview,
    start_interview,
    handle_interview_answer,
    set_llm_clients,
)

from agents6_2.nodes import get_llm_client, GROQ_MODEL_FAST, GROQ_MODEL_SMART

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
session_store: Dict[str, Dict] = {}  # thread_id → state


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
    debug_state: Optional[Dict[str, Any]] = None
    state: Optional[str] = None  # JSON для передачи в следующий запрос


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

@app.get("/", response_class=HTMLResponse)
async def chat_ui():
    """Простой чат-интерфейс для тестирования."""
    return HTMLResponse(content="""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI Career Assistant</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f0f2f5; height: 100vh; display: flex; flex-direction: column; }
  header { background: #1a1a2e; color: white; padding: 14px 20px;
           display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 16px; font-weight: 600; }
  #thread-label { font-size: 11px; color: #888; }
  #messages { flex: 1; overflow-y: auto; padding: 20px;
              display: flex; flex-direction: column; gap: 12px; }
  .msg { max-width: 72%; padding: 10px 14px; border-radius: 16px;
         line-height: 1.5; font-size: 14px; white-space: pre-wrap; word-break: break-word; }
  .msg.user { align-self: flex-end; background: #1a1a2e; color: white;
              border-bottom-right-radius: 4px; }
  .msg.bot  { align-self: flex-start; background: white; color: #222;
              border-bottom-left-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
  .msg.bot strong { font-weight: 700; }
  .msg.typing { color: #999; font-style: italic; }
  #input-row { display: flex; gap: 8px; padding: 14px 20px;
               background: white; border-top: 1px solid #e0e0e0; }
  #msg-input { flex: 1; padding: 10px 14px; border: 1px solid #ddd;
               border-radius: 24px; font-size: 14px; outline: none;
               resize: none; max-height: 120px; overflow-y: auto; }
  #msg-input:focus { border-color: #1a1a2e; }
  #send-btn { padding: 10px 20px; background: #1a1a2e; color: white;
              border: none; border-radius: 24px; cursor: pointer;
              font-size: 14px; font-weight: 600; white-space: nowrap; }
  #send-btn:disabled { background: #ccc; cursor: not-allowed; }
  #new-chat-btn { padding: 6px 12px; background: transparent; color: #aaa;
                  border: 1px solid #444; border-radius: 12px; cursor: pointer;
                  font-size: 12px; }
  #new-chat-btn:hover { color: white; border-color: white; }
</style>
</head>
<body>
<header>
  <h1>🤖 AI Career Assistant</h1>
  <div style="display:flex;align-items:center;gap:12px">
    <span id="thread-label">thread: —</span>
    <button id="new-chat-btn" onclick="newChat()">Новый чат</button>
  </div>
</header>
<div id="messages">
  <div class="msg bot">Привет! Я помогу найти вакансии, составить план развития или адаптировать резюме. Что ищешь?</div>
</div>
<div id="input-row">
  <textarea id="msg-input" placeholder="Напиши сообщение..." rows="1"
    onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendMsg()}"
    oninput="this.style.height='auto';this.style.height=this.scrollHeight+'px'"></textarea>
  <button id="send-btn" onclick="sendMsg()">Отправить</button>
</div>
<script>
  function getThreadId() {
    let tid = localStorage.getItem('career_thread_id');
    if (!tid) { tid = 'chat-' + Math.random().toString(36).slice(2,9); localStorage.setItem('career_thread_id', tid); }
    return tid;
  }
  function newChat() {
    localStorage.removeItem('career_thread_id');
    location.reload();
  }
  document.getElementById('thread-label').textContent = 'thread: ' + getThreadId();

  function renderMarkdown(text) {
    return text
      .replace(/\\*\\*(.+?)\\*\\*/g, '<strong>$1</strong>')
      .replace(/^• /gm, '&bull; ')
      .replace(/\\n/g, '<br>');
  }

  function addMsg(text, role) {
    const div = document.createElement('div');
    div.className = 'msg ' + role;
    div.innerHTML = role === 'bot' ? renderMarkdown(text) : text;
    document.getElementById('messages').appendChild(div);
    div.scrollIntoView({behavior:'smooth'});
    return div;
  }

  async function sendMsg() {
    const input = document.getElementById('msg-input');
    const text = input.value.trim();
    if (!text) return;
    input.value = ''; input.style.height = 'auto';

    const btn = document.getElementById('send-btn');
    btn.disabled = true;

    addMsg(text, 'user');
    const typing = addMsg('Печатает...', 'bot typing');

    try {
      const res = await fetch('/chat_json', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({message: text, thread_id: getThreadId()})
      });
      const data = await res.json();
      typing.remove();
      addMsg(data.response || '(нет ответа)', 'bot');
    } catch(e) {
      typing.remove();
      addMsg('Ошибка соединения: ' + e.message, 'bot');
    }
    btn.disabled = false;
    input.focus();
  }
</script>
</body>
</html>""")


@app.post("/chat")
async def chat_legacy(
    request: Request,
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
    thread_id: Optional[str] = Form(None),
):
    """Обработчик чата с гарантированным возвратом ответа."""
    try:
        # === 1. Парсинг входных данных ===
        current_state = {}

        if state:
            try:
                current_state = json.loads(state)
                if not thread_id:
                    thread_id = current_state.get("thread_id")
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Невалидный state JSON")
                current_state = {}

        if not thread_id:
            thread_id = str(uuid.uuid4())

        # Восстанавливаем state из сервер-сайд хранилища если клиент не передал
        if not current_state and thread_id in session_store:
            current_state = session_store[thread_id]
            logger.info(f"🔄 Восстановлен state из session_store для thread_id={thread_id}")

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
        candidate = result_dict.get("candidate")
        candidate_skills = candidate.get("skills") if isinstance(candidate, dict) else getattr(candidate, "skills", None)
        mc = result_dict.get("market_context") or {}
        vacancies_count = len(mc.get("top_vacancies", [])) if isinstance(mc, dict) else 0
        logger.info(f"📊 STATE: skills={candidate_skills}, vacancies={vacancies_count}, iter={result_dict.get('iteration_count')}, filled_keys={[k for k, v in result_dict.items() if v]}")
        response_message = _extract_assistant_response(result_dict)

        debug_state = {
            "skills": candidate_skills,
            "vacancies_count": vacancies_count,
            "iteration_count": result_dict.get("iteration_count"),
            "skills_gap": result_dict.get("skills_gap"),
            "market_salary_median": mc.get("salary_median") if isinstance(mc, dict) else None,
            "filled_keys": [k for k, v in result_dict.items() if v],
        }
        # Сериализуем state для передачи в следующий запрос
        candidate_raw = result_dict.get("candidate")
        state_to_pass = {
            "thread_id": thread_id,
            "market_context": mc if isinstance(mc, dict) else None,
            "top_vacancies": result_dict.get("top_vacancies"),
            "skills_gap": result_dict.get("skills_gap"),
            "candidate": candidate_raw.model_dump() if hasattr(candidate_raw, "model_dump") else candidate_raw,
        }
        state_json = json.dumps({k: v for k, v in state_to_pass.items() if v is not None}, ensure_ascii=False)
        session_store[thread_id] = {k: v for k, v in state_to_pass.items() if v is not None}
        logger.info(f"💾 State сохранён для thread_id={thread_id}")

        logger.info(f"📤 Ответ: '{response_message[:100]}...'")
        return ChatResponse(response=response_message, thread_id=thread_id, debug_state=debug_state, state=state_json)

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

    # Восстанавливаем market_context и другие поля из сессии
    if thread_id in session_store:
        saved = session_store[thread_id]
        logger.info(f"🔄 /chat_json: восстановлен state из session_store для thread_id={thread_id}")
        for key in ["market_context", "interview", "candidate", "skills_gap", "top_vacancies"]:
            if saved.get(key) and not input_state.get(key):
                input_state[key] = saved[key]

    result_dict = await agent.ainvoke(input_state, config=config)
    response_message = _extract_assistant_response(result_dict)

    # Сохраняем state в сессию
    mc = result_dict.get("market_context") or {}
    session_store[thread_id] = {k: v for k, v in {
        "thread_id": thread_id,
        "market_context": mc if isinstance(mc, dict) else None,
        "top_vacancies": result_dict.get("top_vacancies"),
        "skills_gap": result_dict.get("skills_gap"),
        "candidate": result_dict.get("candidate"),
    }.items() if v is not None}
    logger.info(f"💾 /chat_json: state сохранён для thread_id={thread_id}")

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
