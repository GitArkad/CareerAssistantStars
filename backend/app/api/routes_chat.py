import json
import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, File, Form, UploadFile
from pydantic import BaseModel
from langchain_core.messages import AIMessage, HumanMessage

from app.agents6_2.graph import get_agent
from app.agents6_2.nodes import (
    GROQ_MODEL_FAST,
    GROQ_MODEL_SMART,
    generate_roadmap_func,
    get_llm_client,
)
from app.agents6_2.resume_parser import parse_resume_from_pdf, parse_resume_from_text
from app.agents6_2.services.interview_service import (
    handle_interview_answer,
    set_llm_clients,
    should_trigger_interview,
    start_interview,
)
from app.agents6_2.services.resume_adapter import (
    adapt_resume_to_vacancy,
    extract_resume_data_from_state,
    should_trigger_resume_adaptation,
)
from app.agents6_2.state import CandidateProfile
from app.agents6_2.utils.pdf_parser import parse_pdf

router = APIRouter()
logger = logging.getLogger(__name__)

agent = None
session_store: Dict[str, Dict[str, Any]] = {}


class ChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None
    location: Optional[Dict[str, str]] = None


def _extract_assistant_response(result: Dict[str, Any]) -> str:
    messages = result.get("messages", [])
    if not messages:
        return "Нет ответа"

    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and msg.content:
            return str(msg.content).strip()
        if isinstance(msg, dict) and msg.get("role") == "assistant" and msg.get("content"):
            return str(msg["content"]).strip()

    last_message = messages[-1]
    if hasattr(last_message, "content") and last_message.content:
        return str(last_message.content).strip()

    return "Извините, не удалось сформировать ответ. Попробуйте ещё раз."


def _ensure_agent() -> None:
    global agent
    if agent is None:
        startup()


def _serialize_candidate(candidate: Any) -> Optional[Dict[str, Any]]:
    if candidate is None:
        return None
    if hasattr(candidate, "model_dump"):
        return candidate.model_dump()
    if isinstance(candidate, dict):
        return candidate
    return None


def _build_initial_state(
    message: Optional[str],
    candidate_data: Optional[Dict[str, Any]] = None,
    candidate_resume: Optional[str] = None,
    thread_id: Optional[str] = None,
    location: Optional[Dict[str, str]] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    messages = [HumanMessage(content=message)] if message else []

    current_skills = []
    if isinstance(candidate_data, dict):
        skills = candidate_data.get("skills", [])
        if isinstance(skills, list):
            current_skills = [s for s in skills if isinstance(s, str) and s.strip()]
        elif isinstance(skills, str):
            current_skills = [s.strip() for s in skills.split(",") if s.strip()]

    candidate = None
    if isinstance(candidate_data, dict):
        try:
            candidate = CandidateProfile(
                **{k: v for k, v in candidate_data.items() if k in CandidateProfile.model_fields}
            )
        except Exception as exc:
            logger.warning("Не удалось собрать CandidateProfile: %s", exc)

    state = {
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
        state.update(extra_fields)

    return state


def _restore_state(input_state: Dict[str, Any], stored_state: Dict[str, Any]) -> Dict[str, Any]:
    for key in ["market_context", "interview", "candidate", "skills_gap", "top_vacancies"]:
        if stored_state.get(key) and not input_state.get(key):
            input_state[key] = stored_state[key]
    return input_state


def _build_state_payload(thread_id: str, result_dict: Dict[str, Any], fallback_state: Dict[str, Any]) -> Dict[str, Any]:
    market_context = result_dict.get("market_context")
    if not isinstance(market_context, dict):
        market_context = fallback_state.get("market_context")

    candidate = _serialize_candidate(result_dict.get("candidate")) or fallback_state.get("candidate")
    top_vacancies = result_dict.get("top_vacancies")
    if isinstance(market_context, dict) and "top_vacancies" in market_context:
        top_vacancies = market_context.get("top_vacancies") or []
    elif top_vacancies is None:
        top_vacancies = fallback_state.get("top_vacancies")

    skills_gap = result_dict.get("skills_gap")
    if skills_gap is None:
        skills_gap = fallback_state.get("skills_gap")

    interview = result_dict.get("interview")
    if interview is None:
        interview = fallback_state.get("interview")

    return {
        "thread_id": thread_id,
        "market_context": market_context,
        "top_vacancies": top_vacancies,
        "skills_gap": skills_gap,
        "candidate": candidate,
        "interview": interview,
    }


def _is_roadmap_request(message: Optional[str]) -> bool:
    if not message:
        return False
    message_lower = message.lower()
    keywords = [
        "roadmap",
        "роадмап",
        "план развития",
        "план обучения",
        "что учить",
        "что изучать",
        "составь план",
    ]
    return any(keyword in message_lower for keyword in keywords)


def _extract_candidate_skills(current_state: Dict[str, Any]) -> list[str]:
    candidate = current_state.get("candidate") or {}
    if hasattr(candidate, "model_dump"):
        candidate = candidate.model_dump()
    if isinstance(candidate, dict):
        skills = candidate.get("skills", [])
        if isinstance(skills, list):
            return [skill for skill in skills if isinstance(skill, str) and skill.strip()]
    current_skills = current_state.get("current_skills", [])
    if isinstance(current_skills, list):
        return [skill for skill in current_skills if isinstance(skill, str) and skill.strip()]
    return []


def _format_roadmap_response(result: Dict[str, Any]) -> str:
    priorities = result.get("skill_priorities", [])
    if not priorities:
        return "Не удалось построить roadmap по текущим данным. Попробуйте сначала обновить поиск вакансий."

    lines = ["План развития навыков:\n"]
    for item in priorities[:5]:
        skill = item.get("skill", "Навык")
        demand = item.get("market_demand", 0)
        weeks = item.get("estimated_weeks", "?")
        roles = ", ".join(item.get("seen_in_roles", [])[:2])
        salary_impact = item.get("avg_salary_impact")

        line = f"- {skill}: встречается в {demand} вакансиях, срок ~{weeks} нед."
        if salary_impact:
            line += f", средняя зарплата {salary_impact:,} ₽".replace(",", " ")
        if roles:
            line += f" ({roles})"
        lines.append(line)

    salary_range = result.get("expected_salary_range", [])
    if isinstance(salary_range, list) and len(salary_range) == 2 and salary_range[0]:
        lines.append(
            f"\nПрогноз зарплаты: {salary_range[0]:,} → {salary_range[1]:,} ₽".replace(",", " ")
        )

    growth = result.get("growth_explanation")
    if growth:
        lines.append(growth)

    return "\n".join(lines)


@router.on_event("startup")
def startup() -> None:
    global agent
    if agent is not None:
        return

    logger.info("Инициализация нового chat-агента")
    agent = get_agent()

    try:
        fast_llm = get_llm_client("fast")
        smart_llm = get_llm_client("smart") if GROQ_MODEL_SMART != GROQ_MODEL_FAST else None
        set_llm_clients(fast_llm, smart_llm)
    except Exception as exc:
        logger.warning("Не удалось инициализировать interview LLM: %s", exc)


@router.post("/chat")
async def chat(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    state: Optional[str] = Form(None),
    thread_id: Optional[str] = Form(None),
):
    current_state: Dict[str, Any] = {}

    try:
        _ensure_agent()
        if state:
            try:
                current_state = json.loads(state)
            except json.JSONDecodeError:
                logger.warning("Невалидный state JSON в /chat")
                current_state = {}

        if not isinstance(current_state, dict):
            current_state = {}

        thread_id = thread_id or current_state.get("thread_id") or str(uuid.uuid4())

        if thread_id in session_store:
            merged_state = dict(session_store[thread_id])
            merged_state.update({k: v for k, v in current_state.items() if v is not None})
            current_state = merged_state

        if not message and not file:
            return {
                "response": "Отправьте сообщение или загрузите резюме.",
                "thread_id": thread_id,
                "state": current_state,
                "history": current_state.get("history", []),
                "action": current_state.get("action"),
                "stage": current_state.get("stage"),
            }

        candidate_data = None
        candidate_resume = None
        if file:
            file_bytes = await file.read()
            try:
                if file.content_type == "application/pdf":
                    candidate_resume = await parse_pdf(file_bytes)
                    candidate_data = await parse_resume_from_pdf(file_bytes)
                else:
                    candidate_resume = file_bytes.decode("utf-8", errors="ignore")
                    candidate_data = await parse_resume_from_text(candidate_resume)
            except Exception as exc:
                logger.warning("Ошибка парсинга резюме: %s", exc)

        input_state = _build_initial_state(
            message=message,
            candidate_data=candidate_data,
            candidate_resume=candidate_resume,
            thread_id=thread_id,
            location=current_state.get("location"),
        )
        input_state = _restore_state(input_state, current_state)

        existing_interview = input_state.get("interview") or current_state.get("interview")
        if isinstance(existing_interview, dict) and existing_interview.get("active"):
            result = await handle_interview_answer(
                user_answer=message or "",
                interview_state=existing_interview,
            )
            updated_state = dict(current_state)
            updated_state["thread_id"] = thread_id
            updated_state["interview"] = result.get("interview_state")
            session_store[thread_id] = updated_state
            return {
                "response": result["response"],
                "thread_id": thread_id,
                "state": updated_state,
                "interview_state": result.get("interview_state"),
                "history": current_state.get("history", []),
                "action": "interview",
                "stage": "interview",
            }

        if message and should_trigger_interview(message):
            result = await start_interview(message, current_state, input_state)
            updated_state = dict(current_state)
            updated_state["thread_id"] = thread_id
            updated_state["interview"] = result.get("interview_state")
            session_store[thread_id] = updated_state
            return {
                "response": result["response"],
                "thread_id": thread_id,
                "state": updated_state,
                "interview_state": result.get("interview_state"),
                "history": current_state.get("history", []),
                "action": "interview",
                "stage": "interview",
            }

        if message and should_trigger_resume_adaptation(message):
            resume_data = extract_resume_data_from_state(current_state)
            response = await adapt_resume_to_vacancy(
                message=message,
                candidate_resume=resume_data.get("resume_text"),
                vacancy_context=resume_data.get("vacancy_context"),
            )
            current_state["thread_id"] = thread_id
            session_store[thread_id] = current_state
            return {
                "response": response,
                "thread_id": thread_id,
                "state": current_state,
                "history": current_state.get("history", []),
                "action": "resume",
                "stage": "completed",
            }

        if message and _is_roadmap_request(message):
            market_context = current_state.get("market_context") or {}
            top_vacancies = market_context.get("top_vacancies", []) if isinstance(market_context, dict) else []
            if not top_vacancies:
                return {
                    "response": "Сейчас нет сохранённых вакансий для построения roadmap. Сначала выполните поиск вакансий с тем же thread_id, потом повторите запрос.",
                    "thread_id": thread_id,
                    "state": current_state,
                    "history": current_state.get("history", []),
                    "action": "roadmap",
                    "stage": "waiting_market_context",
                }

            candidate_skills = _extract_candidate_skills(current_state)
            candidate = current_state.get("candidate") or {}
            if hasattr(candidate, "model_dump"):
                candidate = candidate.model_dump()
            target_role = candidate.get("specialization") if isinstance(candidate, dict) else None

            roadmap_result = generate_roadmap_func(
                current_skills=candidate_skills,
                market_context=market_context,
                target_role=target_role,
                timeframe_months=6,
            )
            response_message = _format_roadmap_response(roadmap_result)

            state_payload = dict(current_state)
            state_payload["thread_id"] = thread_id
            state_payload["roadmap"] = roadmap_result
            history = list(current_state.get("history", []))
            history.append({"user": message, "assistant": response_message})
            state_payload["history"] = history[-10:]
            session_store[thread_id] = state_payload

            return {
                "response": response_message,
                "thread_id": thread_id,
                "state": state_payload,
                "history": state_payload["history"],
                "action": "roadmap",
                "stage": "completed",
            }

        config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 10}
        result_dict = await agent.ainvoke(input_state, config=config)
        response_message = _extract_assistant_response(result_dict)

        state_payload = _build_state_payload(thread_id, result_dict, current_state)
        history = list(current_state.get("history", []))
        history.append({"user": message, "assistant": response_message})
        state_payload["history"] = history[-10:]
        session_store[thread_id] = state_payload

        return {
            "response": response_message,
            "thread_id": thread_id,
            "state": state_payload,
            "history": state_payload["history"],
            "action": result_dict.get("action"),
            "stage": result_dict.get("stage"),
            "debug_state": {
                "iteration_count": result_dict.get("iteration_count"),
                "skills_gap": result_dict.get("skills_gap"),
                "top_vacancies": len((state_payload.get("market_context") or {}).get("top_vacancies", []))
                if isinstance(state_payload.get("market_context"), dict)
                else 0,
            },
        }
    except Exception as exc:
        logger.exception("Ошибка в /chat: %s", exc)
        return {
            "response": f"⚠️ Ошибка: {str(exc)[:300]}",
            "thread_id": thread_id or "unknown",
            "state": current_state if isinstance(current_state, dict) else {},
            "history": current_state.get("history", []) if isinstance(current_state, dict) else [],
            "action": None,
            "stage": "error",
        }


@router.post("/chat_json")
async def chat_json(request: ChatRequest):
    state = session_store.get(request.thread_id or "", {})
    payload = {
        "message": request.message,
        "thread_id": request.thread_id,
        "state": json.dumps(state, ensure_ascii=False),
    }
    return await chat(
        message=payload["message"],
        state=payload["state"],
        thread_id=payload["thread_id"],
        file=None,
    )
