import math
import uuid
from datetime import date, datetime

import pandas as pd
import streamlit as st

from config import API_BASE_URL
from utils.api_client import APIClient
from app.utils.style_loader import apply_custom_styles

st.set_page_config(page_title="Карьерный чат", page_icon="💬", layout="wide")
st.title("💬 Карьерный чат")
apply_custom_styles()

api_client = APIClient(API_BASE_URL)


if "backend_state" not in st.session_state:
    st.session_state.backend_state = {}

if "messages" not in st.session_state:
    st.session_state.messages = []

if "show_debug_chat" not in st.session_state:
    st.session_state.show_debug_chat = False

if "chat_messages_seed" not in st.session_state:
    st.session_state.chat_messages_seed = None

if "chat_thread_id" not in st.session_state:
    st.session_state.chat_thread_id = str(uuid.uuid4())


def reset_chat():
    st.session_state.messages = []
    st.session_state.chat_messages_seed = None
    st.session_state.chat_thread_id = str(uuid.uuid4())
    st.session_state.backend_state = {
        k: v
        for k, v in st.session_state.backend_state.items()
        if k in {"candidate", "selected_vacancy"}
    }
    st.rerun()


def make_json_safe(value):
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [make_json_safe(v) for v in value]

    if isinstance(value, tuple):
        return [make_json_safe(v) for v in value]

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def send_to_backend(message: str = "", uploaded_file=None):
    st.session_state.backend_state = make_json_safe(st.session_state.backend_state)
    response = api_client.chat(
        message=message,
        uploaded_file=uploaded_file,
        state=st.session_state.backend_state,
        thread_id=st.session_state.chat_thread_id,
    )
    return response


def render_response_content(response_data):
    if isinstance(response_data, str):
        st.markdown(response_data)
    elif isinstance(response_data, list):
        for idx, item in enumerate(response_data, start=1):
            st.markdown(f"**{idx}.** {item}")
    elif isinstance(response_data, dict):
        st.json(response_data)
    else:
        st.write(response_data)


st.markdown("### Что умеет чат")
st.markdown(
    """
- искать вакансии по роли и локации
- строить roadmap по топу вакансий или по выбранной вакансии
- адаптировать резюме под найденную вакансию
- запускать мини-интервью
"""
)

meta_col1, meta_col2 = st.columns([2, 1])
with meta_col1:
    st.caption(f"Thread ID: `{st.session_state.chat_thread_id}`")
with meta_col2:
    if st.button("🗑 Очистить чат", use_container_width=True):
        reset_chat()

st.session_state.show_debug_chat = st.checkbox(
    "Показать debug backend response",
    value=st.session_state.show_debug_chat,
)

candidate_profile = st.session_state.get("candidate_profile", {})
selected_vacancy = st.session_state.backend_state.get("selected_vacancy") or {}

if candidate_profile:
    st.success(
        f"Профиль: {candidate_profile.get('specialization', 'не определена')} | "
        f"грейд: {candidate_profile.get('grade', '-')}"
    )
else:
    st.info("Профиль кандидата пока не загружен. Можно искать вакансии и без резюме.")

if isinstance(selected_vacancy, dict) and selected_vacancy:
    st.caption(
        f"Выбрана вакансия: {selected_vacancy.get('title', 'Без названия')} "
        f"@ {selected_vacancy.get('company', 'Компания не указана')}"
    )

if st.session_state.chat_messages_seed and not st.session_state.messages:
    st.session_state.messages = st.session_state.chat_messages_seed
    st.session_state.chat_messages_seed = None

with st.container():
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            render_response_content(msg["content"])

user_input = st.chat_input("Напишите сообщение...")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})

    try:
        data = send_to_backend(message=user_input)
        st.session_state.chat_thread_id = data.get("thread_id", st.session_state.chat_thread_id)
        st.session_state.backend_state = data.get("state", {}) or {}

        if st.session_state.show_debug_chat:
            with st.expander("Debug backend response", expanded=False):
                st.json(data)

        reply = data.get("response", "Нет ответа от backend")
    except Exception as exc:
        reply = f"Ошибка при обращении к backend: {exc}"

    st.session_state.messages.append({"role": "assistant", "content": reply})
    st.rerun()
