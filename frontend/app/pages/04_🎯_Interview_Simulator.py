import json
import streamlit as st
import requests
from app.utils.style_loader import apply_custom_styles
apply_custom_styles()

API_URL = "http://localhost:8000/api/v1/chat"

st.set_page_config(page_title="Interview Simulator", page_icon="💬", layout="wide")
st.title("💬 Interview Simulator")

# =========================================
# INIT SESSION STATE
# =========================================
if "chat_state" not in st.session_state:
    st.session_state.chat_state = {}

if "messages" not in st.session_state:
    st.session_state.messages = []

if "show_debug_chat" not in st.session_state:
    st.session_state.show_debug_chat = False


# =========================================
# SIDEBAR / INFO BLOCK
# =========================================
with st.sidebar:
    st.markdown("## 🤖 Что умеет чат")
    st.markdown("""
Чат может помогать в нескольких сценариях:

- **Interview simulator** — подготовка к интервью
- **Roadmap** — построение плана развития
- **Resume help** — советы по улучшению резюме
- **Vacancy search** — помощь с поиском вакансий
- **Career guidance** — карьерные рекомендации
    """)

    st.markdown("## 💡 Примеры запросов")
    st.markdown("""
- Помоги подготовиться к интервью по Python backend
- Построй roadmap для Data Scientist
- Как улучшить моё резюме под ML Engineer?
- Найди подходящие вакансии по моим навыкам
    """)

    st.markdown("## ⚙️ Управление")
    if st.button("🗑 Очистить чат", use_container_width=True):
        st.session_state.chat_state = {}
        st.session_state.messages = []
        st.rerun()

    st.session_state.show_debug_chat = st.checkbox(
        "Показать debug backend response",
        value=st.session_state.show_debug_chat
    )


# =========================================
# HELPER: FALLBACK REPLY
# =========================================
def get_fallback_reply(action: str) -> str:
    """
    Временные ответы, пока backend не начал возвращать response.
    Когда появится data['response'], они будут использоваться только как запасной вариант.
    """
    if action == "interview":
        return "Ок, давай начнём интервью. Первый вопрос: расскажи про свой опыт с FastAPI."
    if action == "roadmap":
        return "Хорошо, давай построим roadmap развития по твоей цели."
    if action == "resume":
        return "Помогу улучшить резюме. Могу подсказать, что стоит усилить и какие навыки подчеркнуть."
    if action == "search":
        return "Начинаю поиск подходящих вакансий и направлений."
    return f"Получено действие: {action}"


# =========================================
# MAIN CHAT CONTAINER
# =========================================
chat_container = st.container()

with chat_container:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])


# =========================================
# CHAT INPUT
# =========================================
user_input = st.chat_input("Напиши сообщение...")

if user_input:
    # 1. показываем сообщение пользователя
    st.session_state.messages.append(
        {
            "role": "user",
            "content": user_input,
        }
    )

    with chat_container:
        with st.chat_message("user"):
            st.markdown(user_input)

    # 2. отправляем в backend
    try:
        response = requests.post(
            API_URL,
            data={
                "message": user_input,
                "state": json.dumps(st.session_state.chat_state),
            },
            timeout=60,
        )

        response.raise_for_status()
        data = response.json()

        # 3. сохраняем новый state
        st.session_state.chat_state = data

        # 4. debug показываем только по чекбоксу
        if st.session_state.show_debug_chat:
            with st.expander("🔧 Debug: raw backend response", expanded=False):
                st.json(data)

        # 5. пробуем взять реальный текст ответа из backend
        # приоритет:
        # response -> assistant_message -> reply -> fallback по action
        action = data.get("action", "unknown")
        reply = (
            data.get("response")
            or data.get("assistant_message")
            or data.get("reply")
            or get_fallback_reply(action)
        )

    except Exception as e:
        reply = f"Ошибка при обращении к backend: {str(e)}"

    # 6. показываем ответ ассистента
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": reply,
        }
    )

    with chat_container:
        with st.chat_message("assistant"):
            st.markdown(reply)