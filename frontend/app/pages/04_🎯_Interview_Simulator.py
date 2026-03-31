import json
import streamlit as st
import requests
from app.utils.style_loader import apply_custom_styles

apply_custom_styles()

API_URL = "http://localhost:8000/api/v1/chat"

st.set_page_config(page_title="Interview Simulator", page_icon="💬", layout="wide")
st.title("💬 Interview Simulator")

if "backend_state" not in st.session_state:
    st.session_state.backend_state = {}

if "messages" not in st.session_state:
    st.session_state.messages = []

if "show_debug_chat" not in st.session_state:
    st.session_state.show_debug_chat = False

if "chat_messages_seed" not in st.session_state:
    st.session_state.chat_messages_seed = None


def reset_chat():
    st.session_state.messages = []
    st.session_state.chat_messages_seed = None
    # НЕ трогаем backend_state!
    st.rerun()


def send_to_backend(message: str = "", uploaded_file=None):
    data = {
        "message": message,
        "state": json.dumps(st.session_state.backend_state, ensure_ascii=False),
    }

    files = None
    if uploaded_file is not None:
        files = {
            "file": (
                uploaded_file.name,
                uploaded_file.getvalue(),
                uploaded_file.type or "application/octet-stream",
            )
        }

    response = requests.post(
        API_URL,
        data=data,
        files=files,
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def render_response_content(response_data):
    if isinstance(response_data, str):
        st.markdown(response_data)

    elif isinstance(response_data, list):
    # список вакансий
        if response_data and isinstance(response_data[0], dict):
            st.markdown("### Найденные вакансии")
            for idx, item in enumerate(response_data, start=1):
                st.markdown(f"**{idx}. {item.get('title', 'Без названия')}**")
                st.write(f"Компания: {item.get('company', '—')}")
                st.write(f"Город: {item.get('city', '—')}")
                st.write(f"Зарплата от: {item.get('salary_from', '—')}")
                if item.get("url"):
                    st.write(f"Ссылка: {item.get('url')}")
                st.divider()
        else:
            # список строк / список простых значений
            for idx, item in enumerate(response_data, start=1):
                st.markdown(f"**{idx}.** {str(item)}")

    elif isinstance(response_data, dict):
        if response_data.get("message"):
            st.markdown(f"**{response_data['message']}**")

        if "vacancies" in response_data:
            st.markdown("### Выберите вакансию")

            for vacancy in response_data["vacancies"]:
                col1, col2 = st.columns([6, 1])

                with col1:
                    st.write(f"{vacancy['id']}. {vacancy['title']}")

                with col2:
                    if st.button("Выбрать", key=f"vacancy_select_{vacancy['id']}"):
                        try:
                            result = send_to_backend(
                                message=str(vacancy["id"]),
                                uploaded_file=None,
                            )

                            st.session_state.backend_state = result.get("state", {})
                            st.session_state.messages.append(
                                {"role": "user", "content": str(vacancy["id"])}
                            )

                            assistant_content = result.get("response", "Нет ответа от backend")
                            st.session_state.messages.append(
                                {"role": "assistant", "content": assistant_content}
                            )

                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка выбора вакансии: {e}")
        else:
            st.json(response_data)

    else:
        st.write(response_data)


st.markdown("### 🤖 Что умеет чат")
st.markdown("""
Чат может помогать в нескольких сценариях:

- **Interview simulator** — подготовка к интервью
- **Roadmap** — построение плана развития
- **Resume help** — советы по улучшению резюме
- **Vacancy search** — помощь с поиском вакансий
- **Career guidance** — карьерные рекомендации
""")

info_col1, info_col2 = st.columns([2, 1])

with info_col1:
    st.markdown("#### 💡 Примеры запросов")
    st.markdown("""
- Помоги подготовиться к интервью по Python backend  
- Построй roadmap для Data Scientist  
- Проанализируй мое резюме  
- Найди подходящие вакансии по моим навыкам  
""")

with info_col2:
    st.markdown("#### ⚙️ Управление")

    if st.button("🗑 Очистить чат", use_container_width=True):
        reset_chat()

    st.session_state.show_debug_chat = st.checkbox(
        "Показать debug backend response",
        value=st.session_state.show_debug_chat
    )

st.info("Для анализа резюме сначала загрузите PDF на странице «📄 Загрузка резюме», затем возвращайтесь сюда для интервью, roadmap и карьерного чата.")
st.divider()
candidate_profile = st.session_state.get("candidate_profile", {})

if candidate_profile:
    st.success(
        f"Профиль загружен: {candidate_profile.get('specialization', 'Специализация не определена')}"
    )
    st.caption(
        f"Грейд: {candidate_profile.get('grade', '-')} | "
        f"Навыков: {len(candidate_profile.get('skills', []))}"
    )
else:
    st.warning("Профиль кандидата пока не загружен.")
    
if st.session_state.chat_messages_seed:
    st.session_state.messages = st.session_state.chat_messages_seed
    st.session_state.chat_messages_seed = None

chat_container = st.container()

with chat_container:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            content = msg["content"]

            if isinstance(content, str):
                st.markdown(content)
            elif isinstance(content, list):
                render_response_content(content)
            elif isinstance(content, dict):
                render_response_content(content)
            else:
                st.write(content)

user_input = st.chat_input("Напиши сообщение...")

if user_input:
    st.session_state.messages.append(
        {
            "role": "user",
            "content": user_input,
        }
    )

    try:
        data = send_to_backend(
            message=user_input,
            uploaded_file=None,
        )

        st.session_state.backend_state = data.get("state", {})

        if st.session_state.show_debug_chat:
            with st.expander("🔧 Debug: raw backend response", expanded=False):
                st.json(data)

        reply = data.get("response", "Нет ответа от backend")

    except Exception as e:
        reply = f"Ошибка при обращении к backend: {str(e)}"

    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": reply,
        }
    )

    st.rerun()