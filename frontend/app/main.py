import streamlit as st
from pathlib import Path

from config import PAGE_CONFIG, API_BASE_URL
from styles import get_custom_css
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(**PAGE_CONFIG)
st.markdown(get_custom_css(), unsafe_allow_html=True)

api_client = APIClient(API_BASE_URL)

BASE_DIR = Path(__file__).parent.parent
ASSETS_DIR = BASE_DIR / "assets"
LOGO_PATH = ASSETS_DIR / "logo.png"


if "backend_state" not in st.session_state:
    st.session_state.backend_state = {}

if "candidate_profile" not in st.session_state:
    st.session_state.candidate_profile = {}


def render_header():
    col1, col2 = st.columns([1, 4])

    with col1:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=88)
        else:
            st.markdown("## 🎯")

    with col2:
        st.title("Career Assistant Pro")
        st.markdown("ИИ-платформа для поиска вакансий, планов развития и подготовки к интервью.")


def render_capabilities():
    st.markdown("### Что умеет платформа")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
- загрузка и разбор резюме
- карьерный чат с памятью
- поиск вакансий по роли, стеку и локации
"""
        )

    with col2:
        st.markdown(
            """
- roadmap по топу вакансий
- roadmap по выбранной вакансии
- мини-интервью и советы по подготовке
"""
        )


def render_actions():
    st.markdown("### Разделы")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📄 Загрузка резюме", use_container_width=True):
            st.switch_page("pages/01_📄_Resume_Upload.py")

    with col2:
        if st.button("💼 Вакансии", use_container_width=True):
            st.switch_page("pages/02_💼_Vacancies.py")

    with col3:
        if st.button("💬 Карьерный чат", use_container_width=True):
            st.switch_page("pages/04_🎯_Chat.py")


def render_status():
    st.markdown("### Статус")
    if api_client.check_health():
        st.success("Backend подключён")
    else:
        st.error("Backend недоступен")

    candidate = st.session_state.get("candidate_profile", {})
    if candidate:
        st.caption(
            f"Профиль загружен: {candidate.get('specialization', 'не определена')} | "
            f"навыков: {len(candidate.get('skills', []))}"
        )

    selected_vacancy = st.session_state.backend_state.get("selected_vacancy")
    if isinstance(selected_vacancy, dict) and selected_vacancy:
        st.caption(
            f"Текущая выбранная вакансия: {selected_vacancy.get('title', 'Без названия')}"
        )


def main():
    render_header()
    st.markdown(
        """
<div class='welcome-banner'>
    <h2>Рабочее пространство карьерного ассистента</h2>
    <p>Здесь можно загрузить резюме, выбрать вакансию, построить план обучения и продолжить разговор в чате.</p>
</div>
""",
        unsafe_allow_html=True,
    )
    render_capabilities()
    render_actions()
    render_status()


if __name__ == "__main__":
    main()
