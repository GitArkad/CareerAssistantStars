import streamlit as st
from streamlit_pdf_viewer import pdf_viewer
import tempfile
import os
from pathlib import Path
from typing import Any, Dict

from components.header import render_page_header
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(page_title="Загрузка резюме", page_icon="📄", layout="wide")


def _get_backend_candidate() -> Dict[str, Any]:
    backend_state = st.session_state.get("backend_state", {}) or {}
    candidate = backend_state.get("candidate")
    return candidate if isinstance(candidate, dict) else {}


def _render_candidate_analysis(profile: Dict[str, Any], source_label: str):
    desired_salary = profile.get("desired_salary")
    desired_salary_display = desired_salary if desired_salary not in (None, 0, "", "0") else "Не указано"

    st.caption(source_label)
    st.markdown("### 👤 Профиль кандидата")

    st.write(f"**Имя:** {profile.get('name', '-')}")
    st.write(f"**Страна:** {profile.get('country', '-')}")
    st.write(f"**Город:** {profile.get('city', '-')}")
    st.write(f"**Грейд:** {profile.get('grade', '-')}")
    st.write(f"**Специализация:** {profile.get('specialization', '-')}")
    st.write(f"**Опыт:** {profile.get('experience_years', '-')} лет")
    st.write(f"**Желаемая зарплата:** {desired_salary_display}")
    st.write(f"**Релокация:** {'Да' if profile.get('relocation') else 'Нет'}")


    st.markdown("### 🏢 Формат работы")
    work_format = profile.get("work_format", [])
    if work_format:
        for item in work_format:
            st.markdown(f"- {item}")
    else:
        st.markdown("Не указан")


    st.markdown("### 💡 Навыки")
    skills = profile.get("skills", [])
    if skills:
        for skill in skills:
            st.markdown(f"- {skill}")
    else:
        st.info("Навыки не найдены")
    
    st.markdown("### 🌍 Языки")
    langs = profile.get("foreign_languages", [])
    if langs:
        for lang in langs:
            st.markdown(f"- {lang}")
    else:
        st.info("Не указаны")


def main():
    if "candidate_profile" not in st.session_state:
        st.session_state.candidate_profile = {}

    if "backend_state" not in st.session_state:
        st.session_state.backend_state = {}

    if "show_analysis" not in st.session_state:
        st.session_state.show_analysis = False

    render_page_header(
        "📄 Загрузка и анализ резюме",
        "Загрузите и проанализируйте своё резюме с помощью ИИ"
    )

    api_client = APIClient(
        st.secrets.get("API_BASE_URL", "http://localhost:8000")
    )

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### Загрузите ваше резюме")

        uploaded_file = st.file_uploader(
            "Выберите файл",
            type=["pdf", "doc", "docx", "txt"],
        )

        tmp_path = None

        if uploaded_file is not None:
            st.success(f"✅ Файл загружен: {uploaded_file.name}")
            st.info(f"Размер: {uploaded_file.size / 1024:.2f} КБ")

            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name

            if uploaded_file.type == "application/pdf":
                st.markdown("### Предпросмотр")
                try:
                    pdf_viewer(tmp_path)
                except Exception:
                    st.warning("Предпросмотр недоступен")

            if st.button("🔍 Проанализировать резюме", use_container_width=True):
                with st.spinner("🤖 ИИ анализирует резюме..."):
                    try:
                        result = api_client.upload_resume(uploaded_file)

                        profile = result.get("profile", {})
                        state = result.get("state", {})

                        st.session_state.backend_state = state or {}
                        st.session_state.candidate_profile = profile

                        # важно: backend fit использует state["candidate"]
                        st.session_state.backend_state["candidate"] = profile
                        st.session_state.show_analysis = True
                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": "Я загрузил резюме"},
                            {
                                "role": "assistant",
                                "content": (
                                    f"Резюме загружено. "
                                    f"Специализация: {profile.get('specialization', '-')}. "
                                    f"Грейд: {profile.get('grade', '-')}. "
                                    f"Навыков найдено: {len(profile.get('skills', []))}. "
                                    f"Теперь можно перейти к вакансиям или продолжить в карьерном чате."
                                ),
                            },
                        ]

                        st.success("✅ Резюме обработано!")
                        st.rerun()

                    except Exception as e:
                        st.error(f"Ошибка: {e}")

        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

        if st.session_state.show_analysis and st.session_state.candidate_profile:
            st.markdown("### 🚀 Что дальше?")

            nav_col1, nav_col2 = st.columns(2)

            with nav_col1:
                if st.button("💼 Перейти к вакансиям", use_container_width=True, key="go_to_vacancies"):
                    st.session_state.show_analysis = False
                    st.switch_page("pages/02_💼_Vacancies.py")

            with nav_col2:
                if st.button("🤖 Открыть карьерный чат", use_container_width=True, key="go_to_chat"):
                    st.session_state.show_analysis = False
                    st.switch_page("pages/04_🎯_Chat.py")

    with col2:
        st.markdown("### Результаты анализа")

        backend_profile = _get_backend_candidate()
        profile = backend_profile or st.session_state.candidate_profile

        if profile:
            source_label = "Данные загружены" if backend_profile else "Данные получены после анализа резюме"
            _render_candidate_analysis(profile, source_label)

        else:
            st.info("Загрузите резюме, чтобы справа появился профиль кандидата из backend.")


if __name__ == "__main__":
    main()
