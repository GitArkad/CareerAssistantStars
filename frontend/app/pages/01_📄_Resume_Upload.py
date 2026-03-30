import streamlit as st
from streamlit_pdf_viewer import pdf_viewer
import tempfile
import os
from pathlib import Path

from components.header import render_page_header
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

# Применяем стили
apply_custom_styles()
st.set_page_config(page_title="Загрузка резюме", page_icon="📄", layout="wide")


def main():
    # ✅ ИНИЦИАЛИЗАЦИЯ STATE
    if "candidate_profile" not in st.session_state:
        st.session_state.candidate_profile = {}

    if "show_analysis" not in st.session_state:
        st.session_state.show_analysis = False

    render_page_header(
        "📄 Загрузка и анализ резюме",
        "Загрузите и проанализируйте своё резюме с помощью ИИ"
    )

    api_client = APIClient(
        st.secrets.get("API_BASE_URL", "http://localhost:8000")
    )

    # =========================
    # ЗАГРУЗКА
    # =========================
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### Загрузите ваше резюме")

        uploaded_file = st.file_uploader(
            "Выберите файл",
            type=['pdf', 'doc', 'docx', 'txt'],
        )

        if uploaded_file is not None:
            st.success(f"✅ Файл загружен: {uploaded_file.name}")
            st.info(f"Размер: {uploaded_file.size / 1024:.2f} КБ")

            # временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name

            # PDF preview
            if uploaded_file.type == "application/pdf":
                st.markdown("### Предпросмотр")
                try:
                    pdf_viewer(tmp_path)
                except:
                    st.warning("Предпросмотр недоступен")

            # КНОПКА АНАЛИЗА
            if st.button("🔍 Проанализировать резюме", use_container_width=True):
                with st.spinner("🤖 ИИ анализирует резюме..."):
                    try:
                        result = api_client.upload_resume(uploaded_file)

                        profile = result.get("profile", {})

                        # ✅ сохраняем
                        st.session_state.candidate_profile = profile
                        st.session_state.show_analysis = True

                        st.success("✅ Резюме обработано!")

                    except Exception as e:
                        st.error(f"Ошибка: {e}")

                    finally:
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)

    # =========================
    # РЕЗУЛЬТАТЫ
    # =========================
    with col2:
        st.markdown("### Результаты анализа")

        profile = st.session_state.candidate_profile

        if st.session_state.show_analysis and profile:

            st.markdown("### 👤 Профиль кандидата")

            st.write(f"**Имя:** {profile.get('name', '-')}")
            st.write(f"**Страна:** {profile.get('country', '-')}")
            st.write(f"**Город:** {profile.get('city', '-')}")
            st.write(f"**Грейд:** {profile.get('grade', '-')}")
            st.write(f"**Специализация:** {profile.get('specialization', '-')}")
            st.write(f"**Опыт:** {profile.get('experience_years', '-')} лет")
            st.write(f"**Зарплата:** {profile.get('desired_salary', '-')}")
            st.write(f"**Релокация:** {'Да' if profile.get('relocation') else 'Нет'}")

            st.markdown("### 🏢 Формат работы")
            for item in profile.get("work_format", []):
                st.markdown(f"- {item}")

            st.markdown("### 🌍 Языки")
            for lang in profile.get("foreign_languages", []):
                st.markdown(f"- {lang}")

            st.markdown("### 💡 Навыки")
            skills = profile.get("skills", [])
            if skills:
                for skill in skills:
                    st.markdown(f"- {skill}")
            else:
                st.info("Навыки не найдены")

        else:
            st.info("Загрузите резюме, чтобы увидеть анализ")


if __name__ == "__main__":
    main()