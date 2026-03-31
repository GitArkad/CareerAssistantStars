import streamlit as st
from config import API_BASE_URL
from components.vacancy_card import render_vacancy_card
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(page_title="Вакансии", page_icon="💼", layout="wide")

if "backend_state" not in st.session_state:
    st.session_state.backend_state = {}

if "vacancies_result" not in st.session_state:
    st.session_state.vacancies_result = None

if "chat_messages_seed" not in st.session_state:
    st.session_state.chat_messages_seed = None

st.markdown("""
<style>
input {
    color: black !important;
}
input::placeholder {
    color: #888 !important;
}
label {
    color: black !important;
}
</style>
""", unsafe_allow_html=True)


def run_vacancy_search(api_client: APIClient, profile: dict, search_query: str):
    if search_query and search_query.strip():
        message = f"search {search_query.strip()}"
    elif profile:
        specialization = profile.get("specialization", "")
        skills = profile.get("skills", [])
        skills_text = ", ".join(skills[:8]) if skills else ""
        message = f"search {specialization} {skills_text}".strip()
    else:
        message = "search python developer"

    result = api_client.chat(
        message=message,
        state=st.session_state.backend_state,
    )

    st.session_state.backend_state = result.get("state", {})
    st.session_state.vacancies_result = result
    return result


def main():
    profile = st.session_state.get("candidate_profile", {})
    if profile:
        st.session_state.backend_state["candidate"] = profile
    st.markdown("""
        <div class='page-header'>
            <h1>💼 Вакансии</h1>
            <p>Найдите вакансии, подобранные ИИ на основе вашего профиля</p>
        </div>
    """, unsafe_allow_html=True)

    if not profile:
        st.info("Сначала загрузите резюме на странице 01, чтобы получать более релевантные вакансии.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        location = st.selectbox("Локация", ["Все", "Удалённо", "Москва", "Санкт-Петербург", "Другое"])
    with col2:
        experience = st.selectbox("Опыт", ["Все", "Junior", "Middle", "Senior", "Lead"])
    with col3:
        salary_min = st.number_input("Мин. зарплата", min_value=0, value=100000, step=50000)
    with col4:
        job_type = st.selectbox("Тип занятости", ["Все", "Полная", "Частичная", "Проектная"])

    search_query = st.text_input("🔍 Поиск вакансий", placeholder="Должность, ключевые слова...")

    api_client = APIClient(API_BASE_URL)

    search_clicked = st.button("🔍 Найти вакансии", use_container_width=True)
    vacancies = []

    try:
        if search_clicked or st.session_state.vacancies_result is None:
            with st.spinner("Загрузка вакансий..."):
                result = run_vacancy_search(api_client, profile, search_query)

                response = result.get("response", [])
                vacancies = response if isinstance(response, list) else []
        else:
            cached_result = st.session_state.vacancies_result
            response = cached_result.get("response", [])
            vacancies = response if isinstance(response, list) else []

    except Exception as e:
        st.error(f"Ошибка загрузки вакансий: {e}")
        vacancies = []

    normalized_vacancies = []
    for backend_idx, v in enumerate(vacancies, start=1):
        item = dict(v)
        item["backend_index"] = backend_idx

        if "company" not in item:
            item["company"] = item.get("company_name", "Не указано")

        salary_from = item.get("salary_from")
        salary_to = item.get("salary_to")

        if salary_from and salary_to:
            item["salary"] = f"{salary_from:,.0f} - {salary_to:,.0f} ₽".replace(",", " ")
        elif salary_from:
            item["salary"] = f"от {salary_from:,.0f} ₽".replace(",", " ")
        elif salary_to:
            item["salary"] = f"до {salary_to:,.0f} ₽".replace(",", " ")
        else:
            item["salary"] = "Не указана"

        normalized_vacancies.append(item)

    vacancies = normalized_vacancies

    if search_query:
        q = search_query.lower().strip()
        vacancies = [
            v for v in vacancies
            if q in str(v.get("title", "")).lower()
            or q in str(v.get("company", "")).lower()
        ]

    if salary_min:
        vacancies = [
            v for v in vacancies
            if (v.get("salary_from") or 0) >= salary_min
            or (v.get("salary_to") or 0) >= salary_min
        ]

    vacancies = sorted(
        vacancies,
        key=lambda x: x.get("salary_from") or 0,
        reverse=True
    )

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    with metric_col1:
        st.metric("Всего вакансий", len(vacancies))
    with metric_col2:
        st.metric("Совпадения с ИИ", sum(1 for v in vacancies if v.get("final_score", 0) > 0.7))
    with metric_col3:
        avg_salary = 0
        if vacancies:
            salaries = []
            for v in vacancies:
                if v.get("salary_from") and v.get("salary_to"):
                    salaries.append((v["salary_from"] + v["salary_to"]) / 2)
            if salaries:
                avg_salary = sum(salaries) / len(salaries)

        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽")

    top_col1, top_col2 = st.columns(2)
    with top_col1:
        if st.button("🤖 Перейти в карьерный чат", use_container_width=True):
            st.switch_page("pages/04_🎯_Interview_Simulator.py")
    with top_col2:
        if st.button("📄 Перейти к резюме", use_container_width=True):
            st.switch_page("pages/01_📄_Resume_Upload.py")

    if vacancies:
        st.markdown("### Доступные позиции")

        for idx, vacancy in enumerate(vacancies, start=1):
            render_vacancy_card(vacancy)

            action_col1, action_col2, action_col3, action_col4 = st.columns(4)

            with action_col1:
                if st.button(f"Выбрать #{idx}", key=f"choose_vacancy_{idx}"):
                    try:
                        st.session_state.backend_state["selected_vacancy"] = vacancy

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"Я выбрал вакансию #{idx}"},
                            {
                                "role": "assistant",
                                "content": f"Выбрана вакансия: {vacancy.get('title', 'Без названия')}. Теперь можно запросить fit analysis, roadmap или interview."
                            },
                        ]

                        st.switch_page("pages/04_🎯_Interview_Simulator.py")

                    except Exception as e:
                        st.error(f"Ошибка выбора вакансии: {e}")

            with action_col2:
                if st.button(f"Roadmap #{idx}", key=f"roadmap_vacancy_{idx}"):
                    try:
                        selected = api_client.chat(
                            message=str(vacancy["backend_index"]),
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = selected.get("state", {})

                        result = api_client.chat(
                            message="roadmap",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", {})

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"roadmap по вакансии #{idx}"},
                            {"role": "assistant", "content": result.get("response", "Нет ответа от backend")},
                        ]
                        st.switch_page("pages/04_🎯_Interview_Simulator.py")
                    except Exception as e:
                        st.error(f"Ошибка roadmap: {e}")

            with action_col3:
                if st.button(f"Interview #{idx}", key=f"interview_vacancy_{idx}"):
                    try:
                        selected = api_client.chat(
                            message=str(vacancy["backend_index"]),
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = selected.get("state", {})

                        result = api_client.chat(
                            message="interview",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", {})

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"interview по вакансии #{idx}"},
                            {"role": "assistant", "content": result.get("response", "Нет ответа от backend")},
                        ]
                        st.switch_page("pages/04_🎯_Interview_Simulator.py")
                    except Exception as e:
                        st.error(f"Ошибка interview: {e}")
                        
            with action_col4:
                if st.button(f"Fit #{idx}", key=f"fit_vacancy_{idx}"):
                    try:
                        # 1. Кладём выбранную вакансию напрямую в state
                        st.session_state.backend_state["selected_vacancy"] = vacancy

                        # 2. Убеждаемся, что candidate тоже есть в state
                        if profile:
                            st.session_state.backend_state["candidate"] = profile

                        # 3. Просим backend сделать fit analysis
                        result = api_client.chat(
                            message="fit analysis",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", {})

                        # 4. Передаём результат в чат
                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"fit analysis по вакансии #{idx}"},
                            {"role": "assistant", "content": result.get("response", "Нет ответа от backend")},
                        ]

                        st.switch_page("pages/04_🎯_Interview_Simulator.py")

                    except Exception as e:
                        st.error(f"Ошибка fit analysis: {e}")

            st.markdown("---")
    else:
        st.warning("Не найдено вакансий, соответствующих вашим критериям")


if __name__ == "__main__":
    main()