import streamlit as st
from config import API_BASE_URL
from components.vacancy_card import render_vacancy_card
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(page_title="Вакансии", page_icon="💼", layout="wide")

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


def main():
    profile = st.session_state.get("candidate_profile", {})

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

    try:
        with st.spinner("Загрузка вакансий..."):
            selected_seniority = None
            if experience != "Все":
                selected_seniority = experience.lower()
            elif profile.get("grade"):
                selected_seniority = str(profile.get("grade")).lower()

            response = api_client.get_jobs(
                seniority=selected_seniority,
                remote=True if location == "Удалённо" else None,
                limit=20
            )

        if isinstance(response, dict):
            vacancies = response.get("data", [])
        elif isinstance(response, list):
            vacancies = response
        else:
            vacancies = []

        if not isinstance(vacancies, list):
            vacancies = []

    except Exception as e:
        st.error(f"Ошибка загрузки вакансий: {e}")
        vacancies = []

    # Нормализация данных для UI
    normalized_vacancies = []
    for v in vacancies:
        item = dict(v)

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

    # Фильтрация на фронте
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

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего вакансий", len(vacancies))
    with col2:
        st.metric("Совпадения с ИИ", sum(1 for v in vacancies if v.get("final_score", 0) > 0.7))
    with col3:
        avg_salary = 0
        if vacancies:
            salaries = []
            for v in vacancies:
                if v.get("salary_from") and v.get("salary_to"):
                    salaries.append((v["salary_from"] + v["salary_to"]) / 2)

            if salaries:
                avg_salary = sum(salaries) / len(salaries)

        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽")

    st.markdown("### Доступные позиции")

    if vacancies:
        for vacancy in vacancies:
            render_vacancy_card(vacancy)
            st.markdown("---")
    else:
        st.warning("Не найдено вакансий, соответствующих вашим критериям")


if __name__ == "__main__":
    main()