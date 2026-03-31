import streamlit as st
import pandas as pd
import plotly.express as px

from config import API_BASE_URL
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(page_title="Аналитика рынка", page_icon="📊", layout="wide")

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


def build_df(vacancies: list[dict]) -> pd.DataFrame:
    if not vacancies:
        return pd.DataFrame()

    df = pd.DataFrame(vacancies).copy()

    for col in ["salary_from", "salary_to"]:
        if col not in df.columns:
            df[col] = None

    df["salary_from"] = pd.to_numeric(df["salary_from"], errors="coerce")
    df["salary_to"] = pd.to_numeric(df["salary_to"], errors="coerce")

    df["salary_avg"] = df[["salary_from", "salary_to"]].mean(axis=1)
    df["salary_known"] = df["salary_avg"].notna()

    if "company_name" in df.columns and "company" not in df.columns:
        df["company"] = df["company_name"]

    if "city" not in df.columns:
        df["city"] = "Не указано"

    if "title" not in df.columns:
        df["title"] = "Без названия"

    df["city"] = df["city"].fillna("Не указано")
    df["company"] = df["company"].fillna("Не указано")
    df["title"] = df["title"].fillna("Без названия")

    return df


def safe_top_words(series: pd.Series, n: int = 15) -> pd.DataFrame:
    words: dict[str, int] = {}

    for value in series.dropna():
        for word in str(value).replace("/", " ").replace(",", " ").split():
            w = word.strip().lower()
            if len(w) < 3:
                continue
            words[w] = words.get(w, 0) + 1

    if not words:
        return pd.DataFrame(columns=["word", "count"])

    top = sorted(words.items(), key=lambda x: x[1], reverse=True)[:n]
    return pd.DataFrame(top, columns=["word", "count"])


def main():
    st.markdown("""
        <div class='page-header'>
            <h1>📊 Аналитика рынка</h1>
            <p>Интерактивный dashboard по вакансиям из backend API</p>
        </div>
    """, unsafe_allow_html=True)

    api_client = APIClient(API_BASE_URL)

    st.markdown("### 🔎 Фильтры")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        experience = st.selectbox("Опыт", ["Все", "Junior", "Middle", "Senior", "Lead"])
    with col2:
        remote_only = st.selectbox("Формат", ["Все", "Только удалённо"])
    with col3:
        limit = st.slider("Сколько вакансий загрузить", 20, 300, 100, 20)
    with col4:
        min_salary = st.number_input("Мин. зарплата", min_value=0, value=0, step=50000)

    selected_seniority = None if experience == "Все" else experience.lower()
    selected_remote = True if remote_only == "Только удалённо" else None

    try:
        with st.spinner("Загрузка аналитики..."):
            response = api_client.get_jobs(
                seniority=selected_seniority,
                remote=selected_remote,
                limit=limit
            )

        if isinstance(response, dict):
            vacancies = response.get("data", [])
        elif isinstance(response, list):
            vacancies = response
        else:
            vacancies = []

    except Exception as e:
        st.error(f"Ошибка загрузки аналитики: {e}")
        return

    df = build_df(vacancies)

    if df.empty:
        st.warning("Нет данных для построения аналитики.")
        return

    if min_salary > 0:
        df = df[(df["salary_from"].fillna(0) >= min_salary) | (df["salary_to"].fillna(0) >= min_salary)]

    if df.empty:
        st.warning("После применения фильтров данные отсутствуют.")
        return

    # KPI
    st.markdown("### 📌 Ключевые метрики")

    total_jobs = len(df)
    jobs_with_salary = int(df["salary_known"].sum())
    avg_salary = round(df["salary_avg"].dropna().mean(), 0) if jobs_with_salary else 0
    median_salary = round(df["salary_avg"].dropna().median(), 0) if jobs_with_salary else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Всего вакансий", total_jobs)
    with c2:
        st.metric("С известной зарплатой", jobs_with_salary)
    with c3:
        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽".replace(",", " "))
    with c4:
        st.metric("Медианная зарплата", f"{median_salary:,.0f} ₽".replace(",", " "))

    # 1–2
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        city_counts = df["city"].value_counts().head(10).reset_index()
        city_counts.columns = ["city", "count"]
        fig = px.bar(city_counts, x="city", y="count", title="Топ городов по числу вакансий")
        st.plotly_chart(fig, use_container_width=True)

    with row1_col2:
        company_counts = df["company"].value_counts().head(10).reset_index()
        company_counts.columns = ["company", "count"]
        fig = px.bar(company_counts, x="company", y="count", title="Топ компаний по числу вакансий")
        st.plotly_chart(fig, use_container_width=True)

    # 3–4
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        if jobs_with_salary:
            fig = px.histogram(
                df[df["salary_known"]],
                x="salary_avg",
                nbins=20,
                title="Распределение средних зарплат"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных для распределения зарплат.")

    with row2_col2:
        if jobs_with_salary:
            city_salary = (
                df[df["salary_known"]]
                .groupby("city", as_index=False)["salary_avg"]
                .mean()
                .sort_values("salary_avg", ascending=False)
                .head(10)
            )
            fig = px.bar(city_salary, x="city", y="salary_avg", title="Средняя зарплата по городам")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных для средней зарплаты по городам.")

    # 5–6
    row3_col1, row3_col2 = st.columns(2)

    with row3_col1:
        title_words = safe_top_words(df["title"], n=15)
        if not title_words.empty:
            fig = px.bar(title_words, x="word", y="count", title="Частые слова в названиях вакансий")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Недостаточно данных по названиям.")

    with row3_col2:
        salary_coverage = pd.DataFrame({
            "category": ["С зарплатой", "Без зарплаты"],
            "count": [jobs_with_salary, total_jobs - jobs_with_salary]
        })
        fig = px.pie(salary_coverage, values="count", names="category", hole=0.45, title="Заполненность зарплат")
        st.plotly_chart(fig, use_container_width=True)

    # 7–8
    row4_col1, row4_col2 = st.columns(2)

    with row4_col1:
        if "salary_from" in df.columns and "salary_to" in df.columns:
            salary_compare = df[df["salary_known"]][["title", "salary_from", "salary_to"]].head(20)
            if not salary_compare.empty:
                melted = salary_compare.melt(
                    id_vars="title",
                    value_vars=["salary_from", "salary_to"],
                    var_name="type",
                    value_name="value"
                )
                fig = px.bar(
                    melted,
                    x="title",
                    y="value",
                    color="type",
                    barmode="group",
                    title="Salary from / to по вакансиям"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет данных для сравнения salary_from / salary_to.")

    with row4_col2:
        jobs_per_salary_band = pd.cut(
            df["salary_avg"],
            bins=[0, 100000, 200000, 300000, 500000, 1000000],
            labels=["0-100k", "100-200k", "200-300k", "300-500k", "500k+"]
        ).value_counts().sort_index().reset_index()
        jobs_per_salary_band.columns = ["band", "count"]
        fig = px.bar(jobs_per_salary_band, x="band", y="count", title="Вакансии по зарплатным диапазонам")
        st.plotly_chart(fig, use_container_width=True)

    # 9–10
    row5_col1, row5_col2 = st.columns(2)

    with row5_col1:
        top_salary_jobs = (
            df[df["salary_known"]]
            .sort_values("salary_avg", ascending=False)
            .head(10)[["title", "company", "city", "salary_avg"]]
        )
        if not top_salary_jobs.empty:
            fig = px.bar(
                top_salary_jobs,
                x="title",
                y="salary_avg",
                color="city",
                title="Топ вакансий по средней зарплате"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных по самым высокооплачиваемым вакансиям.")

    with row5_col2:
        city_vs_company = (
            df.groupby(["city", "company"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=False)
            .head(20)
        )
        fig = px.scatter(
            city_vs_company,
            x="city",
            y="company",
            size="count",
            title="Города × компании"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Таблицы
    st.markdown("### 🧾 Таблица вакансий")
    st.dataframe(
        df[["job_id", "title", "company", "city", "salary_from", "salary_to", "salary_avg"]].copy(),
        use_container_width=True
    )

    if jobs_with_salary:
        st.markdown("### 📈 Описательная статистика зарплат")
        stats_df = df["salary_avg"].dropna().describe().round(2).to_frame(name="salary_avg")
        st.dataframe(stats_df, use_container_width=True)


if __name__ == "__main__":
    main()