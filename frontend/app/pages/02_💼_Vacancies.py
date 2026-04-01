import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

from config import API_BASE_URL
from components.vacancy_card import render_vacancy_card
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles
from datetime import date, datetime
import math

apply_custom_styles()
st.set_page_config(page_title="Вакансии", page_icon="💼", layout="wide")

st.markdown("""
<style>
input, textarea, .stTextInput input, .stNumberInput input {
    color: black !important;
}
input::placeholder, textarea::placeholder {
    color: #f7f5f5 !important;
}
label {
    color: white !important;
}
/* универсально для всех input */
input {
    background-color: white !important;
    color: black !important;
}

/* контейнеры streamlit */
div[data-baseweb="input"] {
    background-color: white !important;
}
</style>
""", unsafe_allow_html=True)

# =========================
# SESSION STATE
# =========================
if "backend_state" not in st.session_state:
    st.session_state.backend_state = {}

if "vacancies_df" not in st.session_state:
    st.session_state.vacancies_df = None

if "chat_messages_seed" not in st.session_state:
    st.session_state.chat_messages_seed = None

if "selected_vacancy_id" not in st.session_state:
    st.session_state.selected_vacancy_id = None

# =========================
# DB CONFIG
# =========================
DEFAULT_HOST = os.getenv("POSTGRES_HOST", "16.54.110.212")
DEFAULT_PORT = int(os.getenv("POSTGRES_PORT", 5433))
DEFAULT_DB = os.getenv("POSTGRES_DB", "ai_career")
DEFAULT_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
DEFAULT_TABLE = os.getenv("POSTGRES_VACANCIES_TABLE", "jobs_curated")
DEFAULT_USER = os.getenv("POSTGRES_USER", "postgres")
DEFAULT_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")


def get_db_url(host: str, port: int, database: str, user: str, password: str) -> str:
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


@st.cache_resource(show_spinner=False)
def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)


@st.cache_data(ttl=60, show_spinner=False)
def load_vacancies_from_db(
    db_url: str,
    schema: str,
    table_name: str,
    limit: int,
) -> pd.DataFrame:
    query = text(f'SELECT * FROM "{schema}"."{table_name}" LIMIT :limit')
    engine = get_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"limit": limit})


def normalize_vacancies_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if "company" not in df.columns and "company_name" in df.columns:
        df["company"] = df["company_name"]

    if "company" not in df.columns:
        df["company"] = "Не указано"

    if "title" not in df.columns:
        df["title"] = "Без названия"

    if "city" not in df.columns:
        df["city"] = "Не указано"

    if "job_id" not in df.columns:
        if "id" in df.columns:
            df["job_id"] = df["id"]
        else:
            df["job_id"] = range(1, len(df) + 1)

    if "salary_from" not in df.columns:
        df["salary_from"] = None
    if "salary_to" not in df.columns:
        df["salary_to"] = None

    df["salary_from"] = pd.to_numeric(df["salary_from"], errors="coerce")
    df["salary_to"] = pd.to_numeric(df["salary_to"], errors="coerce")

    df["salary_avg"] = df[["salary_from", "salary_to"]].mean(axis=1)

    df["title"] = df["title"].fillna("Без названия")
    df["company"] = df["company"].fillna("Не указано")
    df["city"] = df["city"].fillna("Не указано")

    return df


def build_salary_text(row: pd.Series) -> str:
    salary_from = row.get("salary_from")
    salary_to = row.get("salary_to")

    if pd.notna(salary_from) and pd.notna(salary_to):
        return f"{salary_from:,.0f} - {salary_to:,.0f} ₽".replace(",", " ")
    if pd.notna(salary_from):
        return f"от {salary_from:,.0f} ₽".replace(",", " ")
    if pd.notna(salary_to):
        return f"до {salary_to:,.0f} ₽".replace(",", " ")
    return "Не указана"


def apply_filters(
    df: pd.DataFrame,
    search_query: str,
    location: str,
    experience: str,
    salary_min: int,
    job_type: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()

    if search_query.strip():
        q = search_query.lower().strip()
        searchable_cols = ["title", "company", "city"]
        if "description" in filtered.columns:
            searchable_cols.append("description")

        mask = False
        for col in searchable_cols:
            mask = mask | filtered[col].astype(str).str.lower().str.contains(q, na=False)
        filtered = filtered[mask]

    if location != "Все":
        if location == "Удалённо":
            if "remote" in filtered.columns:
                filtered = filtered[filtered["remote"].astype(str).str.lower().isin(["true", "1", "yes", "да"])]
            else:
                filtered = filtered[
                    filtered["title"].astype(str).str.lower().str.contains("удал", na=False)
                    | filtered["city"].astype(str).str.lower().str.contains("удал", na=False)
                ]
        elif location == "Другое":
            filtered = filtered[~filtered["city"].isin(["Москва", "Санкт-Петербург"])]
        else:
            filtered = filtered[filtered["city"].astype(str) == location]

    if experience != "Все":
        exp_lower = experience.lower()
        exp_cols = [c for c in ["experience", "seniority", "level"] if c in filtered.columns]
        if exp_cols:
            mask = False
            for col in exp_cols:
                mask = mask | filtered[col].astype(str).str.lower().str.contains(exp_lower, na=False)
            filtered = filtered[mask]

    if salary_min > 0:
        filtered = filtered[
            (filtered["salary_from"].fillna(0) >= salary_min)
            | (filtered["salary_to"].fillna(0) >= salary_min)
        ]

    if job_type != "Все":
        job_type_map = {
            "Полная": "полная",
            "Частичная": "частичная",
            "Проектная": "проект"
        }
        job_type_value = job_type_map.get(job_type, job_type.lower())

        if "employment_type" in filtered.columns:
            filtered = filtered[
                filtered["employment_type"].astype(str).str.lower().str.contains(job_type_value, na=False)
            ]
        elif "schedule" in filtered.columns:
            filtered = filtered[
                filtered["schedule"].astype(str).str.lower().str.contains(job_type_value, na=False)
            ]

    filtered = filtered.sort_values(
        by="salary_from",
        ascending=False,
        na_position="last"
    )

    return filtered


def dataframe_to_cards(df: pd.DataFrame) -> list[dict]:
    vacancies = []
    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        item = row.to_dict()
        item["backend_index"] = idx
        item["salary"] = build_salary_text(row)
        vacancies.append(item)
    return vacancies

def make_json_safe(value):
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [make_json_safe(v) for v in value]

    if isinstance(value, tuple):
        return [make_json_safe(v) for v in value]

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    if isinstance(value, float) and math.isnan(value):
        return None

    return value

def push_selected_vacancy_to_state(vacancy: dict, profile: dict | None = None):
    safe_vacancy = make_json_safe(vacancy)

    st.session_state.backend_state["selected_vacancy"] = safe_vacancy
    st.session_state.selected_vacancy_id = safe_vacancy.get("job_id")

    if profile:
        st.session_state.backend_state["candidate"] = make_json_safe(profile)


def go_to_chat_with_selected_vacancy(vacancy: dict):
    title = vacancy.get("title", "Без названия")
    st.session_state.chat_messages_seed = [
        {"role": "user", "content": f"Я выбрал вакансию: {title}"},
        {
            "role": "assistant",
            "content": f"Выбрана вакансия: {title}. Теперь можно запросить fit analysis, roadmap или interview."
        },
    ]
    st.switch_page("pages/04_🎯_Interview_Simulator.py")

def go_to_chat_with_fit_seed(vacancy: dict, profile: dict | None = None):
    push_selected_vacancy_to_state(vacancy, profile)

    title = vacancy.get("title", "Без названия")
    st.session_state.chat_messages_seed = [
        {"role": "user", "content": f"Сделай анализ соответствия по вакансии: {title}"},
        {
            "role": "assistant",
            "content": (
                f"Выбрана вакансия: {title}. "
                f"Я готов сделать fit analysis, разобрать gaps, roadmap и interview preparation."
            ),
        },
    ]

    st.switch_page("pages/04_🎯_Interview_Simulator.py")

def main():
    profile = st.session_state.get("candidate_profile", {})
    if profile:
        st.session_state.backend_state["candidate"] = profile

    st.markdown("""
        <div class='page-header'>
            <h1>💼 Вакансии</h1>
            <p>Список вакансий из PostgreSQL + переход в чат с выбранной вакансией</p>
        </div>
    """, unsafe_allow_html=True)

    if not profile:
        st.info("Резюме не загружено. Можно смотреть вакансии без профиля, но AI-анализ будет точнее после загрузки резюме.")

    with st.sidebar:
        st.markdown("## ⚙️ Подключение к БД")

        host = st.text_input("Host", value=DEFAULT_HOST, type="password")
        port = st.number_input("Port", min_value=1, max_value=65535, value=DEFAULT_PORT)
        database = st.text_input("Database", value=DEFAULT_DB)
        schema = st.text_input("Schema", value=DEFAULT_SCHEMA)
        table_name = st.text_input("Таблица вакансий", value=DEFAULT_TABLE)
        user = st.text_input("User", value=DEFAULT_USER)
        password = st.text_input("Password", value=DEFAULT_PASSWORD, type="password")
        limit = st.slider("Сколько вакансий загрузить", 20, 50000, 500, 20)

        load_btn = st.button("Загрузить вакансии из БД", use_container_width=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        location = st.selectbox("Локация", ["Все", "Удалённо", "Москва", "Санкт-Петербург", "Другое"])
    with col2:
        experience = st.selectbox("Опыт", ["Все", "Junior", "Middle", "Senior", "Lead"])
    with col3:
        salary_min = st.number_input("Мин. зарплата", min_value=0, value=100000, step=50000)
    with col4:
        job_type = st.selectbox("Тип занятости", ["Все", "Полная", "Частичная", "Проектная"])

    search_query = st.text_input("🔍 Поиск вакансий", placeholder="Должность, компания, город...")

    if load_btn or st.session_state.vacancies_df is None:
        try:
            db_url = get_db_url(host, port, database, user, password)
            with st.spinner("Загрузка вакансий из PostgreSQL..."):
                df = load_vacancies_from_db(
                    db_url=db_url,
                    schema=schema,
                    table_name=table_name,
                    limit=limit,
                )
                df = normalize_vacancies_df(df)
                st.session_state.vacancies_df = df
            st.success(f"Загружено вакансий: {len(st.session_state.vacancies_df)}")
        except Exception as e:
            st.error(f"Ошибка загрузки вакансий из БД: {e}")
            st.session_state.vacancies_df = pd.DataFrame()

    df = st.session_state.vacancies_df
    if df is None or df.empty:
        st.warning("Нет данных по вакансиям. Проверь подключение к БД и имя таблицы.")
        return

    filtered_df = apply_filters(
        df=df,
        search_query=search_query,
        location=location,
        experience=experience,
        salary_min=salary_min,
        job_type=job_type,
    )

    vacancies = dataframe_to_cards(filtered_df)

    metric_col1, metric_col2, metric_col3 = st.columns(3)

    with metric_col1:
        st.metric("Всего вакансий", len(vacancies))

    with metric_col2:
        matches = 0
        for v in vacancies:
            if float(v.get("final_score", 0) or 0) > 0.7:
                matches += 1
        st.metric("Совпадения с ИИ", matches)

    with metric_col3:
        avg_salary = 0
        salary_values = filtered_df["salary_avg"].dropna().tolist() if "salary_avg" in filtered_df.columns else []
        if salary_values:
            avg_salary = sum(salary_values) / len(salary_values)
        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽".replace(",", " "))

    top_col1, top_col2 = st.columns(2)
    with top_col1:
        if st.button("🤖 Перейти в карьерный чат", use_container_width=True):
            st.switch_page("pages/04_🎯_Interview_Simulator.py")
    with top_col2:
        if st.button("📄 Перейти к резюме", use_container_width=True):
            st.switch_page("pages/01_📄_Resume_Upload.py")

    api_client = APIClient(API_BASE_URL)

    if vacancies:
        st.markdown("### Доступные позиции")

        for idx, vacancy in enumerate(vacancies, start=1):
            render_vacancy_card(
                vacancy,
                on_analyze=lambda v, profile=profile: go_to_chat_with_fit_seed(v, profile),
            )

            action_col1, action_col2, action_col3, action_col4 = st.columns(4)

            with action_col1:
                if st.button(f"Выбрать #{idx}", key=f"choose_vacancy_{idx}"):
                    push_selected_vacancy_to_state(vacancy, profile)
                    go_to_chat_with_selected_vacancy(vacancy)

            with action_col2:
                if st.button(f"Roadmap #{idx}", key=f"roadmap_vacancy_{idx}"):
                    try:
                        push_selected_vacancy_to_state(vacancy, profile)

                        result = api_client.chat(
                            message="roadmap",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", st.session_state.backend_state)

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"roadmap по вакансии: {vacancy.get('title', 'Без названия')}"},
                            {"role": "assistant", "content": result.get("response", "Нет ответа от backend")},
                        ]
                        st.switch_page("pages/04_🎯_Interview_Simulator.py")
                    except Exception as e:
                        st.error(f"Ошибка roadmap: {e}")

            with action_col3:
                if st.button(f"Interview #{idx}", key=f"interview_vacancy_{idx}"):
                    try:
                        push_selected_vacancy_to_state(vacancy, profile)

                        result = api_client.chat(
                            message="interview",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", st.session_state.backend_state)

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"interview по вакансии: {vacancy.get('title', 'Без названия')}"},
                            {"role": "assistant", "content": result.get("response", "Нет ответа от backend")},
                        ]
                        st.switch_page("pages/04_🎯_Interview_Simulator.py")
                    except Exception as e:
                        st.error(f"Ошибка interview: {e}")

            with action_col4:
                if st.button(f"Fit #{idx}", key=f"fit_vacancy_{idx}"):
                    try:
                        push_selected_vacancy_to_state(vacancy, profile)

                        result = api_client.chat(
                            message="fit analysis",
                            state=st.session_state.backend_state,
                        )
                        st.session_state.backend_state = result.get("state", st.session_state.backend_state)

                        st.session_state.chat_messages_seed = [
                            {"role": "user", "content": f"fit analysis по вакансии: {vacancy.get('title', 'Без названия')}"},
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