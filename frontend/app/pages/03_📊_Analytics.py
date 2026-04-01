import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, inspect, text

from utils.style_loader import apply_custom_styles

apply_custom_styles()
st.set_page_config(page_title="Аналитика рынка", page_icon="📊", layout="wide")

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
# CONFIG
# =========================
DEFAULT_HOST = "16.54.110.212"
DEFAULT_PORT = 5433
DEFAULT_DB = "ai_career"   # замени на реальное имя БД
DEFAULT_SCHEMA = "public"

# =========================
# COLUMN MAPPING (RU)
# =========================
COLUMN_MAPPING = {
    "job_id": "ID вакансии",
    "title": "Название",
    "company": "Компания",
    "city": "Город",
    "salary_from": "ЗП от",
    "salary_to": "ЗП до",
    "salary_avg": "Средняя ЗП",
    "experience": "Опыт",
    "seniority": "Уровень",
    "created_at": "Дата создания",
    "updated_at": "Дата обновления",
    "source": "Источник",
    "remote": "Удалённо",
    "employment_type": "Тип занятости",
    "schedule": "График",
    "description": "Описание",
    "skills": "Навыки",
    "url": "Ссылка",
    "role": "Роль",
    "sample_size": "Количество",
    "count": "Количество"
}


def col_label(col_name: str) -> str:
    return COLUMN_MAPPING.get(col_name, col_name)


def get_db_url(host: str, port: int, database: str, user: str, password: str) -> str:
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


@st.cache_resource(show_spinner=False)
def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)


@st.cache_data(ttl=60, show_spinner=False)
def get_tables(db_url: str, schema: str):
    engine = get_engine(db_url)
    inspector = inspect(engine)
    return inspector.get_table_names(schema=schema)


@st.cache_data(ttl=60, show_spinner=False)
def get_columns(db_url: str, schema: str, table_name: str):
    engine = get_engine(db_url)
    inspector = inspect(engine)
    cols = inspector.get_columns(table_name, schema=schema)
    return [c["name"] for c in cols]


@st.cache_data(ttl=60, show_spinner=False)
def load_data(db_url: str, query: str) -> pd.DataFrame:
    engine = get_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def guess_date_column(df: pd.DataFrame):
    for col in df.columns:
        col_lower = str(col).lower()
        if any(x in col_lower for x in ["date", "created", "updated", "time", "posted"]):
            try:
                parsed = pd.to_datetime(df[col], errors="coerce")
                if parsed.notna().sum() > 0:
                    return col
            except Exception:
                pass
    return None


def guess_salary_column(df: pd.DataFrame):
    candidates = [
        "salary_avg", "salary_from", "salary_to",
        "salary", "compensation", "income"
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def guess_category_column(df: pd.DataFrame):
    candidates = ["city", "company", "title", "seniority", "source", "experience"]
    for col in candidates:
        if col in df.columns:
            return col
    for col in df.columns:
        if df[col].dtype == "object":
            return col
    return None


def safe_top_words(series: pd.Series, n: int = 15) -> pd.DataFrame:
    words = {}

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
            <h1>📊 Аналитика из PostgreSQL</h1>
            <p>BI-страница в стиле mini-Superset: таблицы, фильтры, SQL и графики</p>
        </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("## ⚙️ Подключение к БД")

        host = st.text_input("Host", value=DEFAULT_HOST, type="password")
        port = st.number_input(
            "Port",
            min_value=1,
            max_value=65535,
            value=int(os.getenv("POSTGRES_PORT", DEFAULT_PORT))
        )
        database = st.text_input("Database", value=os.getenv("POSTGRES_DB", DEFAULT_DB))
        schema = st.text_input("Schema", value=os.getenv("POSTGRES_SCHEMA", DEFAULT_SCHEMA))
        user = st.text_input("User", value=os.getenv("POSTGRES_USER", "postgres"))
        password = st.text_input("Password", value=os.getenv("POSTGRES_PASSWORD", ""), type="password")

        connect_btn = st.button("Подключиться", use_container_width=True)

    if "db_connected" not in st.session_state:
        st.session_state.db_connected = False
    if "db_url" not in st.session_state:
        st.session_state.db_url = None

    if connect_btn:
        try:
            db_url = get_db_url(host, port, database, user, password)
            engine = get_engine(db_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.session_state.db_connected = True
            st.session_state.db_url = db_url
            st.success("Подключение к PostgreSQL успешно.")
        except Exception as e:
            st.session_state.db_connected = False
            st.error(f"Ошибка подключения к PostgreSQL: {e}")

    if not st.session_state.db_connected or not st.session_state.db_url:
        st.info("Заполни параметры подключения слева и нажми «Подключиться».")
        return

    db_url = st.session_state.db_url

    try:
        tables = get_tables(db_url, schema)
    except Exception as e:
        st.error(f"Не удалось получить список таблиц: {e}")
        return

    if not tables:
        st.warning(f"В схеме '{schema}' нет таблиц.")
        return

    st.markdown("### 🗂 Источник данных")

    top1, top2 = st.columns([2, 3])

    with top1:
        selected_table = st.selectbox("Таблица", tables)

    try:
        columns = get_columns(db_url, schema, selected_table)
    except Exception as e:
        st.error(f"Не удалось получить колонки таблицы: {e}")
        return

    default_limit = 30000

    with top2:
        st.caption("Можно использовать автозагрузку таблицы или написать свой SQL.")
        mode = st.radio("Режим", ["Таблица", "SQL"], horizontal=True)

    if mode == "Таблица":
        query = f'SELECT * FROM "{schema}"."{selected_table}" LIMIT {default_limit}'
    else:
        query = st.text_area(
            "SQL запрос",
            value=f'SELECT * FROM "{schema}"."{selected_table}" LIMIT {default_limit}',
            height=140
        )

    with st.expander("Показать SQL"):
        st.code(query, language="sql")

    try:
        with st.spinner("Загрузка данных из PostgreSQL..."):
            df = load_data(db_url, query)
    except Exception as e:
        st.error(f"Ошибка выполнения SQL: {e}")
        return

    if df.empty:
        st.warning("Запрос не вернул данных.")
        return

    # Попытка привести даты
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                converted = pd.to_datetime(df[col], errors="ignore")
                df[col] = converted
            except Exception:
                pass

    # Фильтры
    st.markdown("### 🔎 Фильтры")
    date_col = guess_date_column(df)
    category_col = guess_category_column(df)
    salary_col = guess_salary_column(df)

    f1, f2, f3 = st.columns(3)

    filtered_df = df.copy()

    with f1:
        if category_col and category_col in filtered_df.columns:
            options = sorted([str(x) for x in filtered_df[category_col].dropna().astype(str).unique().tolist()])
            selected_values = st.multiselect(
                f"Фильтр по {col_label(category_col)}",
                options
            )
            if selected_values:
                filtered_df = filtered_df[filtered_df[category_col].astype(str).isin(selected_values)]
        else:
            st.write(" ")

    with f2:
        if salary_col and salary_col in filtered_df.columns:
            filtered_df[salary_col] = pd.to_numeric(filtered_df[salary_col], errors="coerce")
            min_val = int(filtered_df[salary_col].dropna().min()) if filtered_df[salary_col].dropna().shape[0] else 0
            max_val = int(filtered_df[salary_col].dropna().max()) if filtered_df[salary_col].dropna().shape[0] else 0

            if max_val > min_val:
                salary_range = st.slider(
                    f"Диапазон: {col_label(salary_col)}",
                    min_value=min_val,
                    max_value=max_val,
                    value=(min_val, max_val)
                )
                filtered_df = filtered_df[
                    filtered_df[salary_col].fillna(0).between(salary_range[0], salary_range[1])
                ]
        else:
            st.write(" ")

    with f3:
        if date_col and date_col in filtered_df.columns:
            parsed_dates = pd.to_datetime(filtered_df[date_col], errors="coerce").dropna()
            if not parsed_dates.empty:
                min_date = parsed_dates.min().date()
                max_date = parsed_dates.max().date()
                selected_dates = st.date_input(
                    f"Период по {col_label(date_col)}",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date
                )
                if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                    start_date, end_date = selected_dates
                    date_series = pd.to_datetime(filtered_df[date_col], errors="coerce")
                    filtered_df = filtered_df[
                        (date_series.dt.date >= start_date) &
                        (date_series.dt.date <= end_date)
                    ]
        else:
            st.write(" ")

    if filtered_df.empty:
        st.warning("После применения фильтров данные отсутствуют.")
        return

    # KPI
    st.markdown("### 📌 Ключевые метрики")
    total_rows = len(filtered_df)
    total_cols = len(filtered_df.columns)
    null_cells = int(filtered_df.isna().sum().sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Строк", total_rows)
    c2.metric("Колонок", total_cols)
    c3.metric("Пустых значений", null_cells)

    if salary_col and salary_col in filtered_df.columns:
        filtered_df[salary_col] = pd.to_numeric(filtered_df[salary_col], errors="coerce")
        avg_salary = round(filtered_df[salary_col].dropna().mean(), 0) if filtered_df[salary_col].notna().sum() else 0
        c4.metric(f"Среднее по {col_label(salary_col)}", f"{avg_salary:,.0f}".replace(",", " "))
    else:
        c4.metric("Среднее", "—")

    # Авто-визуализации
    st.markdown("### 📈 Визуализации")

    left, right = st.columns(2)

    with left:
        if category_col and category_col in filtered_df.columns:
            vc = (
                filtered_df[category_col]
                .astype(str)
                .value_counts()
                .head(10)
                .reset_index()
            )
            vc.columns = [category_col, "count"]
            fig = px.bar(
                vc,
                x=category_col,
                y="count",
                title=f"Топ значений: {col_label(category_col)}",
                labels={
                    category_col: col_label(category_col),
                    "count": col_label("count")
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет категориальной колонки для столбчатой диаграммы.")

    with right:
        numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
        hist_col = salary_col if salary_col in numeric_cols else (numeric_cols[0] if numeric_cols else None)

        if hist_col:
            fig = px.histogram(
                filtered_df,
                x=hist_col,
                nbins=30,
                title=f"Распределение: {col_label(hist_col)}",
                labels={
                    hist_col: col_label(hist_col),
                    "count": col_label("count")
                }
            )

            fig.update_xaxes(title_text=col_label(hist_col))
            fig.update_yaxes(title_text=col_label("count"))

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет числовой колонки для гистограммы.")

    left2, right2 = st.columns(2)

    with left2:
        if "city" in filtered_df.columns:
            city_grouped = (
                filtered_df["city"]
                .fillna("Не указано")
                .astype(str)
                .value_counts()
                .head(15)
                .reset_index()
            )
            city_grouped.columns = ["city", "count"]

            fig = px.bar(
                city_grouped,
                x="count",
                y="city",
                orientation="h",
                title="Топ городов по числу вакансий",
                labels={
                    "city": "Город",
                    "count": "Количество вакансий"
                }
            )

            fig.update_layout(
                xaxis_title="Количество вакансий",
                yaxis_title="Город",
                yaxis=dict(autorange="reversed")
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет колонки city для анализа.")

    with right2:
        text_col = "title" if "title" in filtered_df.columns else category_col
        if text_col and text_col in filtered_df.columns:
            words_df = safe_top_words(filtered_df[text_col], n=15)
            if not words_df.empty:
                fig = px.bar(
                    words_df,
                    x="word",
                    y="count",
                    title=f"Частые слова: {col_label(text_col)}",
                    labels={
                        "word": "Дожность",
                        "count": "Количество"
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Недостаточно текстовых данных.")
        else:
            st.info("Нет текстовой колонки для анализа слов.")

    # Пользовательские графики
    st.markdown("### 🛠 Конструктор графика")

    numeric_cols = filtered_df.select_dtypes(include=["number"]).columns.tolist()
    all_cols = filtered_df.columns.tolist()

    col_options = {col_label(col): col for col in all_cols}
    numeric_options = {col_label(col): col for col in numeric_cols} if numeric_cols else col_options

    p1, p2, p3 = st.columns(3)

    with p1:
        chart_type = st.selectbox("Тип графика", ["bar", "line", "scatter", "pie"])

    with p2:
        x_label_selected = st.selectbox("Ось X", list(col_options.keys()))
        x_col = col_options[x_label_selected]

    with p3:
        y_label_selected = st.selectbox("Ось Y", list(numeric_options.keys()))
        y_col = numeric_options[y_label_selected]

    try:
        if chart_type == "bar":
            plot_df = filtered_df.groupby(x_col, dropna=False)[y_col].mean().reset_index().head(30)
            fig = px.bar(
                plot_df,
                x=x_col,
                y=y_col,
                title=f"Столбчатая диаграмма: {col_label(x_col)} × {col_label(y_col)}",
                labels={x_col: col_label(x_col), y_col: col_label(y_col)}
            )
        elif chart_type == "line":
            plot_df = filtered_df.groupby(x_col, dropna=False)[y_col].mean().reset_index().head(100)
            fig = px.line(
                plot_df,
                x=x_col,
                y=y_col,
                title=f"Линейный график: {col_label(x_col)} × {col_label(y_col)}",
                labels={x_col: col_label(x_col), y_col: col_label(y_col)}
            )
        elif chart_type == "scatter":
            fig = px.scatter(
                filtered_df.head(1000),
                x=x_col,
                y=y_col,
                title=f"Точечный график: {col_label(x_col)} × {col_label(y_col)}",
                labels={x_col: col_label(x_col), y_col: col_label(y_col)}
            )
        else:
            pie_df = filtered_df[x_col].astype(str).value_counts().head(10).reset_index()
            pie_df.columns = [x_col, "count"]
            fig = px.pie(
                pie_df,
                names=x_col,
                values="count",
                title=f"Круговая диаграмма: {col_label(x_col)}",
                labels={x_col: col_label(x_col), "count": "Количество"}
            )

        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Не удалось построить пользовательский график: {e}")

    # Данные
    st.markdown("### 🧾 Данные")
    display_df = filtered_df.rename(columns=COLUMN_MAPPING)
    st.dataframe(display_df, use_container_width=True)

    csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Скачать CSV",
        data=csv_data,
        file_name=f"{selected_table}_analytics.csv",
        mime="text/csv",
        use_container_width=True
    )


if __name__ == "__main__":
    main()