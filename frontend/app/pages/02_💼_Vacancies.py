# app/pages/03_💼_Вакансии.py
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

from components.vacancy_card import render_vacancy_card
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles  # ← ДОБАВИТЬ

# Применяем стили
apply_custom_styles()
st.set_page_config(page_title="Вакансии", page_icon="💼", layout="wide")

def main():
    st.markdown("""
        <div class='page-header'>
            <h1>💼 Вакансии</h1>
            <p>Найдите вакансии, подобранные ИИ на основе вашего профиля</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Фильтры
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        location = st.selectbox("Локация", ["Все", "Удалённо", "Москва", "Санкт-Петербург", "Другое"])
    with col2:
        experience = st.selectbox("Опыт", ["Все", "Junior", "Middle", "Senior", "Lead"])
    with col3:
        salary_min = st.number_input("Мин. зарплата", min_value=0, value=100000, step=50000)
    with col4:
        job_type = st.selectbox("Тип занятости", ["Все", "Полная", "Частичная", "Проектная"])
    
    # Поиск
    search_query = st.text_input("🔍 Поиск вакансий", placeholder="Должность, ключевые слова...")
    
    # Получить вакансии из API
    api_client = APIClient(st.secrets.get("API_BASE_URL", "http://localhost:8501"))
    
    ##############
    # AIRFLOW PIPELINE PLACEHOLDER
    # Этот раздел получает данные, которые были собраны и обработаны пайплайном Airflow
    # Пайплайн Airflow обрабатывает:
    # - Парсинг сайтов с вакансиями (hh.ru, Adzuna и др.)
    # - Очистку и нормализацию данных
    # - Загрузку в PostgreSQL + QDrant
    ##############
    
    vacancies = api_client.get_vacancies(
        location=location if location != "Все" else None,
        experience=experience if experience != "Все" else None,
        salary_min=salary_min,
        search_query=search_query
    )
    
    ##############
    # END AIRFLOW PLACEHOLDER
    ##############
    
    # Отобразить статистику
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего вакансий", len(vacancies))
    with col2:
        st.metric("Совпадения с ИИ", sum(1 for v in vacancies if v.get('match_score', 0) > 80))
    with col3:
        avg_salary = sum(v.get('salary', 0) for v in vacancies) / len(vacancies) if vacancies else 0
        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽")
    
    # Список вакансий
    st.markdown("### Доступные позиции")
    
    if vacancies:
        for vacancy in vacancies:
            render_vacancy_card(vacancy)
            st.markdown("---")
    else:
        st.warning("Не найдено вакансий, соответствующих вашим критериям")

if __name__ == "__main__":
    main()