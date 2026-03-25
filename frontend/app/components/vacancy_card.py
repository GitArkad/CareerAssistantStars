"""
Компоненты для отображения вакансий
"""

import streamlit as st
from typing import Dict, List
from datetime import datetime


def render_vacancy_card(vacancy: Dict, show_match_score: bool = True):
    """
    Рендерит карточку вакансии
    
    Args:
        vacancy: Данные вакансии
        show_match_score: Показывать процент совпадения
    """
    st.markdown("""
        <div class='vacancy-card'>
    """, unsafe_allow_html=True)
    
    # Шапка вакансии
    col1, col2 = st.columns([4, 1])
    
    with col1:
        st.markdown(f"""
            <div class='vacancy-header'>
                <h3>{vacancy.get('title', 'Название вакансии')}</h3>
                <p class='vacancy-company'>🏢 {vacancy.get('company', 'Компания не указана')}</p>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        if show_match_score and 'match_score' in vacancy:
            render_job_match_score(vacancy['match_score'])
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Основная информация
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("### 💰 Зарплата")
        salary_from = vacancy.get('salary_from')
        salary_to = vacancy.get('salary_to')
        currency = vacancy.get('currency', '₽')
        
        if salary_from or salary_to:
            salary_text = f"{salary_from:,} - {salary_to:,} {currency}" if salary_to else f"от {salary_from:,} {currency}"
            st.markdown(f"**{salary_text}**")
        else:
            st.write("По собеседованию")
    
    with col2:
        st.markdown("### 📍 Локация")
        st.write(f"📍 {vacancy.get('location', 'Не указано')}")
        st.write(f"{'🏠' if vacancy.get('remote', False) else '🏢'} {vacancy.get('work_type', 'Полная')}")
    
    with col3:
        st.markdown("### 💼 Опыт")
        st.write(f"📅 {vacancy.get('experience', 'Не указан')}")
    
    with col4:
        st.markdown("###  Опубликовано")
        posted_date = vacancy.get('posted_date')
        if posted_date:
            days_ago = (datetime.now() - datetime.fromisoformat(posted_date)).days
            st.write(f"{'Сегодня' if days_ago == 0 else f'{days_ago} дн. назад'}")
    
    # Описание
    if 'description' in vacancy and vacancy['description']:
        st.markdown("### 📋 Описание")
        st.write(vacancy['description'][:300] + "..." if len(vacancy['description']) > 300 else vacancy['description'])
    
    # Требования
    if 'requirements' in vacancy and vacancy['requirements']:
        st.markdown("### ✅ Требования")
        for req in vacancy['requirements'][:5]:
            st.write(f"• {req}")
        if len(vacancy['requirements']) > 5:
            st.caption(f"+ ещё {len(vacancy['requirements']) - 5} требований")
    
    # Навыки
    if 'skills' in vacancy and vacancy['skills']:
        st.markdown("### 💡 Необходимые навыки")
        cols = st.columns(min(len(vacancy['skills']), 6))
        for i, skill in enumerate(vacancy['skills'][:6]):
            with cols[i % 6]:
                st.markdown(f"""
                    <div class='vacancy-skill-tag'>
                        {skill}
                    </div>
                """, unsafe_allow_html=True)
    
    # Кнопки действий
    st.markdown("<div class='vacancy-actions'>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📤 Откликнуться", type="primary", use_container_width=True):
            st.success("Отклик отправлен!")
    
    with col2:
        if st.button("💾 Сохранить", use_container_width=True):
            st.info("Вакансия сохранена")
    
    with col3:
        if st.button("📊 Анализ соответствия", use_container_width=True):
            st.info("Запуск анализа...")
    
    st.markdown("</div>", unsafe_allow_html=True)


def render_job_match_score(score: float):
    """
    Рендерит индикатор процента совпадения с вакансией
    
    Args:
        score: Процент совпадения (0-100)
    """
    # Определяем цвет и эмодзи
    if score >= 80:
        color = '#10b981'
        emoji = '🎯'
        text = 'Отличное совпадение!'
    elif score >= 60:
        color = '#3b82f6'
        emoji = '👍'
        text = 'Хорошее совпадение'
    elif score >= 40:
        color = '#f59e0b'
        emoji = '⚠️'
        text = 'Среднее совпадение'
    else:
        color = '#ef4444'
        emoji = '❌'
        text = 'Слабое совпадение'
    
    st.markdown(f"""
        <div class='match-score' style='border-color: {color}'>
            <div class='match-score-circle' style='background-color: {color}'>
                <span>{int(score)}%</span>
            </div>
            <div class='match-score-text' style='color: {color}'>
                {emoji} {text}
            </div>
        </div>
    """, unsafe_allow_html=True)


def render_vacancy_filters():
    """
    Рендерит панель фильтров для вакансий
    
    Returns:
        Dict с выбранными фильтрами
    """
    st.markdown("### 🔍 Фильтры")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        location = st.selectbox(
            "📍 Локация",
            ["Все", "Москва", "Санкт-Петербург", "Удалённо", "Другое"]
        )
    
    with col2:
        experience = st.selectbox(
            "💼 Опыт",
            ["Все", "Junior", "Middle", "Senior", "Lead"]
        )
    
    with col3:
        salary_range = st.slider(
            "💰 Зарплата (тыс. ₽)",
            min_value=0,
            max_value=500,
            value=(100, 300)
        )
    
    with col4:
        job_type = st.multiselect(
            "🏠 Тип занятости",
            ["Полная", "Частичная", "Проектная", "Стажировка"]
        )
    
    return {
        'location': location if location != "Все" else None,
        'experience': experience if experience != "Все" else None,
        'salary_min': salary_range[0] * 1000,
        'salary_max': salary_range[1] * 1000,
        'job_type': job_type if job_type else None
    }


def render_vacancy_stats(vacancies: List[Dict]):
    """
    Рендерит статистику по вакансиям
    
    Args:
        vacancies: Список вакансий
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего вакансий", len(vacancies))
    
    with col2:
        high_match = sum(1 for v in vacancies if v.get('match_score', 0) >= 80)
        st.metric("Отличные совпадения", high_match)
    
    with col3:
        avg_salary = sum(v.get('salary_from', 0) for v in vacancies) / len(vacancies) if vacancies else 0
        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽")
    
    with col4:
        companies = len(set(v.get('company') for v in vacancies if v.get('company')))
        st.metric("Компаний", companies)