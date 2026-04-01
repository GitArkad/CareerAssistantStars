"""
Компоненты для отображения вакансий
"""

import streamlit as st
from typing import Dict, List
from datetime import datetime


def render_vacancy_card(
    vacancy: Dict,
    show_match_score: bool = True,
    on_analyze=None,
):
    """
    Рендерит карточку вакансии

    Args:
        vacancy: Данные вакансии
        show_match_score: Показывать процент совпадения
        on_analyze: callback для кнопки "Анализ соответствия"
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
        salary_from = vacancy.get("salary_from")
        salary_to = vacancy.get("salary_to")
        currency = vacancy.get("currency", "₽")

        if salary_from or salary_to:
            if salary_to:
                salary_text = f"{salary_from:,} - {salary_to:,} {currency}"
            else:
                salary_text = f"от {salary_from:,} {currency}"
            st.markdown(f"**{salary_text}**")
        else:
            st.write("По собеседованию")

    with col2:
        st.markdown("### 📍 Локация")
        st.write(f"📍 {vacancy.get('location', vacancy.get('city', 'Не указано'))}")
        st.write(f"{'🏠' if vacancy.get('remote', False) else '🏢'} {vacancy.get('work_type', 'Полная')}")

    with col3:
        st.markdown("### 💼 Опыт")
        st.write(f"📅 {vacancy.get('experience', vacancy.get('seniority', 'Не указан'))}")

    with col4:
        st.markdown("### Опубликовано")
        posted_date = vacancy.get("posted_date") or vacancy.get("created_at")
        if posted_date:
            try:
                posted_str = str(posted_date).replace("Z", "+00:00")
                days_ago = (datetime.now() - datetime.fromisoformat(posted_str)).days
                st.write("Сегодня" if days_ago == 0 else f"{days_ago} дн. назад")
            except Exception:
                st.write(str(posted_date)[:10])
        else:
            st.write("Не указано")

    # Описание
    if vacancy.get("description"):
        st.markdown("### 📋 Описание")
        description = str(vacancy["description"])
        st.write(description[:300] + "..." if len(description) > 300 else description)

    # Требования
    if vacancy.get("requirements"):
        st.markdown("### ✅ Требования")
        requirements = vacancy["requirements"]
        if isinstance(requirements, str):
            requirements = [requirements]

        for req in requirements[:5]:
            st.write(f"• {req}")

        if len(requirements) > 5:
            st.caption(f"+ ещё {len(requirements) - 5} требований")

    # Навыки
    if vacancy.get("skills"):
        st.markdown("### 💡 Необходимые навыки")
        skills = vacancy["skills"]
        if isinstance(skills, str):
            skills = [s.strip() for s in skills.split(",") if s.strip()]

        cols = st.columns(min(len(skills), 6))
        for i, skill in enumerate(skills[:6]):
            with cols[i % len(cols)]:
                st.markdown(f"""
                    <div class='vacancy-skill-tag'>
                        {skill}
                    </div>
                """, unsafe_allow_html=True)

    # Кнопки действий
    st.markdown("<div class='vacancy-actions'>", unsafe_allow_html=True)

    vacancy_id = (
        vacancy.get("job_id")
        or vacancy.get("id")
        or vacancy.get("title")
        or "vacancy"
    )

    vacancy_url = (
        vacancy.get("url")
        or vacancy.get("vacancy_url")
        or vacancy.get("alternate_url")
        or vacancy.get("apply_url")
        or vacancy.get("job_url")
        or vacancy.get("source_url")
        or vacancy.get("hh_url")
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        if vacancy_url:
            st.link_button(
                "📤 Откликнуться",
                url=vacancy_url,
                use_container_width=True,
                type="primary",
            )
        else:
            st.button(
                "📤 Откликнуться",
                type="primary",
                use_container_width=True,
                key=f"respond_{vacancy_id}",
                disabled=True,
            )
            st.caption("Ссылка на вакансию отсутствует")

    with col2:
        if st.button(
            "💾 Сохранить",
            use_container_width=True,
            key=f"save_{vacancy_id}",
        ):
            st.info("Вакансия сохранена")

    with col3:
        if st.button(
            "📊 Анализ соответствия",
            use_container_width=True,
            key=f"analyze_{vacancy_id}",
        ):
            if on_analyze is not None:
                on_analyze(vacancy)
            else:
                st.info("Запуск анализа...")

    st.markdown("</div>", unsafe_allow_html=True)


def render_job_match_score(score: float):
    """
    Рендерит индикатор процента совпадения с вакансией
    """
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
        "location": location if location != "Все" else None,
        "experience": experience if experience != "Все" else None,
        "salary_min": salary_range[0] * 1000,
        "salary_max": salary_range[1] * 1000,
        "job_type": job_type if job_type else None
    }


def render_vacancy_stats(vacancies: List[Dict]):
    """
    Рендерит статистику по вакансиям
    """
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Всего вакансий", len(vacancies))

    with col2:
        high_match = sum(1 for v in vacancies if v.get("match_score", 0) >= 80)
        st.metric("Отличные совпадения", high_match)

    with col3:
        avg_salary = sum(v.get("salary_from", 0) for v in vacancies) / len(vacancies) if vacancies else 0
        st.metric("Средняя зарплата", f"{avg_salary:,.0f} ₽")

    with col4:
        companies = len(set(v.get("company") for v in vacancies if v.get("company")))
        st.metric("Компаний", companies)