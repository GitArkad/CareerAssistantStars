"""
Компоненты для отображения резюме
"""

import streamlit as st
from typing import Dict, List, Optional


def render_resume_card(resume_data: Dict, show_actions: bool = True):
    """
    Рендерит карточку резюме
    
    Args:
        resume_data: Данные резюме
        show_actions: Показывать кнопки действий
    """
    st.markdown("""
        <div class='resume-card'>
    """, unsafe_allow_html=True)
    
    # Заголовок
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown(f"""
            <div class='resume-header'>
                <h3>{resume_data.get('name', 'Имя не указано')}</h3>
                <p class='resume-position'>{resume_data.get('target_position', 'Должность не указана')}</p>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Общая оценка
        score = resume_data.get('overall_score', 0)
        score_color = get_score_color(score)
        st.markdown(f"""
            <div class='resume-score' style='color: {score_color}'>
                {score}/100
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Основная информация
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 📧 Контакты")
        st.write(f"📱 {resume_data.get('phone', 'Не указан')}")
        st.write(f"✉️ {resume_data.get('email', 'Не указан')}")
        st.write(f"📍 {resume_data.get('location', 'Не указано')}")
    
    with col2:
        st.markdown("### 💼 Опыт")
        st.write(f"📅 {resume_data.get('total_experience', '0 лет')}")
        st.write(f"🏢 {len(resume_data.get('experience', []))} компаний")
    
    with col3:
        st.markdown("### 🎓 Образование")
        st.write(f"🎓 {len(resume_data.get('education', []))} учебных заведений")
    
    # Навыки
    if 'skills' in resume_data and resume_data['skills']:
        st.markdown("### 💡 Ключевые навыки")
        skills_container = st.container()
        with skills_container:
            cols = st.columns(min(len(resume_data['skills']), 5))
            for i, skill in enumerate(resume_data['skills'][:5]):
                with cols[i % 5]:
                    render_skill_badge(skill)
            
            if len(resume_data['skills']) > 5:
                st.caption(f"+ {len(resume_data['skills']) - 5} других навыков")
    
    # Рекомендации
    if 'recommendations' in resume_data and resume_data['recommendations']:
        st.markdown("### 💡 Рекомендации по улучшению")
        for rec in resume_data['recommendations'][:3]:
            st.info(f"💬 {rec}")
    
    # Кнопки действий
    if show_actions:
        st.markdown("<div class='resume-actions'>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📥 Скачать PDF", use_container_width=True):
                st.success("Резюме скачано!")
        
        with col2:
            if st.button("✏️ Редактировать", use_container_width=True):
                st.info("Редактирование...")
        
        with col3:
            if st.button("📊 Анализ", use_container_width=True):
                st.info("Запуск анализа...")
        
        with col4:
            if st.button("🗑️ Удалить", use_container_width=True):
                st.warning("Резюме удалено")
        
        st.markdown("</div>", unsafe_allow_html=True)


def render_skill_badge(skill: str, level: Optional[str] = None):
    """
    Рендерит бейдж навыка
    
    Args:
        skill: Название навыка
        level: Уровень (Junior/Middle/Senior)
    """
    level_colors = {
        'Junior': '#10b981',
        'Middle': '#3b82f6',
        'Senior': '#8b5cf6',
        None: '#6b7280'
    }
    
    color = level_colors.get(level, level_colors[None])
    
    st.markdown(f"""
        <div class='skill-badge' style='background-color: {color}20; border-color: {color}'>
            <span style='color: {color}'>{skill}</span>
            {f'<span class="skill-level">{level}</span>' if level else ''}
        </div>
    """, unsafe_allow_html=True)


def render_experience_item(experience: Dict):
    """
    Рендерит элемент опыта работы
    
    Args:
        experience: Данные о месте работы
    """
    with st.expander(f"**{experience.get('position', 'Должность')}** в {experience.get('company', 'Компания')}"):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.write(experience.get('description', 'Описание отсутствует'))
            
            if 'achievements' in experience:
                st.markdown("##### 🏆 Достижения:")
                for achievement in experience['achievements']:
                    st.write(f"• {achievement}")
        
        with col2:
            st.caption(f"📅 {experience.get('start_date', '')} — {experience.get('end_date', 'н.в.')}")
            st.caption(f"📍 {experience.get('location', '')}")


def get_score_color(score: int) -> str:
    """Возвращает цвет для оценки"""
    if score >= 80:
        return '#10b981'  # Зеленый
    elif score >= 60:
        return '#3b82f6'  # Синий
    elif score >= 40:
        return '#f59e0b'  # Оранжевый
    else:
        return '#ef4444'  # Красный