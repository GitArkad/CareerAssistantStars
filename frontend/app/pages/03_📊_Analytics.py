# app/pages/04_📊_Аналитика.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.style_loader import apply_custom_styles  # ← ДОБАВИТЬ

# Применяем стили
apply_custom_styles()
st.set_page_config(page_title="Аналитика рынка", page_icon="📊", layout="wide")

def main():
    st.markdown("""
        <div class='page-header'>
            <h1>📊 Аналитика рынка</h1>
            <p>Актуальные инсайты о трендах и спросе на рынке труда</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Выбор периода
    period = st.selectbox("Период", ["Последние 7 дней", "Последние 30 дней", "Последние 3 месяца", "Последний год"])
    
    # Метрики
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего вакансий", "12 453", "+8,5%")
    with col2:
        st.metric("Рост средней ЗП", "+12%", "+2,1%")
    with col3:
        st.metric("Самый востребованный навык", "Python", "+15%")
    with col4:
        st.metric("Конкуренция на рынке", "Средняя", "-3%")
    
    # Графики
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Динамика зарплат по уровню опыта")
        salary_data = pd.DataFrame({
            'Опыт': ['Junior', 'Middle', 'Senior', 'Lead'],
            'Зарплата': [80000, 150000, 250000, 400000]
        })
        fig = px.bar(salary_data, x='Опыт', y='Зарплата', 
                     color='Опыт', template='plotly_white')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Топ востребованных навыков")
        skills_data = pd.DataFrame({
            'Навык': ['Python', 'SQL', 'Docker', 'Kubernetes', 'AWS'],
            'Спрос': [85, 72, 65, 58, 55]
        })
        fig = px.pie(skills_data, values='Спрос', names='Навык', 
                     hole=0.4, template='plotly_white')
        st.plotly_chart(fig, use_container_width=True)
    
    # Тренды рынка
    st.markdown("### 📊 Тренды рынка во времени")
    trend_data = pd.DataFrame({
        'Дата': pd.date_range(start='2024-01-01', periods=12, freq='M'),
        'Вакансии': [1000, 1100, 1050, 1200, 1350, 1400, 1380, 1450, 1500, 1550, 1600, 1650]
    })
    fig = px.line(trend_data, x='Дата', y='Вакансии', template='plotly_white',
                  markers=True)
    st.plotly_chart(fig, use_container_width=True)
    
    # Географическое распределение
    st.markdown("### 🌍 Географическое распределение")
    geo_data = pd.DataFrame({
        'Город': ['Москва', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Удалённо'],
        'Вакансии': [450, 200, 100, 80, 300]
    })
    fig = px.bar(geo_data, x='Город', y='Вакансии', color='Вакансии',
                 color_continuous_scale='Blues', template='plotly_white')
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()