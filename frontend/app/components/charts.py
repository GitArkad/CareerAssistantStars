"""
Компоненты для визуализации данных (графики)
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import List, Dict, Optional


def render_salary_chart(data: pd.DataFrame, title: str = "Динамика зарплат"):
    """
    Рендерит график динамики зарплат
    
    Args:
        data: DataFrame с колонками ['date', 'salary']
        title: Заголовок графика
    """
    fig = px.line(
        data, 
        x='date', 
        y='salary',
        title=title,
        markers=True,
        template='plotly_white'
    )
    
    fig.update_traces(
        line=dict(color='#667eea', width=3),
        marker=dict(size=8, color='#764ba2')
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Arial", size=12),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_skills_chart(skills_data: Dict[str, int], title: str = "Востребованные навыки"):
    """
    Рендерит круговую диаграмму навыков
    
    Args:
        skills_data: Словарь {навык: количество}
        title: Заголовок графика
    """
    df = pd.DataFrame({
        'Навык': list(skills_data.keys()),
        'Количество': list(skills_data.values())
    })
    
    fig = px.pie(
        df,
        values='Количество',
        names='Навык',
        title=title,
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_trend_chart(data: pd.DataFrame, title: str = "Тренд"):
    """
    Рендерит график тренда с областями
    
    Args:
        data: DataFrame с колонками ['date', 'value']
        title: Заголовок графика
    """
    fig = px.area(
        data,
        x='date',
        y='value',
        title=title,
        template='plotly_white'
    )
    
    fig.update_traces(
        fillcolor='rgba(102, 126, 234, 0.3)',
        line=dict(color='#667eea', width=2)
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.05)')
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_match_distribution(scores: List[float]):
    """
    Рендерит распределение совпадений с вакансиями
    
    Args:
        scores: Список процентов совпадения
    """
    # Создаем категории
    categories = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
    counts = [0, 0, 0, 0, 0]
    
    for score in scores:
        if score < 20:
            counts[0] += 1
        elif score < 40:
            counts[1] += 1
        elif score < 60:
            counts[2] += 1
        elif score < 80:
            counts[3] += 1
        else:
            counts[4] += 1
    
    df = pd.DataFrame({
        'Диапазон': categories,
        'Количество': counts,
        'Цвет': ['#ef4444', '#f97316', '#f59e0b', '#3b82f6', '#10b981']
    })
    
    fig = px.bar(
        df,
        x='Диапазон',
        y='Количество',
        color='Цвет',
        title='Распределение совпадений с вакансиями',
        template='plotly_white',
        color_discrete_map={color: color for color in df['Цвет']}
    )
    
    fig.update_layout(
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_comparison_chart(data: Dict, title: str = "Сравнение"):
    """
    Рендерит сравнительную диаграмму
    
    Args:
        data: Словарь с данными для сравнения
        title: Заголовок графика
    """
    df = pd.DataFrame({
        'Категория': list(data.keys()),
        'Значение': list(data.values())
    })
    
    fig = px.bar(
        df,
        x='Категория',
        y='Значение',
        title=title,
        color='Значение',
        color_continuous_scale='Blues',
        template='plotly_white'
    )
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        coloraxis_showscale=False
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_progress_gauge(value: float, max_value: float = 100, title: str = "Прогресс"):
    """
    Рендерит индикатор прогресса (спидометр)
    
    Args:
        value: Текущее значение
        max_value: Максимальное значение
        title: Заголовок
    """
    percentage = (value / max_value) * 100
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 24}},
        delta={'reference': max_value * 0.7, 'increasing': {'color': "#10b981"}},
        gauge={
            'axis': {'range': [None, max_value], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': "#667eea"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, max_value * 0.4], 'color': '#fee2e2'},
                {'range': [max_value * 0.4, max_value * 0.7], 'color': '#fef3c7'},
                {'range': [max_value * 0.7, max_value], 'color': '#d1fae5'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, use_container_width=True)


def render_timeline_chart(events: List[Dict], title: str = "Хронология"):
    """
    Рендерит временную шкалу событий
    
    Args:
        events: Список событий [{'date': '2024-01-01', 'event': 'Событие'}, ...]
        title: Заголовок графика
    """
    df = pd.DataFrame(events)
    
    fig = px.scatter(
        df,
        x='date',
        y=[1] * len(events),
        text='event',
        title=title,
        template='plotly_white'
    )
    
    fig.update_traces(
        marker=dict(size=12, color='#667eea'),
        textposition="top center"
    )
    
    fig.update_layout(
        showlegend=False,
        yaxis=dict(showticklabels=False, showgrid=False),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    st.plotly_chart(fig, use_container_width=True)