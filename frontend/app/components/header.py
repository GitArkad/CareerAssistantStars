"""
Компоненты шапки и навигации
"""

import streamlit as st
from datetime import datetime


def render_page_header(title: str, subtitle: str = ""):
    """
    Рендерит красивую шапку страницы с градиентом
    
    Args:
        title: Заголовок страницы
        subtitle: Подзаголовок (опционально)
    """
    st.markdown(f"""
        <div class='page-header'>
            <h1>{title}</h1>
            {f'<p>{subtitle}</p>' if subtitle else ''}
        </div>
    """, unsafe_allow_html=True)


def render_navbar():
    """
    Рендерит навигационную панель
    """
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col1:
        # Логотип
        st.markdown("""
            <div class='logo-container'>
                <span class='logo-icon'>🎯</span>
                <span class='logo-text'>CareerAI</span>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Навигационные ссылки
        nav_style = """
            <style>
            .nav-link {
                color: #667eea;
                text-decoration: none;
                padding: 0.5rem 1rem;
                border-radius: 6px;
                transition: all 0.3s ease;
                font-weight: 500;
            }
            .nav-link:hover {
                background-color: #f0f4ff;
                color: #764ba2;
            }
            </style>
        """
        st.markdown(nav_style, unsafe_allow_html=True)
    
    with col3:
        # Информация о пользователе
        if 'user_name' in st.session_state:
            st.markdown(f"""
                <div class='user-info'>
                    <span class='user-avatar'>👤</span>
                    <span>{st.session_state.user_name}</span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class='user-info'>
                    <span class='user-avatar'>👤</span>
                    <span>Гость</span>
                </div>
            """, unsafe_allow_html=True)


def render_breadcrumb(pages: list):
    """
    Рендерит навигационную цепочку (хлебные крошки)
    
    Args:
        pages: Список страниц [{'name': 'Главная', 'url': '/'}, ...]
    """
    breadcrumb_html = "<div class='breadcrumb'>"
    for i, page in enumerate(pages):
        if i > 0:
            breadcrumb_html += " <span class='breadcrumb-separator'>/</span> "
        if i == len(pages) - 1:
            breadcrumb_html += f"<span class='breadcrumb-current'>{page['name']}</span>"
        else:
            breadcrumb_html += f"<a href='{page['url']}' class='breadcrumb-link'>{page['name']}</a>"
    breadcrumb_html += "</div>"
    
    st.markdown(breadcrumb_html, unsafe_allow_html=True)


def render_status_indicator(status: str, message: str):
    """
    Рендерит индикатор статуса системы
    
    Args:
        status: 'online', 'offline', 'loading'
        message: Текст сообщения
    """
    status_config = {
        'online': {'color': '#10b981', 'icon': '🟢'},
        'offline': {'color': '#ef4444', 'icon': '🔴'},
        'loading': {'color': '#f59e0b', 'icon': '🟡'}
    }
    
    config = status_config.get(status, status_config['offline'])
    
    st.markdown(f"""
        <div class='status-indicator'>
            <span class='status-icon'>{config['icon']}</span>
            <span class='status-text' style='color: {config['color']}'>{message}</span>
        </div>
    """, unsafe_allow_html=True)