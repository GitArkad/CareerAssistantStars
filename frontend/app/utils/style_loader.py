# app/utils/style_loader.py
"""
Загрузчик кастомных CSS стилей
"""

import streamlit as st
import sys
from pathlib import Path

# Добавляем путь к styles.py
sys.path.append(str(Path(__file__).parent.parent.parent))

def apply_custom_styles():
    """
    Применяет кастомные CSS стили к текущей странице
    """
    try:
        from styles import get_custom_css
        custom_css = get_custom_css()
        st.markdown(custom_css, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Ошибка загрузки стилей: {e}")