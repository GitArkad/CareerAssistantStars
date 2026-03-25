# app/pages/01_📄_Загрузка_резюме.py
import streamlit as st
import pandas as pd
from streamlit_pdf_viewer import pdf_viewer
import tempfile
import os
from pathlib import Path

from components.header import render_page_header
from components.resume_card import render_resume_card, render_skill_badge
from config import API_BASE_URL, get_secret
from utils.api_client import APIClient
from utils.style_loader import apply_custom_styles

# Применяем стили
apply_custom_styles()
st.set_page_config(page_title="Загрузка резюме", page_icon="📄", layout="wide")

##############
# LANGGRAPH ORCHESTRATOR PLACEHOLDER
# Этот раздел интегрируется с LangGraph для парсинга и анализа резюме
# Оркестратор LangGraph обрабатывает:
# - Парсинг резюме (PDF/DOC/DOCX/TXT)
# - Извлечение данных (навыки, опыт, образование)
# - Графовый анализ и сопоставление с вакансиями
##############

def parse_resume_with_langgraph(file_content, file_type):
    """
    Отправить резюме в оркестратор LangGraph для парсинга
    Возвращает структурированные данные из графа
    """
    # TODO: Реализовать интеграцию с LangGraph
    # Здесь должен быть вызов FastAPI endpoint, который запускает воркфлоу LangGraph
    pass

def analyze_resume_graph(resume_data):
    """
    Запустить граф анализа на распарсенных данных резюме
    Сравнивает с вакансиями, извлекает навыки, рассчитывает баллы совпадения
    """
    # TODO: Реализовать граф анализа
    pass

##############
# END LANGGRAPH PLACEHOLDER
##############

def main():
    render_page_header("📄 Загрузка и анализ резюме", "Загрузите и проанализируйте своё резюме с помощью ИИ")
    
    api_client = APIClient(st.secrets.get("API_BASE_URL", "http://localhost:8000"))
    
    # Секция загрузки
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### Загрузите ваше резюме")
        uploaded_file = st.file_uploader(
            "Выберите файл",
            type=['pdf', 'doc', 'docx', 'txt'],
            help="Поддерживаемые форматы: PDF, DOC, DOCX, TXT"
        )
        
        if uploaded_file is not None:
            # Отобразить информацию о файле
            st.success(f"✅ Файл загружен: {uploaded_file.name}")
            st.info(f"Размер: {uploaded_file.size / 1024:.2f} КБ")
            
            # Сохранить во временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name
            
            # Предпросмотр для PDF
            if uploaded_file.type == "application/pdf":
                st.markdown("### Предпросмотр")
                try:
                    pdf_viewer(tmp_path)
                except:
                    st.warning("Предпросмотр недоступен")
            
            # Кнопка анализа
            if st.button("🔍 Проанализировать резюме", type="primary", use_container_width=True):
                with st.spinner("🤖 ИИ анализирует ваше резюме..."):
                    try:
                        # Прочитать содержимое файла
                        file_content = uploaded_file.getvalue()
                        
                        # Вызвать оркестратор LangGraph
                        ##############
                        # LANGGRAPH INTEGRATION POINT
                        # Здесь вызывается оркестратор LangGraph, описанный выше
                        ##############
                        resume_data = parse_resume_with_langgraph(file_content, uploaded_file.type)
                        
                        # Сохранить в session state
                        st.session_state.resume_data = resume_data
                        
                        st.success("✅ Резюме проанализировано успешно!")
                        
                        # Показать результаты анализа
                        st.session_state.show_analysis = True
                        
                    except Exception as e:
                        st.error(f"Ошибка при анализе резюме: {str(e)}")
                    finally:
                        os.unlink(tmp_path)
    
    with col2:
        st.markdown("### Результаты анализа")
        
        if 'show_analysis' in st.session_state and st.session_state.show_analysis:
            resume_data = st.session_state.resume_data
            
            # Извлечённая информация
            st.markdown("#### 📋 Извлечённая информация")
            
            tabs = st.tabs(["Навыки", "Опыт работы", "Образование", "Проекты"])
            
            with tabs[0]:
                st.markdown("### 💡 Навыки")
                if 'skills' in resume_data:
                    for skill in resume_data['skills']:
                        st.markdown(f"- {skill}")
                else:
                    st.warning("Навыки не извлечены")
            
            with tabs[1]:
                st.markdown("### 💼 Опыт работы")
                if 'experience' in resume_data:
                    for exp in resume_data['experience']:
                        with st.expander(f"{exp.get('position', 'Должность')} в {exp.get('company', 'Компания')}"):
                            st.write(exp.get('description', ''))
                            st.caption(f"{exp.get('start_date', '')} — {exp.get('end_date', '')}")
            
            with tabs[2]:
                st.markdown("### 🎓 Образование")
                if 'education' in resume_data:
                    for edu in resume_data['education']:
                        st.markdown(f"**{edu.get('degree', '')}**")
                        st.write(edu.get('institution', ''))
                        st.caption(edu.get('year', ''))
            
            with tabs[3]:
                st.markdown("### 🚀 Проекты")
                if 'projects' in resume_data:
                    for proj in resume_data['projects']:
                        st.markdown(f"**{proj.get('name', '')}**")
                        st.write(proj.get('description', ''))
            
            # Оценка резюме
            st.markdown("### 🎯 Оценка резюме")
            score = resume_data.get('overall_score', 0)
            st.progress(score / 100)
            st.write(f"**{score}/100** — {get_score_description(score)}")
            
            # Рекомендации
            st.markdown("### 💡 Рекомендации ИИ")
            if 'recommendations' in resume_data:
                for rec in resume_data['recommendations']:
                    st.info(f"💬 {rec}")
            else:
                st.info("Загрузите и проанализируйте резюме, чтобы получить рекомендации")

def get_score_description(score):
    if score >= 90:
        return "Отлично! Ваше резюме высококонкурентно"
    elif score >= 75:
        return "Хорошо! Требуются небольшие улучшения"
    elif score >= 60:
        return "Удовлетворительно! Рассмотрите значительные обновления"
    else:
        return "Требуется доработка! Рекомендуются серьёзные улучшения"

if __name__ == "__main__":
    main()