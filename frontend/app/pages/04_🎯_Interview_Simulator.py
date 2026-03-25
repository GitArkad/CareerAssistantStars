# app/pages/05_🎯_Симулятор_интервью.py
import streamlit as st
from streamlit_chat import message
import json
from utils.style_loader import apply_custom_styles  # ← ДОБАВИТЬ

# Применяем стили
apply_custom_styles()
st.set_page_config(page_title="Симулятор интервью", page_icon="🎯", layout="wide")

##############
# LANGGRAPH INTERVIEW GRAPH PLACEHOLDER
# Этот раздел использует LangGraph для симуляции интервью
# Граф обрабатывает:
# - Генерацию вопросов на основе требований вакансии
# - Оценку ответов кандидата
# - Адаптивную настройку сложности
# - Генерацию обратной связи
##############

def generate_interview_questions(job_title, experience_level, tech_stack):
    """
    Сгенерировать вопросы для интервью с использованием LangGraph
    """
    # TODO: Реализовать генератор вопросов через LangGraph
    # Должен использовать граф интервью из оркестратора
    pass

def evaluate_answer(question, answer, expected_skills):
    """
    Оценить ответ кандидата с использованием LLM через LangGraph
    """
    # TODO: Реализовать оценку ответов
    pass

##############
# END LANGGRAPH PLACEHOLDER
##############

def main():
    st.markdown("""
        <div class='page-header'>
            <h1>🎯 Симулятор интервью с ИИ</h1>
            <p>Практикуйтесь с вопросами, подобранными ИИ под вашу целевую позицию</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Настройки
    with st.expander("⚙️ Настройки интервью", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            job_title = st.selectbox("Должность", 
                ["Разработчик ПО", "Data Scientist", "DevOps-инженер", "Product Manager"])
        with col2:
            experience = st.selectbox("Уровень опыта",
                ["Junior", "Middle", "Senior", "Lead"])
        with col3:
            interview_type = st.selectbox("Тип интервью",
                ["Техническое", "Поведенческое", "Системный дизайн", "Смешанное"])
        
        tech_stack = st.multiselect("Технологический стек",
            ["Python", "JavaScript", "SQL", "Docker", "Kubernetes", "AWS", "React"])
    
    # Инициализация session state для чата
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None
    if 'score' not in st.session_state:
        st.session_state.score = 0
    if 'question_count' not in st.session_state:
        st.session_state.question_count = 0
    
    # Кнопка начала интервью
    if st.button("🎬 Начать интервью", type="primary"):
        st.session_state.messages = []
        st.session_state.question_count = 0
        st.session_state.score = 0
        st.session_state.interview_active = True
        
        # Сгенерировать первый вопрос
        ##############
        # LANGGRAPH INTEGRATION
        questions = generate_interview_questions(job_title, experience, tech_stack)
        st.session_state.current_question = questions[0] if questions else "Расскажите о себе"
        ##############
        
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"Добро пожаловать на интервью на позицию {job_title}! Начнём.\n\n**Вопрос:** {st.session_state.current_question}"
        })
        st.rerun()
    
    # Отображение сообщений чата
    if st.session_state.get('interview_active', False):
        for i, msg in enumerate(st.session_state.messages):
            message(msg["content"], is_user=msg["role"] == "user", key=f"msg_{i}")
        
        # Ввод пользователя
        if prompt := st.chat_input("Напишите ваш ответ..."):
            # Добавить сообщение пользователя
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            # Оценить ответ
            ##############
            # LANGGRAPH EVALUATION
            evaluation = evaluate_answer(
                st.session_state.current_question,
                prompt,
                tech_stack
            )
            ##############
            
            # Сгенерировать следующий вопрос
            ##############
            # LANGGRAPH NEXT QUESTION
            next_question = generate_interview_questions(job_title, experience, tech_stack)[1]
            ##############
            
            st.session_state.current_question = next_question
            st.session_state.question_count += 1
            
            # Добавить обратную связь
            feedback = f"\n\n**Обратная связь:** {evaluation.get('feedback', 'Хороший ответ!')}\n\n**Следующий вопрос:** {next_question}"
            st.session_state.messages.append({"role": "assistant", "content": feedback})
            
            st.rerun()
        
        # Завершить интервью
        if st.button("Завершить интервью"):
            st.session_state.interview_active = False
            st.success(f"Интервью завершено! Всего вопросов: {st.session_state.question_count}")

if __name__ == "__main__":
    main()