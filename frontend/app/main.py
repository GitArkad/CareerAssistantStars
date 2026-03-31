# app/main.py
import streamlit as st
import streamlit_antd_components as sac
from datetime import datetime
from pathlib import Path

from config import PAGE_CONFIG, API_BASE_URL
from styles import get_custom_css

from utils.style_loader import apply_custom_styles  # ← ДОБАВИТЬ ЭТУ СТРОКУ
# Применяем стили
apply_custom_styles()
# Настройка страницы
st.set_page_config(**PAGE_CONFIG)

# Подключение кастомных стилей
st.markdown(get_custom_css(), unsafe_allow_html=True)

# Пути к файлам
BASE_DIR = Path(__file__).parent.parent  # frontend/
ASSETS_DIR = BASE_DIR / "assets"
LOGO_PATH = ASSETS_DIR / "logo.png"
# Инициализация session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user_data' not in st.session_state:
    st.session_state.user_data = {}
if 'resume_data' not in st.session_state:
    st.session_state.resume_data = None
    


def render_header():
    """Рендерит шапку приложения"""
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col1:
        # ✅ Используем правильный путь к логотипу
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=80)
        else:
            # Фоллбэк на эмодзи если файл не найден
            st.markdown("""
                <div style='
                    width: 80px;
                    height: 80px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    border-radius: 15px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 2.5rem;
                    color: white;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                    margin: 0 auto;
                '>
                    🎯
                </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.title("Career Assistant Pro")
        st.markdown("*Платформа развития карьеры на базе ИИ*")
    
    with col3:
        if st.session_state.authenticated:
            st.write(f"👤 {st.session_state.user_data.get('name', 'Пользователь')}")
            if st.button("Выйти"):
                st.session_state.authenticated = False
                st.rerun()


def render_sidebar():
    """Рендерит боковую панель навигации"""
    with st.sidebar:
        st.markdown("### 📋 Навигация")
        
        menu_items = [
            sac.MenuItem('📄 Загрузка резюме', icon='upload'),
            sac.MenuItem('💼 Вакансии', icon='briefcase'),
            sac.MenuItem('📊 Аналитика рынка', icon='graph'),
            sac.MenuItem('🎯 Симулятор интервью', icon='chat'),
            sac.MenuItem('⚙️ Настройки', icon='gear'),
        ]
        
        selected = sac.menu(menu_items, key='main_menu')
        
        st.markdown("---")
        st.markdown("### 📊 Статус системы")
        st.info("✅ API подключено")
        st.caption(f"Последнее обновление: {datetime.now().strftime('%H:%M:%S')}")

def main():
    """Основная функция приложения"""
    render_header()
    # render_sidebar()
    
    # Приветственный блок
    st.markdown("""
        <div class='welcome-banner'>
            <h2>Добро пожаловать в платформу развития карьеры</h2>
            <p>Используйте ИИ для оптимизации резюме, поиска идеальных вакансий и подготовки к интервью</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Метрики дашборда
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Оценка резюме", "85/100", "+5%")
    with col2:
        st.metric("Подходящие вакансии", "24", "+3")
    with col3:
        st.metric("Успех на интервью", "78%", "+12%")
    with col4:
        st.metric("Спрос на рынке", "Высокий", "📈")
    
    # Быстрые действия (3 кнопки без профиля)
    st.markdown("### ⚡ Быстрые действия")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📤 Загрузить резюме", use_container_width=True):
            st.switch_page("pages/01_📄_Resume_Upload.py")
    with col2:
        if st.button("🔍 Найти вакансии", use_container_width=True):
            st.switch_page("pages/03_💼_Vacancies.py")
    with col3:
        if st.button("📊 Аналитика", use_container_width=True):
            st.switch_page("pages/04_📊_Analytics.py")
    
    # Недавняя активность
    
    st.markdown("### 📈 Недавняя активность")
    st.markdown("""
        <div class='activity-section'>
            <div class='activity-item'>
                <span class='activity-icon'>✅</span>
                <span class='activity-text'>Резюме проанализировано — 85% совпадение с позициями Senior Developer</span>
                <span class='activity-time'>2 часа назад</span>
            </div>
            <div class='activity-item'>
                <span class='activity-icon'>🎯</span>
                <span class='activity-text'>Найдена новая вакансия — Tech Lead в компании из Fortune 500</span>
                <span class='activity-time'>5 часов назад</span>
            </div>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()