"""
Конфигурация приложения Career Assistant
"""

import os
import streamlit as st
from typing import Optional
from pathlib import Path


def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Безопасно получить значение из secrets.toml или env переменных
    
    Args:
        key: Ключ конфигурации
        default: Значение по умолчанию
    
    Returns:
        Значение конфигурации или default
    """
    # Пробуем streamlit secrets
    try:
        value = st.secrets.get(key)
        if value:
            return str(value)
    except:
        pass
    
    # Пробуем переменные окружения
    env_value = os.getenv(key)
    if env_value:
        return env_value
    
    # Возвращаем дефолт
    return default


# ============================================================================
# КОНФИГУРАЦИЯ СТРАНИЦЫ
# ============================================================================

PAGE_CONFIG = {
    "page_title": "Career Assistant Pro",
    "page_icon": "🎯",
    "layout": "wide",
    "initial_sidebar_state": "expanded",
}


# ============================================================================
# API НАСТРОЙКИ
# ============================================================================

API_BASE_URL = get_secret("API_BASE_URL", "http://backend:8000")
API_TIMEOUT = int(get_secret("API_TIMEOUT", "30"))
API_RETRY_COUNT = int(get_secret("API_RETRY_COUNT", "3"))


# ============================================================================
# LANGGRAPH / LLM НАСТРОЙКИ
# ============================================================================

OPENAI_API_KEY = get_secret("OPENAI_API_KEY")
ANTHROPIC_API_KEY = get_secret("ANTHROPIC_API_KEY")
LANGGRAPH_ENDPOINT = get_secret("LANGGRAPH_ENDPOINT", "http://localhost:8501")
LLM_MODEL = get_secret("LLM_MODEL", "gpt-4")
LLM_TEMPERATURE = float(get_secret("LLM_TEMPERATURE", "0.7"))


# ============================================================================
# БАЗА ДАННЫХ
# ============================================================================

DATABASE_URL = get_secret("DATABASE_URL", "postgresql://localhost:5432/career_db")
DATABASE_POOL_SIZE = int(get_secret("DATABASE_POOL_SIZE", "10"))
DATABASE_MAX_OVERFLOW = int(get_secret("DATABASE_MAX_OVERFLOW", "20"))


# ============================================================================
# VEKTORНАЯ БАЗА (QDrant)
# ============================================================================

QDRANT_URL = get_secret("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = get_secret("QDRANT_API_KEY")
QDRANT_COLLECTION = get_secret("QDRANT_COLLECTION", "vacancies")


# ============================================================================
# AIRFLOW НАСТРОЙКИ
# ============================================================================

AIRFLOW_URL = get_secret("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USERNAME = get_secret("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = get_secret("AIRFLOW_PASSWORD")


# ============================================================================
# НАСТРОЙКИ ПРИЛОЖЕНИЯ
# ============================================================================

APP_ENV = get_secret("APP_ENV", "development")  # development, staging, production
DEBUG = get_secret("DEBUG", "true").lower() == "true"
LOG_LEVEL = get_secret("LOG_LEVEL", "INFO")
MAX_UPLOAD_SIZE_MB = int(get_secret("MAX_UPLOAD_SIZE_MB", "10"))


# ============================================================================
# ФЛАГИ ФУНКЦИОНАЛЬНОСТИ
# ============================================================================

FEATURES = {
    "enable_resume_upload": True,
    "enable_interview_simulator": True,
    "enable_market_analytics": True,
    "enable_job_matching": True,
    "enable_profile_management": True,
    "enable_pdf_export": True,
}


# ============================================================================
# ПУТИ И ДИРЕКТОРИИ
# ============================================================================

BASE_DIR = Path(__file__).parent.parent
APP_DIR = Path(__file__).parent
UPLOAD_DIR = APP_DIR / "uploads"
TEMP_DIR = APP_DIR / "temp"

# Создаём директории если не существуют
UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)


# ============================================================================
# ЛИМИТЫ И ОГРАНИЧЕНИЯ
# ============================================================================

LIMITS = {
    "max_resumes_per_user": 10,
    "max_vacancies_display": 50,
    "max_interview_questions": 20,
    "session_timeout_minutes": 60,
    "rate_limit_per_minute": 30,
}


# ============================================================================
# СТИЛИ И ТЕКСТЫ
# ============================================================================

APP_NAME = "Career Assistant Pro"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Платформа развития карьеры на базе ИИ"

SUPPORT_EMAIL = "support@careerassistant.ru"
CONTACT_TELEGRAM = "@career_assistant_bot"


# ============================================================================
# ЦВЕТА И ТЕМЫ
# ============================================================================

THEME_COLORS = {
    "primary": "#667eea",
    "secondary": "#764ba2",
    "success": "#10b981",
    "warning": "#f59e0b",
    "error": "#ef4444",
    "info": "#3b82f6",
}


# ============================================================================
# ПРОВЕРКА КОНФИГУРАЦИИ
# ============================================================================

def validate_config() -> bool:
    """
    Проверить корректность конфигурации
    
    Returns:
        True если все обязательные настройки присутствуют
    """
    required_secrets = []  # Добавьте обязательные ключи если нужны
    
    missing = []
    for secret in required_secrets:
        if not get_secret(secret):
            missing.append(secret)
    
    if missing:
        print(f"⚠️ Отсутствуют обязательные настройки: {missing}")
        return False
    
    return True


def print_config_summary():
    """Вывести сводку конфигурации (для отладки)"""
    print("=" * 60)
    print("📋 Career Assistant - Конфигурация")
    print("=" * 60)
    print(f"🌍 Environment: {APP_ENV}")
    print(f"🐛 Debug Mode: {DEBUG}")
    print(f"🔗 API URL: {API_BASE_URL}")
    print(f"🤖 LLM Model: {LLM_MODEL}")
    print(f"💾 Database: {'✅' if DATABASE_URL else '❌'}")
    print(f"🎯 QDrant: {'✅' if QDRANT_URL else '❌'}")
    print(f"📊 Airflow: {'✅' if AIRFLOW_URL else '❌'}")
    print("=" * 60)


# Запускаем проверку при импорте (только в debug режиме)
if DEBUG:
    try:
        validate_config()
    except Exception as e:
        print(f"⚠️ Warning: Config validation failed: {e}")