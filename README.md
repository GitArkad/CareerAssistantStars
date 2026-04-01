# 🧠 AI Career Assistant

Интеллектуальная система для анализа рынка труда, подбора вакансий и построения карьерной стратегии на основе резюме пользователя.

Проект объединяет:
- сбор вакансий из разных источников  
- обработку и нормализацию данных  
- векторный поиск (RAG)  
- агентную архитектуру (LangGraph + LLM)  
- API (FastAPI)  
- интерфейс пользователя (Streamlit)  

---

## 🚀 Архитектура проекта

![Architecture](CareerAssistantStars/frontend/assets/logo.png)

### Основные блоки:

1. Сбор данных (Data Ingestion)
2. Обработка и хранение (Processing & Storage)
3. Векторный поиск (Vector Search / RAG)
4. Ядро оркестрации (LangGraph + LLM)
5. Инструменты агента (Agent Tools)
6. Интерфейс пользователя (Streamlit + FastAPI)

---

## 📦 Функциональность

- 📄 Анализ резюме
- 💼 Поиск вакансий
- 🎯 Fit analysis (насколько подходишь)
- 🧭 Roadmap развития
- 🎤 Подготовка к интервью
- 🧠 Семантический поиск по вакансиям (Qdrant)

---

## 🏗️ Стек технологий

### Backend
- FastAPI
- LangGraph
- Llama 3 (через Groq)
- Qdrant (Vector DB)
- PostgreSQL

### Data Pipeline
- Apache Airflow
- Python (ETL)
- S3 / MinIO (object storage)

### Frontend
- Streamlit

### Infra
- Docker
- Docker Compose

---

## 🔄 Pipeline данных

1. Парсинг вакансий:
   - HH.ru
   - Adzuna
   - USAJobs

2. Обработка:
   - очистка данных
   - дедупликация
   - нормализация навыков

3. Хранение:
   - S3 / MinIO → raw + clean data
   - PostgreSQL → curated данные

4. Векторизация:
   - embeddings → Qdrant

5. Поиск:
   - semantic match вакансий и резюме

---

## 🧠 Архитектура агентов

Используется LangGraph:

- Router Node → определяет действие
- Assistant Node (LLM)
- Tools Node:
  - search
  - roadmap
  - interview
  - resume tailoring

---

## ⚙️ Запуск проекта

### 1. Клонирование

```bash
git clone https://github.com/your-repo/ai-career-assistant.git
cd ai-career-assistant
```

### 2. Запуск через Docker

```bash
docker-compose up --build
```

### 3. Backend

```bash
cd backend
uvicorn app.main:app --reload
```

### 4. Frontend

```bash
cd frontend
streamlit run app/main.py
```

---

## 📁 Структура проекта

```
project/
├── backend/
├── frontend/
├── dags/
├── data/
├── docker-compose.yml
└── README.md
```

---

## 👥 Команда

- Слинкова-Албул Олеся — Data Pipeline, Airflow, Docker
- Андрей Дубровин — Backend (RAG, LangGraph, поиск)
- Балакин Данила — Backend (FastAPI), Frontend (Streamlit)
