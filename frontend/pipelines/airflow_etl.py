# pipelines/airflow_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

##############
# AIRFLOW ETL PIPELINE
# Этот пайплайн обрабатывает:
# 1. Парсинг вакансий с нескольких источников (hh.ru, Adzuna и др.)
# 2. Очистку и нормализацию данных
# 3. Генерацию векторных эмбеддингов
# 4. Загрузку в PostgreSQL + QDrant
##############

default_args = {
    'owner': 'career_assistant',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job_market_etl_pipeline',
    default_args=default_args,
    description='ETL-пайплайн для данных рынка труда',
    schedule_interval='0 6 * * *',  # Запуск ежедневно в 6:00
    catchup=False,
)

def scrape_hh_ru(**context):
    """Парсинг данных о вакансиях с hh.ru"""
    # TODO: Реализовать парсинг hh.ru
    # Использовать их API или веб-скрапинг
    api_url = "https://api.hh.ru/vacancies"
    params = {
        'text': 'Python разработчик',
        'area': '1',  # Москва
        'per_page': 100
    }
    response = requests.get(api_url, params=params)
    data = response.json()
    
    # Сохранить в XCom для следующей задачи
    context['task_instance'].xcom_push(key='hh_vacancies', value=data['items'])
    return len(data['items'])

def scrape_adzuna(**context):
    """Парсинг данных о вакансиях с Adzuna"""
    # TODO: Реализовать интеграцию с API Adzuna
    api_id = "{{ var.value.adzuna_api_id }}"
    api_key = "{{ var.value.adzuna_api_key }}"
    
    url = f"https://api.adzuna.com/v1/api/jobs/ru/search/1"
    params = {
        'app_id': api_id,
        'app_key': api_key,
        'what': 'software engineer',
        'results_per_page': 100
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    context['task_instance'].xcom_push(key='adzuna_vacancies', value=data['results'])
    return len(data['results'])

def clean_and_normalize_data(**context):
    """Очистка и нормализация данных из нескольких источников"""
    hh_data = context['task_instance'].xcom_pull(task_ids='scrape_hh_ru', key='hh_vacancies')
    adzuna_data = context['task_instance'].xcom_pull(task_ids='scrape_adzuna', key='adzuna_vacancies')
    
    # Нормализовать к общей схеме
    normalized_vacancies = []
    
    for vac in hh_data:
        normalized = {
            'title': vac.get('name', ''),
            'company': vac.get('employer', {}).get('name', ''),
            'salary_from': vac.get('salary', {}).get('from', 0),
            'salary_to': vac.get('salary', {}).get('to', 0),
            'currency': vac.get('salary', {}).get('currency', 'RUB'),
            'location': vac.get('area', {}).get('name', ''),
            'description': vac.get('description', ''),
            'skills': extract_skills(vac.get('description', '')),
            'source': 'hh.ru',
            'posted_date': vac.get('published_at', '')
        }
        normalized_vacancies.append(normalized)
    
    for vac in adzuna_data:
        normalized = {
            'title': vac.get('title', ''),
            'company': vac.get('company', {}).get('display_name', ''),
            'salary_from': vac.get('salary_min', 0),
            'salary_to': vac.get('salary_max', 0),
            'currency': 'RUB',
            'location': vac.get('location', {}).get('display_name', ''),
            'description': vac.get('description', ''),
            'skills': extract_skills(vac.get('description', '')),
            'source': 'Adzuna',
            'posted_date': vac.get('created', '')
        }
        normalized_vacancies.append(normalized)
    
    context['task_instance'].xcom_push(key='normalized_vacancies', value=normalized_vacancies)
    return len(normalized_vacancies)

def extract_skills(description):
    """Извлечь навыки из описания вакансии"""
    # TODO: Реализовать извлечение навыков с помощью NLP или поиска по ключевым словам
    common_skills = ['Python', 'SQL', 'Docker', 'Kubernetes', 'AWS', 'Git']
    found_skills = [skill for skill in common_skills if skill.lower() in description.lower()]
    return found_skills

def load_to_postgres(**context):
    """Загрузить нормализованные данные в PostgreSQL"""
    vacancies = context['task_instance'].xcom_pull(
        task_ids='clean_and_normalize_data', 
        key='normalized_vacancies'
    )
    
    # TODO: Реализовать вставку в PostgreSQL
    # Использовать psycopg2 или SQLAlchemy
    pass

def generate_embeddings_and_load_qdrant(**context):
    """Сгенерировать векторные эмбеддинги и загрузить в QDrant"""
    vacancies = context['task_instance'].xcom_pull(
        task_ids='clean_and_normalize_data', 
        key='normalized_vacancies'
    )
    
    # TODO: Реализовать генерацию эмбеддингов
    # Использовать sentence-transformers или OpenAI embeddings
    # Загрузить в векторную базу QDrant
    pass

# Определить задачи
scrape_hh_task = PythonOperator(
    task_id='scrape_hh_ru',
    python_callable=scrape_hh_ru,
    dag=dag,
)

scrape_adzuna_task = PythonOperator(
    task_id='scrape_adzuna',
    python_callable=scrape_adzuna,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_and_normalize_data',
    python_callable=clean_and_normalize_data,
    dag=dag,
)

load_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

load_qdrant_task = PythonOperator(
    task_id='generate_embeddings_and_load_qdrant',
    python_callable=generate_embeddings_and_load_qdrant,
    dag=dag,
)

# Определить зависимости задач
[scrape_hh_task, scrape_adzuna_task] >> clean_data_task >> load_postgres_task >> load_qdrant_task

##############
# END AIRFLOW PIPELINE
##############