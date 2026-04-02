from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append('/opt/airflow/project')
# ==========================================
# 📁 ПУТЬ К ПРОЕКТУ (ВАЖНО!)
# ==========================================
PROJECT_PATH = "/opt/airflow/project"

if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# импорт твоего пайплайна
from pars import run_pipeline

# ==========================================
# ⚙️ НАСТРОЙКИ DAG
# ==========================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),  # защита от зависаний
}

# ==========================================
# 🚀 ФУНКЦИЯ ДЛЯ ЗАПУСКА
# ==========================================
def run_jobs_parser():
    print("🟢 START JOBS PARSER DAG")
    print(f"📅 Time: {datetime.now()}")

    # можно прокинуть env при необходимости
    os.environ["PYTHONUNBUFFERED"] = "1"

    run_pipeline()

    print("✅ FINISHED JOBS PARSER DAG")


# ==========================================
# 📅 DAG
# ==========================================
with DAG(
    dag_id="jobs_parser_weekly",
    default_args=default_args,
    description="Weekly IT jobs parsing pipeline",
    
    schedule_interval="@weekly",   # 🔥 раз в неделю
    start_date=datetime(2024, 1, 1),
    
    catchup=False,                # не догонять прошлые даты
    max_active_runs=1,            # только один запуск
    
    tags=["jobs", "parser", "etl"],
) as dag:

    run_parser = PythonOperator(
        task_id="run_jobs_parser",
        python_callable=run_jobs_parser,
    )

    run_parser  