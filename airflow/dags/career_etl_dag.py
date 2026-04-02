import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task

sys.path.append('/opt/airflow/scripts')

@dag(
    dag_id='career_assistant_multi_site_v2',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['production', 'multi-source']
)
def career_etl():

    @task()
    def scrape_hh():
        from scrapers import fetch_hh_ru
        return fetch_hh_ru(limit=50)

    @task()
    def scrape_habr():
        from scrapers import fetch_habr_career
        return fetch_habr_career(limit=50)

    @task()
    def upload_to_qdrant(file_list):
        from processors import process_multi_source_data
        return process_multi_source_data(file_list)

    # Запускаем сбор параллельно
    hh_file = scrape_hh()
    habr_file = scrape_habr()

    # Ждем оба файла и загружаем
    upload_to_qdrant([hh_file, habr_file])

career_etl_dag = career_etl()