# Database files

Эта папка содержит SQL-файлы для PostgreSQL, которые используются для инициализации базы данных проекта.

## Файлы

### `init.sql`
Создает основные таблицы, индексы, ограничения и служебные объекты базы данных.

Основные таблицы:
- `jobs_raw` — сырые вакансии, полученные из парсеров / источников
- `jobs_curated` — очищенные и нормализованные вакансии
- `etl_runs` — логи ETL / pipeline запусков
- `skill_synonyms` — словарь синонимов навыков
- `market_skill_stats` — агрегаты по навыкам
- `salary_aggregates` — агрегаты по зарплатам
- `market_role_stats` — агрегаты по ролям

### `reference_data.sql`
Заполняет таблицу `skill_synonyms` начальными reference-данными для нормализации навыков.

### `views.sql`
Создает SQL views для аналитики и проверки качества данных.

Основные views:
- `v_parsing_stats`
- `v_top_skills`
- `v_top_skills_by_country`
- `v_salary_overview`
- `v_pipeline_counts`
- `v_data_quality_by_source`
- `v_seniority_distribution`

### `queries.sql`
Набор SQL-запросов для ручной проверки данных, ETL и качества загрузки.

---

## Логика пайплайна

Общий поток данных:

1. Парсеры / Airflow получают сырые вакансии из внешних источников
2. Сырые данные сохраняются в `jobs_raw`
3. Очистка и нормализация формируют записи в `jobs_curated`
4. На основе `jobs_curated` рассчитываются агрегаты и аналитические представления

---

## Как применить

### Вариант 1. Через docker exec

Если контейнер Postgres называется `project-postgres`:

```bash
docker cp db/init.sql project-postgres:/tmp/init.sql
docker exec -it project-postgres psql -U admin -d ai_career -f /tmp/init.sql

docker cp db/reference_data.sql project-postgres:/tmp/reference_data.sql
docker exec -it project-postgres psql -U admin -d ai_career -f /tmp/reference_data.sql

docker cp db/views.sql project-postgres:/tmp/views.sql
docker exec -it project-postgres psql -U admin -d ai_career -f /tmp/views.sql