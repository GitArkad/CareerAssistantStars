-- SQL-проверки для контроля загрузки и качества данных

-- Общее число вакансий
SELECT COUNT(*) AS total_jobs
FROM jobs_curated;

-- Распределение вакансий по источникам
SELECT source, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY source
ORDER BY jobs_count DESC, source;

-- Распределение вакансий по странам
SELECT country, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY country
ORDER BY jobs_count DESC NULLS LAST, country;

-- Распределение вакансий по seniority
SELECT seniority_normalized, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY seniority_normalized
ORDER BY jobs_count DESC, seniority_normalized;

-- Покрытие новых полей
SELECT
    COUNT(*) FILTER (WHERE specialty IS NOT NULL AND btrim(specialty) <> '') AS with_specialty,
    COUNT(*) FILTER (WHERE specialty_category IS NOT NULL AND btrim(specialty_category) <> '') AS with_specialty_category,
    COUNT(*) FILTER (WHERE title_normalized IS NOT NULL AND btrim(title_normalized) <> '') AS with_title_normalized,
    COUNT(*) FILTER (WHERE posting_language IS NOT NULL AND btrim(posting_language) <> '') AS with_posting_language
FROM jobs_curated;

-- Распределение по specialty
SELECT specialty, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE specialty IS NOT NULL AND btrim(specialty) <> ''
GROUP BY specialty
ORDER BY jobs_count DESC, specialty
LIMIT 50;

-- Распределение по specialty_category
SELECT specialty_category, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE specialty_category IS NOT NULL AND btrim(specialty_category) <> ''
GROUP BY specialty_category
ORDER BY jobs_count DESC, specialty_category;

-- Распределение по posting_language
SELECT posting_language, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY posting_language
ORDER BY jobs_count DESC NULLS LAST, posting_language;

-- Проверка вакансий без title
SELECT COUNT(*) AS jobs_without_title
FROM jobs_curated
WHERE title IS NULL OR btrim(title) = '';

-- Проверка вакансий без title_normalized
SELECT COUNT(*) AS jobs_without_title_normalized
FROM jobs_curated
WHERE title_normalized IS NULL OR btrim(title_normalized) = '';

-- Проверка вакансий без description
SELECT COUNT(*) AS jobs_without_description
FROM jobs_curated
WHERE description IS NULL OR btrim(description) = '';

-- Проверка вакансий без URL
SELECT COUNT(*) AS jobs_without_url
FROM jobs_curated
WHERE url IS NULL OR btrim(url) = '';

-- Поиск возможных дублей по source + url
SELECT source, url, COUNT(*) AS duplicate_count
FROM jobs_curated
WHERE url IS NOT NULL AND btrim(url) <> ''
GROUP BY source, url
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source, url;

-- Проверка некорректного диапазона зарплаты
SELECT job_id, source, title, salary_from, salary_to, currency
FROM jobs_curated
WHERE salary_from IS NOT NULL
  AND salary_to IS NOT NULL
  AND salary_from > salary_to;

-- Подозрительно большой опыт
SELECT job_id,
       source,
       title,
       company_name,
       years_experience_min,
       years_experience_max,
       LEFT(description, 300) AS description_preview,
       url
FROM jobs_curated
WHERE (years_experience_min IS NOT NULL AND years_experience_min > 25)
   OR (years_experience_max IS NOT NULL AND years_experience_max > 25)
ORDER BY GREATEST(
    COALESCE(years_experience_min, 0),
    COALESCE(years_experience_max, 0)
) DESC
LIMIT 100;

-- Подозрительный опыт без явного experience-контекста 
SELECT job_id,
       source,
       title,
       company_name,
       years_experience_min,
       years_experience_max,
       LEFT(description, 400) AS description_preview,
       url
FROM jobs_curated
WHERE (years_experience_min IS NOT NULL OR years_experience_max IS NOT NULL)
  AND COALESCE(description, '') !~* '(experience|required|requirements|must have|minimum|at least|commercial experience)'
ORDER BY created_at DESC
LIMIT 100;

-- Явно подозрительные company-history кейсы
SELECT job_id,
       source,
       title,
       company_name,
       years_experience_min,
       years_experience_max,
       LEFT(description, 400) AS description_preview,
       url
FROM jobs_curated
WHERE (years_experience_min IS NOT NULL OR years_experience_max IS NOT NULL)
  AND COALESCE(description, '') ~* '(company|history|heritage|founded|since [0-9]{4}|for over [0-9]+ years|for more than [0-9]+ years)'
ORDER BY created_at DESC
LIMIT 100;

-- Быстрая сводка по опыту
SELECT
    COUNT(*) FILTER (WHERE years_experience_min IS NOT NULL OR years_experience_max IS NOT NULL) AS with_experience,
    COUNT(*) FILTER (WHERE years_experience_min > 25 OR years_experience_max > 25) AS suspicious_gt_25,
    COUNT(*) FILTER (
        WHERE (years_experience_min IS NOT NULL OR years_experience_max IS NOT NULL)
          AND COALESCE(description, '') ~* '(company|history|heritage|founded|since [0-9]{4}|for over [0-9]+ years|for more than [0-9]+ years)'
    ) AS suspicious_company_history_context
FROM jobs_curated;

-- Проверка некорректного диапазона опыта
SELECT job_id, source, title, years_experience_min, years_experience_max
FROM jobs_curated
WHERE years_experience_min IS NOT NULL
  AND years_experience_max IS NOT NULL
  AND years_experience_min > years_experience_max;

-- Вакансии с опытом, но без specialty
SELECT job_id, source, title, years_experience_min, years_experience_max
FROM jobs_curated
WHERE (years_experience_min IS NOT NULL OR years_experience_max IS NOT NULL)
  AND (specialty IS NULL OR btrim(specialty) = '')
LIMIT 50;

-- Топ-20 компаний по числу вакансий
SELECT company_name, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE company_name IS NOT NULL AND btrim(company_name) <> ''
GROUP BY company_name
ORDER BY jobs_count DESC, company_name
LIMIT 20;

-- Последние 20 загруженных вакансий
SELECT job_id, source, title, specialty, company_name, country, city, created_at
FROM jobs_curated
ORDER BY created_at DESC
LIMIT 20;

-- Топ-20 нормализованных навыков
SELECT skill, COUNT(*) AS jobs_count
FROM (
    SELECT unnest(skills_normalized) AS skill
    FROM jobs_curated
    WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0
) s
GROUP BY skill
ORDER BY jobs_count DESC, skill
LIMIT 20;

-- Распределение по remote_type
SELECT remote_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY remote_type
ORDER BY jobs_count DESC NULLS LAST, remote_type;

-- Распределение по employment_type
SELECT employment_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY employment_type
ORDER BY jobs_count DESC NULLS LAST, employment_type;

-- Последние 50 запусков ETL
SELECT id,
       pipeline_name,
       dag_id,
       run_date,
       source,
       status,
       started_at,
       finished_at,
       jobs_extracted,
       jobs_new_raw,
       jobs_processed_raw,
       jobs_curated_inserted,
       jobs_curated_updated,
       jobs_duplicates,
       embeddings_created,
       aggregates_updated,
       duration_sec
FROM etl_runs
ORDER BY started_at DESC
LIMIT 50;

-- Последние 50 упавших ETL-запусков
SELECT id, pipeline_name, dag_id, source, status, error_message, started_at, finished_at
FROM etl_runs
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 50;

-- Быстрая сверка числа строк по ключевым таблицам
SELECT 'ingestion_manifest' AS layer, COUNT(*) AS row_count FROM ingestion_manifest
UNION ALL
SELECT 'jobs_curated' AS layer, COUNT(*) AS row_count FROM jobs_curated
UNION ALL
SELECT 'etl_runs' AS layer, COUNT(*) AS row_count FROM etl_runs;

-- Динамика загрузки вакансий по created_at
SELECT DATE(created_at) AS created_date, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE created_at IS NOT NULL
GROUP BY DATE(created_at)
ORDER BY created_date DESC;

-- Случайная выборка вакансий для spot check
SELECT job_id, source, title, specialty, company_name, country, seniority_normalized, url
FROM jobs_curated
ORDER BY RANDOM()
LIMIT 10;

-- Проверка статусов эмбеддингов
SELECT embedding_status, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY embedding_status
ORDER BY jobs_count DESC, embedding_status;

-- Проверка role-флагов
SELECT
    COUNT(*) FILTER (WHERE is_data_role) AS data_roles,
    COUNT(*) FILTER (WHERE is_ml_role) AS ml_roles,
    COUNT(*) FILTER (WHERE is_python_role) AS python_roles,
    COUNT(*) FILTER (WHERE is_analyst_role) AS analyst_roles
FROM jobs_curated;

-- Покрытие зарплат по валютам
SELECT currency,
       COUNT(*) AS jobs_count,
       COUNT(*) FILTER (WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary
FROM jobs_curated
GROUP BY currency
ORDER BY jobs_count DESC, currency;

-- Пример данных из salary_aggregates
SELECT role, country, seniority, is_remote, currency, p50, avg_salary, sample_size, updated_at
FROM salary_aggregates
ORDER BY updated_at DESC, sample_size DESC
LIMIT 50;

-- Пример данных из exchange_rates
SELECT rate_date, base_currency, target_currency, rate
FROM exchange_rates
ORDER BY rate_date DESC, base_currency, target_currency
LIMIT 30;