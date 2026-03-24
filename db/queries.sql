-- db/queries.sql
-- Полезные запросы для ручной проверки данных, ETL и аналитики

-- =========================================================
-- 1. ОБЩЕЕ КОЛИЧЕСТВО ВАКАНСИЙ
-- =========================================================
SELECT COUNT(*) AS total_jobs
FROM jobs_curated;

-- =========================================================
-- 2. СКОЛЬКО ВАКАНСИЙ ПО ИСТОЧНИКАМ
-- =========================================================
SELECT source, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY source
ORDER BY jobs_count DESC;

-- =========================================================
-- 3. СКОЛЬКО ВАКАНСИЙ ПО СТРАНАМ
-- =========================================================
SELECT country, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY country
ORDER BY jobs_count DESC NULLS LAST;

-- =========================================================
-- 4. РАСПРЕДЕЛЕНИЕ ПО SENIORITY
-- =========================================================
SELECT seniority_normalized, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY seniority_normalized
ORDER BY jobs_count DESC;

-- =========================================================
-- 5. ВАКАНСИИ БЕЗ TITLE
-- =========================================================
SELECT COUNT(*) AS jobs_without_title
FROM jobs_curated
WHERE title IS NULL OR BTRIM(title) = '';

-- =========================================================
-- 6. ВАКАНСИИ БЕЗ DESCRIPTION
-- =========================================================
SELECT COUNT(*) AS jobs_without_description
FROM jobs_curated
WHERE description IS NULL OR BTRIM(description) = '';

-- =========================================================
-- 7. ВАКАНСИИ БЕЗ URL
-- =========================================================
SELECT COUNT(*) AS jobs_without_url
FROM jobs_curated
WHERE url IS NULL OR BTRIM(url) = '';

-- =========================================================
-- 8. ПОДОЗРИТЕЛЬНЫЕ ДУБЛИ ПО SOURCE + URL
-- =========================================================
SELECT source, url, COUNT(*) AS duplicate_count
FROM jobs_curated
WHERE url IS NOT NULL AND BTRIM(url) <> ''
GROUP BY source, url
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source, url;

-- =========================================================
-- 9. ВАКАНСИИ С НЕВАЛИДНОЙ ЗАРПЛАТОЙ
-- =========================================================
SELECT job_id, source, title, salary_min, salary_max
FROM jobs_curated
WHERE salary_min IS NOT NULL
  AND salary_max IS NOT NULL
  AND salary_min > salary_max;

-- =========================================================
-- 10. ТОП-20 КОМПАНИЙ ПО ЧИСЛУ ВАКАНСИЙ
-- =========================================================
SELECT company, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE company IS NOT NULL AND BTRIM(company) <> ''
GROUP BY company
ORDER BY jobs_count DESC
LIMIT 20;

-- =========================================================
-- 11. ПОСЛЕДНИЕ 20 ВАКАНСИЙ
-- =========================================================
SELECT job_id, source, title, company, country, city, created_at
FROM jobs_curated
ORDER BY created_at DESC
LIMIT 20;

-- =========================================================
-- 12. ТОП-20 ИЗВЛЕЧЕННЫХ НАВЫКОВ
-- =========================================================
SELECT skill, COUNT(*) AS jobs_count
FROM (
    SELECT unnest(skills_extracted) AS skill
    FROM jobs_curated
    WHERE COALESCE(array_length(skills_extracted, 1), 0) > 0
) s
GROUP BY skill
ORDER BY jobs_count DESC
LIMIT 20;

-- =========================================================
-- 13. ТОП-20 НОРМАЛИЗОВАННЫХ НАВЫКОВ
-- =========================================================
SELECT skill, COUNT(*) AS jobs_count
FROM (
    SELECT unnest(skills_normalized) AS skill
    FROM jobs_curated
    WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0
) s
GROUP BY skill
ORDER BY jobs_count DESC
LIMIT 20;

-- =========================================================
-- 14. РАСПРЕДЕЛЕНИЕ ПО REMOTE TYPE
-- =========================================================
SELECT remote_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY remote_type
ORDER BY jobs_count DESC NULLS LAST;

-- =========================================================
-- 15. EMPLOYMENT TYPE
-- =========================================================
SELECT employment_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY employment_type
ORDER BY jobs_count DESC NULLS LAST;

-- =========================================================
-- 16. ETL RUNS
-- =========================================================
SELECT run_id,
       dag_id,
       source,
       status,
       started_at,
       finished_at,
       jobs_extracted,
       jobs_new,
       jobs_duplicates,
       embeddings_created,
       aggregates_updated,
       duration_sec
FROM etl_runs
ORDER BY started_at DESC
LIMIT 50;

-- =========================================================
-- 17. ETL RUNS С ОШИБКАМИ
-- =========================================================
SELECT run_id, dag_id, source, status, error_message, started_at, finished_at
FROM etl_runs
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 50;

-- =========================================================
-- 18. КОЛИЧЕСТВО RAW ЗАПИСЕЙ ПО ДНЯМ ЗАГРУЗКИ
-- =========================================================
SELECT DATE(fetched_at) AS fetched_date, COUNT(*) AS jobs_count
FROM jobs_raw
WHERE fetched_at IS NOT NULL
GROUP BY DATE(fetched_at)
ORDER BY fetched_date DESC;

-- =========================================================
-- 19. КОЛИЧЕСТВО CURATED ЗАПИСЕЙ ПО ДНЯМ СОЗДАНИЯ
-- =========================================================
SELECT DATE(created_at) AS created_date, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE created_at IS NOT NULL
GROUP BY DATE(created_at)
ORDER BY created_date DESC;

-- =========================================================
-- 20. БЫСТРАЯ ПРОВЕРКА 10 СЛУЧАЙНЫХ ВАКАНСИЙ
-- =========================================================
SELECT job_id, source, title, company, country, seniority_normalized, url
FROM jobs_curated
ORDER BY RANDOM()
LIMIT 10;