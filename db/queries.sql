-- db/queries.sql
-- Useful manual checks for ingestion, quality, ETL, and analytics.

-- =========================================================
-- 1. TOTAL CURATED JOBS
-- =========================================================
SELECT COUNT(*) AS total_jobs
FROM jobs_curated;

-- =========================================================
-- 2. JOBS BY SOURCE
-- =========================================================
SELECT source, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY source
ORDER BY jobs_count DESC, source;

-- =========================================================
-- 3. JOBS BY COUNTRY
-- =========================================================
SELECT country, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY country
ORDER BY jobs_count DESC NULLS LAST, country;

-- =========================================================
-- 4. SENIORITY DISTRIBUTION
-- =========================================================
SELECT seniority_normalized, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY seniority_normalized
ORDER BY jobs_count DESC, seniority_normalized;

-- =========================================================
-- 5. JOBS WITHOUT TITLE
-- =========================================================
SELECT COUNT(*) AS jobs_without_title
FROM jobs_curated
WHERE title IS NULL OR btrim(title) = '';

-- =========================================================
-- 6. JOBS WITHOUT DESCRIPTION
-- =========================================================
SELECT COUNT(*) AS jobs_without_description
FROM jobs_curated
WHERE description IS NULL OR btrim(description) = '';

-- =========================================================
-- 7. JOBS WITHOUT URL
-- =========================================================
SELECT COUNT(*) AS jobs_without_url
FROM jobs_curated
WHERE url IS NULL OR btrim(url) = '';

-- =========================================================
-- 8. SUSPICIOUS DUPLICATES BY SOURCE + URL
-- =========================================================
SELECT source, url, COUNT(*) AS duplicate_count
FROM jobs_curated
WHERE url IS NOT NULL AND btrim(url) <> ''
GROUP BY source, url
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source, url;

-- =========================================================
-- 9. INVALID SALARY RANGE
-- =========================================================
SELECT job_id, source, title, salary_from, salary_to, currency
FROM jobs_curated
WHERE salary_from IS NOT NULL
  AND salary_to IS NOT NULL
  AND salary_from > salary_to;

-- =========================================================
-- 10. TOP-20 COMPANIES BY JOB COUNT
-- =========================================================
SELECT company_name, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE company_name IS NOT NULL AND btrim(company_name) <> ''
GROUP BY company_name
ORDER BY jobs_count DESC, company_name
LIMIT 20;

-- =========================================================
-- 11. LAST 20 CURATED JOBS
-- =========================================================
SELECT job_id, source, title, company_name, country, city, created_at
FROM jobs_curated
ORDER BY created_at DESC
LIMIT 20;

-- =========================================================
-- 12. TOP-20 EXTRACTED SKILLS
-- =========================================================
SELECT skill, COUNT(*) AS jobs_count
FROM (
    SELECT unnest(skills_extracted) AS skill
    FROM jobs_curated
    WHERE COALESCE(array_length(skills_extracted, 1), 0) > 0
) s
GROUP BY skill
ORDER BY jobs_count DESC, skill
LIMIT 20;

-- =========================================================
-- 13. TOP-20 NORMALIZED SKILLS
-- =========================================================
SELECT skill, COUNT(*) AS jobs_count
FROM (
    SELECT unnest(skills_normalized) AS skill
    FROM jobs_curated
    WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0
) s
GROUP BY skill
ORDER BY jobs_count DESC, skill
LIMIT 20;

-- =========================================================
-- 14. REMOTE TYPE DISTRIBUTION
-- =========================================================
SELECT remote_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY remote_type
ORDER BY jobs_count DESC NULLS LAST, remote_type;

-- =========================================================
-- 15. EMPLOYMENT TYPE DISTRIBUTION
-- =========================================================
SELECT employment_type, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY employment_type
ORDER BY jobs_count DESC NULLS LAST, employment_type;

-- =========================================================
-- 16. ETL RUNS (LAST 50)
-- =========================================================
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

-- =========================================================
-- 17. FAILED ETL RUNS
-- =========================================================
SELECT id, pipeline_name, dag_id, source, status, error_message, started_at, finished_at
FROM etl_runs
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 50;

-- =========================================================
-- 18. RAW RECORDS BY FETCH DATE
-- =========================================================
SELECT 'ingestion_manifest' AS layer, COUNT(*) AS row_count FROM ingestion_manifest
UNION ALL
SELECT 'jobs_curated' AS layer, COUNT(*) AS row_count FROM jobs_curated
UNION ALL
SELECT 'etl_runs' AS layer, COUNT(*) AS row_count FROM etl_runs;

-- =========================================================
-- 19. CURATED RECORDS BY CREATED DATE
-- =========================================================
SELECT DATE(created_at) AS created_date, COUNT(*) AS jobs_count
FROM jobs_curated
WHERE created_at IS NOT NULL
GROUP BY DATE(created_at)
ORDER BY created_date DESC;

-- =========================================================
-- 20. RANDOM SPOT CHECK (10 JOBS)
-- =========================================================
SELECT job_id, source, title, company_name, country, seniority_normalized, url
FROM jobs_curated
ORDER BY RANDOM()
LIMIT 10;

-- =========================================================
-- 21. EMBEDDING STATUS CHECK
-- =========================================================
SELECT embedding_status, COUNT(*) AS jobs_count
FROM jobs_curated
GROUP BY embedding_status
ORDER BY jobs_count DESC, embedding_status;

-- =========================================================
-- 22. ROLE FLAGS CHECK
-- =========================================================
SELECT
    COUNT(*) FILTER (WHERE is_data_role) AS data_roles,
    COUNT(*) FILTER (WHERE is_ml_role) AS ml_roles,
    COUNT(*) FILTER (WHERE is_python_role) AS python_roles,
    COUNT(*) FILTER (WHERE is_analyst_role) AS analyst_roles
FROM jobs_curated;

-- =========================================================
-- 23. SALARY COVERAGE BY CURRENCY
-- =========================================================
SELECT currency,
       COUNT(*) AS jobs_count,
       COUNT(*) FILTER (WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary
FROM jobs_curated
GROUP BY currency
ORDER BY jobs_count DESC, currency;

-- =========================================================
-- 24. SALARY AGGREGATES SAMPLE
-- =========================================================
SELECT role, country, seniority, is_remote, currency, p50, avg_salary, sample_size, updated_at
FROM salary_aggregates
ORDER BY updated_at DESC, sample_size DESC
LIMIT 50;

-- =========================================================
-- 25. EXCHANGE RATES SAMPLE
-- =========================================================
-- Run this only after exchange_rates_init.sql has been applied.
-- If exchange_rates is not created yet, skip this manual check.
SELECT rate_date, base_currency, target_currency, rate
FROM exchange_rates
ORDER BY rate_date DESC, base_currency, target_currency
LIMIT 30;
