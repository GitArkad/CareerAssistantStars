BEGIN;

-- Аналитика по качеству парсинга и заполненности полей.
CREATE OR REPLACE VIEW v_parsing_stats AS2
SELECT
    source,
    country,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary,
    COUNT(*) FILTER (WHERE remote_type IN ('remote', 'hybrid') OR remote = TRUE) AS remote_or_hybrid_count,
    COUNT(*) FILTER (
        WHERE seniority_normalized IS NOT NULL
          AND seniority_normalized <> 'unknown'
    ) AS with_seniority,
    COUNT(*) FILTER (
        WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0
    ) AS with_skills,
    COUNT(*) FILTER (
        WHERE description IS NOT NULL
          AND btrim(description) <> ''
    ) AS with_description,
    COUNT(*) FILTER (
        WHERE company_name IS NOT NULL
          AND btrim(company_name) <> ''
    ) AS with_company
FROM jobs_curated
GROUP BY source, country;

-- Топ навыков по всей базе.
CREATE OR REPLACE VIEW v_top_skills AS
SELECT
    skill,
    COUNT(*) AS job_count,
    ROUND(
        COUNT(*)::numeric
        / NULLIF((SELECT COUNT(*) FROM jobs_curated), 0)
        * 100,
        1
    ) AS pct_of_all_jobs
FROM jobs_curated
CROSS JOIN LATERAL unnest(skills_normalized) AS skill
GROUP BY skill
ORDER BY job_count DESC, skill;

-- Топ навыков по странам.
CREATE OR REPLACE VIEW v_top_skills_by_country AS
SELECT
    country,
    skill,
    COUNT(*) AS job_count
FROM jobs_curated
CROSS JOIN LATERAL unnest(skills_normalized) AS skill
WHERE country IS NOT NULL
GROUP BY country, skill
ORDER BY country, job_count DESC, skill;

-- Сводка зарплат по стране / seniority / валюте.
CREATE OR REPLACE VIEW v_salary_overview AS
SELECT
    country,
    seniority_normalized AS seniority,
    currency,
    COUNT(*) AS sample_size,
    ROUND(AVG(salary_from)) AS avg_salary_from,
    ROUND(AVG(salary_to)) AS avg_salary_to,
    ROUND(AVG(
        CASE
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                THEN (salary_from + salary_to) / 2.0
            WHEN salary_from IS NOT NULL
                THEN salary_from
            WHEN salary_to IS NOT NULL
                THEN salary_to
            ELSE NULL
        END
    )) AS avg_salary_midpoint,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY
            CASE
                WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                    THEN (salary_from + salary_to) / 2.0
                WHEN salary_from IS NOT NULL
                    THEN salary_from
                WHEN salary_to IS NOT NULL
                    THEN salary_to
                ELSE NULL
            END
    ) AS median_salary_midpoint
FROM jobs_curated
WHERE (salary_from IS NOT NULL AND salary_from > 0)
   OR (salary_to IS NOT NULL AND salary_to > 0)
GROUP BY country, seniority_normalized, currency
HAVING COUNT(*) >= 3
ORDER BY country, seniority, currency;

-- Объём данных по основным слоям
CREATE OR REPLACE VIEW v_layer_counts AS
SELECT 'ingestion_manifest' AS layer, COUNT(*) AS row_count FROM ingestion_manifest
UNION ALL
SELECT 'jobs_curated' AS layer, COUNT(*) AS row_count FROM jobs_curated
UNION ALL
SELECT 'etl_runs' AS layer, COUNT(*) AS row_count FROM etl_runs;


-- Заполненность ключевых полей по источникам
CREATE OR REPLACE VIEW v_data_quality_by_source AS
SELECT
    source,
    COUNT(*) AS total_jobs,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE title IS NOT NULL AND btrim(title) <> ''
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_title,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE description IS NOT NULL AND btrim(description) <> ''
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_description,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE company_name IS NOT NULL AND btrim(company_name) <> ''
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_company,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE country IS NOT NULL AND btrim(country) <> ''
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_country,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_salary,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_skills
FROM jobs_curated
GROUP BY source
ORDER BY total_jobs DESC, source;


-- Распределение вакансий по seniority.
CREATE OR REPLACE VIEW v_seniority_distribution AS
SELECT
    source,
    country,
    seniority_normalized,
    COUNT(*) AS job_count
FROM jobs_curated
GROUP BY source, country, seniority_normalized
ORDER BY source, country, job_count DESC, seniority_normalized;


-- Качество вакансий по компаниям.
CREATE OR REPLACE VIEW v_company_quality AS
SELECT
    company_name,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE description IS NOT NULL AND btrim(description) <> '') AS with_description,
    COUNT(*) FILTER (WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary,
    COUNT(*) FILTER (WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0) AS with_skills
FROM jobs_curated
WHERE company_name IS NOT NULL AND btrim(company_name) <> ''
GROUP BY company_name
ORDER BY total_jobs DESC, company_name;

COMMIT;
