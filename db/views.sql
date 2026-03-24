BEGIN;

-- ==========================================
-- Parsing coverage / ETL quality summary
-- ==========================================
CREATE OR REPLACE VIEW v_parsing_stats AS
SELECT
    source,
    country,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL) AS with_salary,
    COUNT(*) FILTER (WHERE remote_type IN ('remote', 'hybrid')) AS remote_or_hybrid_count,
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
        WHERE company IS NOT NULL
          AND btrim(company) <> ''
    ) AS with_company
FROM jobs_curated
GROUP BY source, country;

-- ==========================================
-- Top skills overall
-- ==========================================
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

-- ==========================================
-- Top skills by country
-- ==========================================
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

-- ==========================================
-- Salary overview by country and seniority
-- ==========================================
CREATE OR REPLACE VIEW v_salary_overview AS
SELECT
    country,
    seniority_normalized AS seniority,
    COUNT(*) AS sample_size,
    ROUND(AVG(salary_min)) AS avg_salary_min,
    ROUND(AVG(salary_max)) AS avg_salary_max,
    ROUND(AVG(
        CASE
            WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL
                THEN (salary_min + salary_max) / 2.0
            WHEN salary_min IS NOT NULL
                THEN salary_min
            WHEN salary_max IS NOT NULL
                THEN salary_max
            ELSE NULL
        END
    )) AS avg_salary_midpoint,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY
            CASE
                WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL
                    THEN (salary_min + salary_max) / 2.0
                WHEN salary_min IS NOT NULL
                    THEN salary_min
                WHEN salary_max IS NOT NULL
                    THEN salary_max
                ELSE NULL
            END
    ) AS median_salary_midpoint
FROM jobs_curated
WHERE (salary_min IS NOT NULL AND salary_min > 0)
   OR (salary_max IS NOT NULL AND salary_max > 0)
GROUP BY country, seniority_normalized
HAVING COUNT(*) >= 3
ORDER BY country, seniority;

-- ==========================================
-- Raw vs curated pipeline counts
-- Helps verify that parsing and transformation ran
-- ==========================================
CREATE OR REPLACE VIEW v_pipeline_counts AS
SELECT 'jobs_raw' AS layer, COUNT(*) AS row_count FROM jobs_raw
UNION ALL
SELECT 'jobs_curated' AS layer, COUNT(*) AS row_count FROM jobs_curated
UNION ALL
SELECT 'etl_runs' AS layer, COUNT(*) AS row_count FROM etl_runs;

-- ==========================================
-- Data completeness by source
-- Useful for spotting weak parsers/sources
-- ==========================================
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
            WHERE company IS NOT NULL AND btrim(company) <> ''
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
            WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL
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

-- ==========================================
-- Seniority distribution
-- Useful for market composition checks
-- ==========================================
CREATE OR REPLACE VIEW v_seniority_distribution AS
SELECT
    source,
    country,
    seniority_normalized,
    COUNT(*) AS job_count
FROM jobs_curated
GROUP BY source, country, seniority_normalized
ORDER BY source, country, job_count DESC, seniority_normalized;

COMMIT;