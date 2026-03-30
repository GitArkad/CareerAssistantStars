BEGIN;

-- Агрегаты зарплат по роли, стране, seniority, remote и валюте
CREATE TABLE IF NOT EXISTS salary_aggregates_v2 (
    role TEXT NOT NULL,
    country TEXT NOT NULL,
    seniority TEXT NOT NULL,
    is_remote BOOLEAN NOT NULL,
    currency TEXT NOT NULL,
    sample_size INT NOT NULL,
    p25 NUMERIC(14,2) NULL,
    p50 NUMERIC(14,2) NULL,
    p75 NUMERIC(14,2) NULL,
    avg_salary NUMERIC(14,2) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_salary_aggregates_v2
        PRIMARY KEY (role, country, seniority, is_remote, currency)
);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_v2_role_country
    ON salary_aggregates_v2 (role, country, seniority);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_v2_currency
    ON salary_aggregates_v2 (currency);


-- Сводная статистика по рынку для каждой группы вакансий
CREATE TABLE IF NOT EXISTS market_role_stats_v2 (
    role TEXT NOT NULL,
    country TEXT NOT NULL,
    seniority TEXT NOT NULL,
    is_remote BOOLEAN NOT NULL,
    jobs_count INT NOT NULL,
    median_salary NUMERIC(14,2) NULL,
    avg_salary NUMERIC(14,2) NULL,
    competition_proxy NUMERIC(8,2) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_market_role_stats_v2
        PRIMARY KEY (role, country, seniority, is_remote)
);

CREATE INDEX IF NOT EXISTS idx_market_role_stats_v2_role_country
    ON market_role_stats_v2 (role, country, seniority);


-- Статистика по навыкам внутри каждой группы вакансий
CREATE TABLE IF NOT EXISTS market_skill_stats_v2 (
    role TEXT NOT NULL,
    country TEXT NOT NULL,
    seniority TEXT NOT NULL,
    is_remote BOOLEAN NOT NULL,
    skill_id BIGINT NOT NULL REFERENCES skills_dictionary(skill_id) ON DELETE CASCADE,
    jobs_with_skill INT NOT NULL,
    jobs_total INT NOT NULL,
    share_pct NUMERIC(7,2) NOT NULL,
    salary_median_with_skill NUMERIC(14,2) NULL,
    salary_median_without_skill NUMERIC(14,2) NULL,
    salary_delta NUMERIC(14,2) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_market_skill_stats_v2
        PRIMARY KEY (role, country, seniority, is_remote, skill_id)
);

CREATE INDEX IF NOT EXISTS idx_market_skill_stats_v2_skill_id
    ON market_skill_stats_v2 (skill_id);

CREATE INDEX IF NOT EXISTS idx_market_skill_stats_v2_role_country
    ON market_skill_stats_v2 (role, country, seniority);


-- Полный пересчёт агрегатов зарплат
TRUNCATE TABLE salary_aggregates_v2;

INSERT INTO salary_aggregates_v2 (
    role,
    country,
    seniority,
    is_remote,
    currency,
    sample_size,
    p25,
    p50,
    p75,
    avg_salary,
    updated_at
)
SELECT
    COALESCE(NULLIF(BTRIM(specialty), ''), NULLIF(BTRIM(title_normalized), ''), NULLIF(BTRIM(title), ''), 'unknown') AS role,
    COALESCE(NULLIF(BTRIM(country), ''), 'unknown') AS country,
    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), 'unknown') AS seniority,
    COALESCE(remote, remote_type IN ('remote', 'hybrid')) AS is_remote,
    COALESCE(NULLIF(BTRIM(currency), ''), 'unknown') AS currency,
    COUNT(*) AS sample_size,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_midpoint) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_midpoint) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_midpoint) AS p75,
    AVG(salary_midpoint) AS avg_salary,
    NOW() AS updated_at
FROM (
    SELECT
        specialty,
        title_normalized,
        title,
        country,
        seniority_normalized,
        remote,
        remote_type,
        currency,
        CASE
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2.0
            WHEN salary_from IS NOT NULL THEN salary_from
            WHEN salary_to IS NOT NULL THEN salary_to
            ELSE NULL
        END AS salary_midpoint
    FROM jobs_curated
) t
WHERE salary_midpoint IS NOT NULL
GROUP BY 1,2,3,4,5;

-- Полный пересчёт общей статистики по ролям
TRUNCATE TABLE market_role_stats_v2;

WITH base AS (
    SELECT
        COALESCE(NULLIF(BTRIM(specialty), ''), NULLIF(BTRIM(title_normalized), ''), NULLIF(BTRIM(title), ''), 'unknown') AS role,
        COALESCE(NULLIF(BTRIM(country), ''), 'unknown') AS country,
        COALESCE(NULLIF(BTRIM(seniority_normalized), ''), 'unknown') AS seniority,
        COALESCE(remote, remote_type IN ('remote', 'hybrid')) AS is_remote,
        CASE
            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2.0
            WHEN salary_from IS NOT NULL THEN salary_from
            WHEN salary_to IS NOT NULL THEN salary_to
            ELSE NULL
        END AS salary_midpoint,
        COALESCE(years_experience_min, years_experience_max, 1) AS years_exp_proxy
    FROM jobs_curated
)
INSERT INTO market_role_stats_v2 (
    role,
    country,
    seniority,
    is_remote,
    jobs_count,
    median_salary,
    avg_salary,
    competition_proxy,
    updated_at
)
SELECT
    role,
    country,
    seniority,
    is_remote,
    COUNT(*) AS jobs_count,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_midpoint) AS median_salary,
    AVG(salary_midpoint) AS avg_salary,
    ROUND(COUNT(*)::numeric / GREATEST(AVG(NULLIF(years_exp_proxy, 0)), 1), 2) AS competition_proxy,
    NOW() AS updated_at
FROM base
GROUP BY role, country, seniority, is_remote;

-- Полный пересчёт статистики по навыкам
TRUNCATE TABLE market_skill_stats_v2;

WITH base_jobs AS (
    SELECT
        jc.job_id,
        COALESCE(NULLIF(BTRIM(jc.specialty), ''), NULLIF(BTRIM(jc.title_normalized), ''), NULLIF(BTRIM(jc.title), ''), 'unknown') AS role,
        COALESCE(NULLIF(BTRIM(jc.country), ''), 'unknown') AS country,
        COALESCE(NULLIF(BTRIM(jc.seniority_normalized), ''), 'unknown') AS seniority,
        COALESCE(jc.remote, jc.remote_type IN ('remote', 'hybrid')) AS is_remote,
        CASE
            WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN (jc.salary_from + jc.salary_to) / 2.0
            WHEN jc.salary_from IS NOT NULL THEN jc.salary_from
            WHEN jc.salary_to IS NOT NULL THEN jc.salary_to
            ELSE NULL
        END AS salary_midpoint
    FROM jobs_curated jc
),
market_slices AS (
    SELECT
        role,
        country,
        seniority,
        is_remote,
        COUNT(*) AS jobs_total
    FROM base_jobs
    GROUP BY role, country, seniority, is_remote
),
jobs_with_skills AS (
    SELECT
        bj.role,
        bj.country,
        bj.seniority,
        bj.is_remote,
        js.skill_id,
        bj.job_id,
        bj.salary_midpoint
    FROM base_jobs bj
    JOIN job_skills js
        ON js.job_id = bj.job_id
),
with_skill AS (
    SELECT
        role,
        country,
        seniority,
        is_remote,
        skill_id,
        COUNT(DISTINCT job_id) AS jobs_with_skill,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_midpoint) AS salary_median_with_skill
    FROM jobs_with_skills
    GROUP BY role, country, seniority, is_remote, skill_id
),
without_skill AS (
    SELECT
        bj.role,
        bj.country,
        bj.seniority,
        bj.is_remote,
        s.skill_id,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bj.salary_midpoint) AS salary_median_without_skill
    FROM base_jobs bj
    JOIN (
        SELECT DISTINCT role, country, seniority, is_remote, skill_id
        FROM jobs_with_skills
    ) s
      ON s.role = bj.role
     AND s.country = bj.country
     AND s.seniority = bj.seniority
     AND s.is_remote = bj.is_remote
    WHERE NOT EXISTS (
        SELECT 1
        FROM job_skills js
        WHERE js.job_id = bj.job_id
          AND js.skill_id = s.skill_id
    )
    GROUP BY bj.role, bj.country, bj.seniority, bj.is_remote, s.skill_id
)
INSERT INTO market_skill_stats_v2 (
    role,
    country,
    seniority,
    is_remote,
    skill_id,
    jobs_with_skill,
    jobs_total,
    share_pct,
    salary_median_with_skill,
    salary_median_without_skill,
    salary_delta,
    updated_at
)
SELECT
    ws.role,
    ws.country,
    ws.seniority,
    ws.is_remote,
    ws.skill_id,
    ws.jobs_with_skill,
    ms.jobs_total,
    ROUND(100.0 * ws.jobs_with_skill::numeric / NULLIF(ms.jobs_total, 0), 2) AS share_pct,
    ws.salary_median_with_skill,
    wos.salary_median_without_skill,
    CASE
        WHEN ws.salary_median_with_skill IS NOT NULL
         AND wos.salary_median_without_skill IS NOT NULL
            THEN ws.salary_median_with_skill - wos.salary_median_without_skill
        ELSE NULL
    END AS salary_delta,
    NOW() AS updated_at
FROM with_skill ws
JOIN market_slices ms
  ON ms.role = ws.role
 AND ms.country = ws.country
 AND ms.seniority = ws.seniority
 AND ms.is_remote = ws.is_remote
LEFT JOIN without_skill wos
  ON wos.role = ws.role
 AND wos.country = ws.country
 AND wos.seniority = ws.seniority
 AND wos.is_remote = ws.is_remote
 AND wos.skill_id = ws.skill_id;

COMMIT;