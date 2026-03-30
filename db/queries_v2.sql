-- Проверки для новых таблиц

-- Базовая сверка числа строк по всем таблицам
SELECT 'skills_dictionary' AS table_name, COUNT(*) AS row_count FROM skills_dictionary
UNION ALL
SELECT 'skill_match_rules' AS table_name, COUNT(*) AS row_count FROM skill_match_rules
UNION ALL
SELECT 'job_skills' AS table_name, COUNT(*) AS row_count FROM job_skills
UNION ALL
SELECT 'salary_aggregates_v2' AS table_name, COUNT(*) AS row_count FROM salary_aggregates_v2
UNION ALL
SELECT 'market_role_stats_v2' AS table_name, COUNT(*) AS row_count FROM market_role_stats_v2
UNION ALL
SELECT 'market_skill_stats_v2' AS table_name, COUNT(*) AS row_count FROM market_skill_stats_v2;


-- Проверки словаря навыков
SELECT skill_id, canonical_name, category, parent_skill_id, is_active
FROM skills_dictionary
ORDER BY canonical_name;

SELECT category, COUNT(*) AS skills_count
FROM skills_dictionary
GROUP BY category
ORDER BY skills_count DESC, category;

SELECT child.canonical_name AS child_skill,
       parent.canonical_name AS parent_skill
FROM skills_dictionary child
LEFT JOIN skills_dictionary parent
       ON parent.skill_id = child.parent_skill_id
ORDER BY child.canonical_name;


-- Проверки правил матчинга
SELECT smr.synonym,
       sd.canonical_name,
       smr.match_type,
       smr.priority,
       smr.is_case_sensitive
FROM skill_match_rules smr
JOIN skills_dictionary sd ON sd.skill_id = smr.skill_id
ORDER BY sd.canonical_name, smr.synonym;

SELECT match_type, COUNT(*) AS rules_count
FROM skill_match_rules
GROUP BY match_type
ORDER BY rules_count DESC, match_type;

-- Дублей по нормализованному synonym быть не должно
SELECT lower(btrim(synonym)) AS synonym_norm, COUNT(*) AS duplicate_count
FROM skill_match_rules
GROUP BY lower(btrim(synonym))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, synonym_norm;


-- Проверки наполнения и качества job_skills
SELECT source_type, COUNT(*) AS row_count
FROM job_skills
GROUP BY source_type
ORDER BY row_count DESC, source_type;

SELECT sd.canonical_name,
       COUNT(*) AS jobs_count
FROM job_skills js
JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
GROUP BY sd.canonical_name
ORDER BY jobs_count DESC, sd.canonical_name
LIMIT 50;

SELECT js.job_id,
       COUNT(*) AS skills_count
FROM job_skills js
GROUP BY js.job_id
ORDER BY skills_count DESC, js.job_id
LIMIT 50;

-- Вакансии без записей в job_skills
SELECT COUNT(*) AS jobs_without_job_skills
FROM jobs_curated jc
WHERE NOT EXISTS (
    SELECT 1
    FROM job_skills js
    WHERE js.job_id = jc.job_id
);

-- Случайная выборка вакансий со скиллами
SELECT jc.job_id,
       jc.title,
       jc.company_name,
       jc.country,
       ARRAY_AGG(sd.canonical_name ORDER BY sd.canonical_name) AS skills
FROM jobs_curated jc
JOIN job_skills js ON js.job_id = jc.job_id
JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
GROUP BY jc.job_id, jc.title, jc.company_name, jc.country
ORDER BY RANDOM()
LIMIT 10;


-- Сверка jobs_curated.skills_normalized и job_skills
SELECT COUNT(*) AS jobs_with_skills_normalized
FROM jobs_curated
WHERE COALESCE(array_length(skills_normalized, 1), 0) > 0;

SELECT COUNT(DISTINCT job_id) AS jobs_with_job_skills
FROM job_skills;

-- Найти вакансии, где skills_normalized были, а job_skills не появились
SELECT jc.job_id,
       jc.title,
       jc.company_name,
       jc.skills_normalized
FROM jobs_curated jc
WHERE COALESCE(array_length(jc.skills_normalized, 1), 0) > 0
  AND NOT EXISTS (
      SELECT 1
      FROM job_skills js
      WHERE js.job_id = jc.job_id
  )
LIMIT 50;


-- Проверки salary_aggregates_v2
SELECT COUNT(*) AS salary_aggregates_v2_rows
FROM salary_aggregates_v2;

SELECT *
FROM salary_aggregates_v2
ORDER BY sample_size DESC, role, country
LIMIT 50;

SELECT country,
       currency,
       COUNT(*) AS groups_count
FROM salary_aggregates_v2
GROUP BY country, currency
ORDER BY groups_count DESC, country, currency;

-- Подозрительные записи с sample_size <= 1
SELECT *
FROM salary_aggregates_v2
WHERE sample_size <= 1
ORDER BY sample_size, role, country;


-- Проверки market_role_stats_v2
SELECT COUNT(*) AS market_role_stats_v2_rows
FROM market_role_stats_v2;

SELECT *
FROM market_role_stats_v2
ORDER BY jobs_count DESC, role, country
LIMIT 50;

SELECT role,
       country,
       seniority,
       is_remote,
       jobs_count,
       median_salary,
       avg_salary,
       competition_proxy
FROM market_role_stats_v2
WHERE jobs_count >= 5
ORDER BY jobs_count DESC, role
LIMIT 50;

-- Проверка на отрицательные / странные competition_proxy
SELECT *
FROM market_role_stats_v2
WHERE competition_proxy IS NOT NULL
  AND competition_proxy < 0
ORDER BY competition_proxy;


-- Проверки market_skill_stats_v2
SELECT COUNT(*) AS market_skill_stats_v2_rows
FROM market_skill_stats_v2;

SELECT mss.role,
       mss.country,
       mss.seniority,
       mss.is_remote,
       sd.canonical_name,
       mss.jobs_with_skill,
       mss.jobs_total,
       mss.share_pct,
       mss.salary_median_with_skill,
       mss.salary_median_without_skill,
       mss.salary_delta
FROM market_skill_stats_v2 mss
JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
ORDER BY mss.share_pct DESC, mss.jobs_with_skill DESC
LIMIT 50;

SELECT sd.canonical_name,
       COUNT(*) AS market_slices_count
FROM market_skill_stats_v2 mss
JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
GROUP BY sd.canonical_name
ORDER BY market_slices_count DESC, sd.canonical_name
LIMIT 50;

-- Навыки с максимальным salary_delta
SELECT mss.role,
       mss.country,
       mss.seniority,
       sd.canonical_name,
       mss.share_pct,
       mss.salary_delta
FROM market_skill_stats_v2 mss
JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
WHERE mss.salary_delta IS NOT NULL
ORDER BY mss.salary_delta DESC
LIMIT 50;

-- Навыки с минимальным salary_delta
SELECT mss.role,
       mss.country,
       mss.seniority,
       sd.canonical_name,
       mss.share_pct,
       mss.salary_delta
FROM market_skill_stats_v2 mss
JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
WHERE mss.salary_delta IS NOT NULL
ORDER BY mss.salary_delta ASC
LIMIT 50;


-- Проверки ссылочной целостности.
-- job_skills без словаря быть не должно.
SELECT COUNT(*) AS orphan_job_skills_without_dictionary
FROM job_skills js
LEFT JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
WHERE sd.skill_id IS NULL;

-- skill_match_rules без словаря быть не должно
SELECT COUNT(*) AS orphan_skill_match_rules_without_dictionary
FROM skill_match_rules smr
LEFT JOIN skills_dictionary sd ON sd.skill_id = smr.skill_id
WHERE sd.skill_id IS NULL;

-- market_skill_stats_v2 без словаря быть не должно
SELECT COUNT(*) AS orphan_market_skill_stats_v2_without_dictionary
FROM market_skill_stats_v2 mss
LEFT JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
WHERE sd.skill_id IS NULL;


-- Случайная ручная проверка данных
SELECT jc.job_id,
       jc.title,
       jc.company_name,
       jc.country,
       jc.seniority_normalized,
       jc.skills_normalized
FROM jobs_curated jc
ORDER BY RANDOM()
LIMIT 10;

SELECT jc.job_id,
       jc.title,
       jc.company_name,
       ARRAY_AGG(sd.canonical_name ORDER BY sd.canonical_name) AS linked_skills
FROM jobs_curated jc
JOIN job_skills js ON js.job_id = jc.job_id
JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
GROUP BY jc.job_id, jc.title, jc.company_name
ORDER BY RANDOM()
LIMIT 10;