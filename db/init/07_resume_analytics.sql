BEGIN;

-- Аналитика по резюме для Streamlit.


-- Количество подходящих вакансий по навыкам пользователя.
CREATE OR REPLACE FUNCTION fn_matching_jobs_count(
    _user_skills      TEXT[],
    _min_match        INT     DEFAULT 1,
    _country          TEXT    DEFAULT NULL,
    _seniority        TEXT    DEFAULT NULL,
    _remote_only      BOOLEAN DEFAULT FALSE,
    _target_currency  TEXT    DEFAULT NULL
)
RETURNS TABLE (
    total_matching_jobs           BIGINT,
    avg_user_skill_coverage_pct   NUMERIC,
    avg_job_fit_pct               NUMERIC,
    median_salary_mid             NUMERIC,
    salary_currency               TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH user_skill_ids AS (
        SELECT sd.skill_id
        FROM skills_dictionary sd
        WHERE lower(sd.canonical_name) = ANY (
            ARRAY(SELECT lower(skill) FROM unnest(_user_skills) AS t(skill))
        )
    ),
    job_total_skills AS (
        SELECT js.job_id, COUNT(DISTINCT js.skill_id) AS total_job_skills
        FROM job_skills js
        GROUP BY js.job_id
    ),
    job_matches AS (
        SELECT
            js.job_id,
            COUNT(DISTINCT js.skill_id)::INT AS matched_skills
        FROM job_skills js
        JOIN user_skill_ids usi ON usi.skill_id = js.skill_id
        GROUP BY js.job_id
        HAVING COUNT(DISTINCT js.skill_id) >= _min_match
    ),
    filtered AS (
        SELECT
            jm.job_id,
            jm.matched_skills,
            jts.total_job_skills,
            CASE
                WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                    (
                        COALESCE(
                            convert_salary(
                                jc.salary_from::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            ),
                            jc.salary_from::numeric
                        )
                        +
                        COALESCE(
                            convert_salary(
                                jc.salary_to::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            ),
                            jc.salary_to::numeric
                        )
                    ) / 2.0
                WHEN jc.salary_from IS NOT NULL THEN
                    COALESCE(
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        ),
                        jc.salary_from::numeric
                    )
                WHEN jc.salary_to IS NOT NULL THEN
                    COALESCE(
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        ),
                        jc.salary_to::numeric
                    )
                ELSE NULL
            END AS salary_mid
        FROM job_matches jm
        JOIN v_jobs_analytics_base jc   ON jc.job_id = jm.job_id
        LEFT JOIN job_total_skills jts ON jts.job_id = jm.job_id
        WHERE (_country IS NULL OR COALESCE(jc.country_normalized, jc.country) = _country)
          AND (_seniority IS NULL OR jc.seniority_normalized = _seniority)
          AND (
                _remote_only = FALSE
                OR jc.remote = TRUE
                OR jc.remote_type IN ('remote', 'hybrid')
          )
    )
    SELECT
        COUNT(*)::BIGINT AS total_matching_jobs,
        ROUND(
            AVG(
                100.0 * matched_skills
                / GREATEST(COALESCE(array_length(_user_skills, 1), 0), 1)
            ),
            1
        ) AS avg_user_skill_coverage_pct,
        ROUND(
            AVG(
                100.0 * matched_skills
                / GREATEST(COALESCE(total_job_skills, 0), 1)
            ),
            1
        ) AS avg_job_fit_pct,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_mid)::NUMERIC AS median_salary_mid,
        COALESCE(UPPER(_target_currency), 'RAW')::TEXT AS salary_currency
    FROM filtered;
END;
$$ LANGUAGE plpgsql STABLE;


-- Список подходящих вакансий с деталями совпадений.
CREATE OR REPLACE FUNCTION fn_matching_jobs(
    _user_skills      TEXT[],
    _min_match        INT     DEFAULT 1,
    _country          TEXT    DEFAULT NULL,
    _seniority        TEXT    DEFAULT NULL,
    _remote_only      BOOLEAN DEFAULT FALSE,
    _limit            INT     DEFAULT 50,
    _offset           INT     DEFAULT 0,
    _target_currency  TEXT    DEFAULT NULL
)
RETURNS TABLE (
    job_id                     TEXT,
    title                      TEXT,
    company_name               TEXT,
    country                    TEXT,
    city                       TEXT,
    seniority                  TEXT,
    remote_type                TEXT,
    salary_from                INTEGER,
    salary_to                  INTEGER,
    currency                   TEXT,
    salary_mid_converted       NUMERIC,
    salary_currency            TEXT,
    url                        TEXT,
    matched_skills             TEXT[],
    matched_count              INT,
    total_job_skills           BIGINT,
    user_skill_coverage_pct    NUMERIC,
    job_skill_fit_pct          NUMERIC,
    missing_skills             TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    WITH user_skill_ids AS (
        SELECT sd.skill_id, sd.canonical_name
        FROM skills_dictionary sd
        WHERE lower(sd.canonical_name) = ANY (
            ARRAY(SELECT lower(skill) FROM unnest(_user_skills) AS t(skill))
        )
    ),
    job_match_detail AS (
        SELECT
            js.job_id,
            ARRAY_AGG(DISTINCT usi.canonical_name ORDER BY usi.canonical_name) AS matched_skills,
            COUNT(DISTINCT js.skill_id)::INT AS matched_count
        FROM job_skills js
        JOIN user_skill_ids usi ON usi.skill_id = js.skill_id
        GROUP BY js.job_id
        HAVING COUNT(DISTINCT js.skill_id) >= _min_match
    ),
    job_total_skills AS (
        SELECT js.job_id, COUNT(DISTINCT js.skill_id) AS total_skills
        FROM job_skills js
        GROUP BY js.job_id
    ),
    job_missing AS (
        SELECT
            jmd.job_id,
            ARRAY_AGG(DISTINCT sd.canonical_name ORDER BY sd.canonical_name) AS missing_skills
        FROM job_match_detail jmd
        JOIN job_skills js       ON js.job_id = jmd.job_id
        JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
        WHERE sd.skill_id NOT IN (SELECT skill_id FROM user_skill_ids)
        GROUP BY jmd.job_id
    )
    SELECT
        jc.job_id,
        jc.title,
        jc.company_name,
        COALESCE(jc.country_normalized, jc.country) AS country,
        jc.city,
        jc.seniority_normalized                     AS seniority,
        jc.remote_type,
        jc.salary_from,
        jc.salary_to,
        jc.currency,
        CASE
            WHEN _target_currency IS NULL THEN
                CASE
                    WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL
                        THEN (jc.salary_from + jc.salary_to) / 2.0
                    WHEN jc.salary_from IS NOT NULL THEN jc.salary_from::numeric
                    WHEN jc.salary_to   IS NOT NULL THEN jc.salary_to::numeric
                    ELSE NULL
                END
            WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                (
                    COALESCE(
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        ),
                        jc.salary_from::numeric
                    )
                    +
                    COALESCE(
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        ),
                        jc.salary_to::numeric
                    )
                ) / 2.0
            WHEN jc.salary_from IS NOT NULL THEN
                COALESCE(
                    convert_salary(
                        jc.salary_from::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    ),
                    jc.salary_from::numeric
                )
            WHEN jc.salary_to IS NOT NULL THEN
                COALESCE(
                    convert_salary(
                        jc.salary_to::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    ),
                    jc.salary_to::numeric
                )
            ELSE NULL
        END AS salary_mid_converted,
        COALESCE(UPPER(_target_currency), jc.currency, 'RAW') AS salary_currency,
        jc.url,
        jmd.matched_skills,
        jmd.matched_count,
        jts.total_skills AS total_job_skills,
        ROUND(
            100.0 * jmd.matched_count
            / GREATEST(COALESCE(array_length(_user_skills, 1), 0), 1),
            1
        ) AS user_skill_coverage_pct,
        ROUND(
            100.0 * jmd.matched_count
            / GREATEST(COALESCE(jts.total_skills, 0), 1),
            1
        ) AS job_skill_fit_pct,
        COALESCE(jms.missing_skills, '{}'::TEXT[]) AS missing_skills
    FROM job_match_detail jmd
    JOIN v_jobs_analytics_base jc      ON jc.job_id = jmd.job_id
    LEFT JOIN job_total_skills jts ON jts.job_id = jmd.job_id
    LEFT JOIN job_missing jms ON jms.job_id = jmd.job_id
    WHERE (_country IS NULL OR COALESCE(jc.country_normalized, jc.country) = _country)
      AND (_seniority IS NULL OR jc.seniority_normalized = _seniority)
      AND (
            _remote_only = FALSE
            OR jc.remote = TRUE
            OR jc.remote_type IN ('remote', 'hybrid')
      )
    ORDER BY
        ROUND(
            100.0 * jmd.matched_count
            / GREATEST(COALESCE(jts.total_skills, 0), 1),
            1
        ) DESC,
        jmd.matched_count DESC,
        jc.published_at DESC NULLS LAST
    LIMIT _limit OFFSET _offset;
END;
$$ LANGUAGE plpgsql STABLE;


-- Топ навыков для выбранного рынка.
CREATE OR REPLACE FUNCTION fn_top_skills_for_market(
    _country     TEXT DEFAULT NULL,
    _role        TEXT DEFAULT NULL,
    _seniority   TEXT DEFAULT NULL,
    _limit       INT  DEFAULT 20
)
RETURNS TABLE (
    skill_name           TEXT,
    category             TEXT,
    jobs_with_skill      INT,
    jobs_total           INT,
    share_pct            NUMERIC,
    salary_median_with   NUMERIC,
    salary_delta         NUMERIC
) AS $$
BEGIN
    -- Сначала используем v2-агрегаты, если они есть.
    IF EXISTS (
        SELECT 1
        FROM market_role_stats_v2 mrs
        WHERE (_country   IS NULL OR mrs.country   = _country)
          AND (_role      IS NULL OR mrs.role      = _role)
          AND (_seniority IS NULL OR mrs.seniority = _seniority)
        LIMIT 1
    ) THEN
        RETURN QUERY
        WITH selected_slices AS (
            SELECT
                mrs.role,
                mrs.country,
                mrs.seniority,
                mrs.is_remote,
                mrs.jobs_count
            FROM market_role_stats_v2 mrs
            WHERE (_country   IS NULL OR mrs.country   = _country)
              AND (_role      IS NULL OR mrs.role      = _role)
              AND (_seniority IS NULL OR mrs.seniority = _seniority)
        ),
        total_jobs AS (
            SELECT COALESCE(SUM(jobs_count), 0)::INT AS jobs_total
            FROM selected_slices
        )
        SELECT
            sd.canonical_name AS skill_name,
            sd.category,
            SUM(mss.jobs_with_skill)::INT AS jobs_with_skill,
            (SELECT jobs_total FROM total_jobs) AS jobs_total,
            ROUND(
                100.0 * SUM(mss.jobs_with_skill)::NUMERIC
                / NULLIF((SELECT jobs_total FROM total_jobs), 0),
                1
            ) AS share_pct,
            ROUND(
                SUM(
                    CASE
                        WHEN mss.salary_median_with_skill IS NOT NULL
                        THEN mss.salary_median_with_skill * mss.jobs_with_skill
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN mss.salary_median_with_skill IS NOT NULL
                            THEN mss.jobs_with_skill
                            ELSE 0
                        END
                    ),
                    0
                ),
                0
            ) AS salary_median_with,
            ROUND(
                SUM(
                    CASE
                        WHEN mss.salary_delta IS NOT NULL
                        THEN mss.salary_delta * mss.jobs_with_skill
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN mss.salary_delta IS NOT NULL
                            THEN mss.jobs_with_skill
                            ELSE 0
                        END
                    ),
                    0
                ),
                0
            ) AS salary_delta
        FROM market_skill_stats_v2 mss
        JOIN selected_slices ss
          ON ss.role      = mss.role
         AND ss.country   = mss.country
         AND ss.seniority = mss.seniority
         AND ss.is_remote = mss.is_remote
        JOIN skills_dictionary sd ON sd.skill_id = mss.skill_id
        GROUP BY sd.canonical_name, sd.category
        ORDER BY share_pct DESC, jobs_with_skill DESC, sd.canonical_name
        LIMIT _limit;

    ELSE
        -- Иначе считаем на лету.
        RETURN QUERY
        WITH base AS (
            SELECT
                jc.job_id,
                COALESCE(
                    NULLIF(BTRIM(jc.specialty), ''),
                    NULLIF(BTRIM(jc.title_normalized), ''),
                    NULLIF(BTRIM(jc.title), ''),
                    'unknown'
                ) AS role,
                COALESCE(jc.country_normalized, jc.country) AS country,
                COALESCE(jc.seniority_normalized, 'unknown') AS seniority,
                CASE
                    WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL
                        THEN (jc.salary_from + jc.salary_to) / 2.0
                    WHEN jc.salary_from IS NOT NULL THEN jc.salary_from::numeric
                    WHEN jc.salary_to   IS NOT NULL THEN jc.salary_to::numeric
                    ELSE NULL
                END AS salary_mid
            FROM v_jobs_analytics_base jc
            WHERE (_country IS NULL OR COALESCE(jc.country_normalized, jc.country) = _country)
              AND (
                    _role IS NULL
                    OR COALESCE(
                        NULLIF(BTRIM(jc.specialty), ''),
                        NULLIF(BTRIM(jc.title_normalized), ''),
                        NULLIF(BTRIM(jc.title), ''),
                        'unknown'
                    ) = _role
              )
              AND (_seniority IS NULL OR jc.seniority_normalized = _seniority)
        ),
        total AS (
            SELECT COUNT(*)::INT AS cnt
            FROM base
        )
        SELECT
            sd.canonical_name AS skill_name,
            sd.category,
            COUNT(DISTINCT b.job_id)::INT AS jobs_with_skill,
            (SELECT cnt FROM total) AS jobs_total,
            ROUND(
                100.0 * COUNT(DISTINCT b.job_id)::NUMERIC
                / NULLIF((SELECT cnt FROM total), 0),
                1
            ) AS share_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.salary_mid)::NUMERIC AS salary_median_with,
            NULL::NUMERIC AS salary_delta
        FROM base b
        JOIN job_skills js        ON js.job_id = b.job_id
        JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
        GROUP BY sd.canonical_name, sd.category
        ORDER BY share_pct DESC, jobs_with_skill DESC, sd.canonical_name
        LIMIT _limit;
    END IF;
END;
$$ LANGUAGE plpgsql STABLE;


-- Анализ недостающих навыков для рынка.
CREATE OR REPLACE FUNCTION fn_skill_gap(
    _user_skills TEXT[],
    _country     TEXT DEFAULT NULL,
    _role        TEXT DEFAULT NULL,
    _seniority   TEXT DEFAULT NULL,
    _limit       INT  DEFAULT 15
)
RETURNS TABLE (
    skill_name           TEXT,
    category             TEXT,
    share_pct            NUMERIC,
    salary_delta         NUMERIC,
    priority_score       NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH user_skill_lower AS (
        SELECT lower(skill) AS name
        FROM unnest(_user_skills) AS t(skill)
    ),
    market AS (
        SELECT *
        FROM fn_top_skills_for_market(_country, _role, _seniority, 100)
    ),
    max_positive_delta AS (
        SELECT MAX(GREATEST(COALESCE(salary_delta, 0), 0)) AS max_delta
        FROM market
    )
    SELECT
        m.skill_name,
        m.category,
        m.share_pct,
        m.salary_delta,
        ROUND(
            m.share_pct
            + COALESCE(
                GREATEST(m.salary_delta, 0)
                / NULLIF((SELECT max_delta FROM max_positive_delta), 0)
                * 20,
                0
            ),
            1
        ) AS priority_score
    FROM market m
    WHERE lower(m.skill_name) NOT IN (SELECT name FROM user_skill_lower)
      AND m.share_pct >= 5
    ORDER BY priority_score DESC, m.share_pct DESC, m.skill_name
    LIMIT _limit;
END;
$$ LANGUAGE plpgsql STABLE;


-- Зарплатный бенчмарк для выбранного профиля.
CREATE OR REPLACE FUNCTION fn_salary_benchmark(
    _role             TEXT,
    _country          TEXT    DEFAULT NULL,
    _seniority        TEXT    DEFAULT NULL,
    _currency         TEXT    DEFAULT NULL,
    _remote_only      BOOLEAN DEFAULT FALSE,
    _target_currency  TEXT    DEFAULT NULL
)
RETURNS TABLE (
    role             TEXT,
    country          TEXT,
    seniority        TEXT,
    currency         TEXT,
    sample_size      INT,
    p25              NUMERIC,
    p50              NUMERIC,
    p75              NUMERIC,
    avg_salary       NUMERIC
) AS $$
BEGIN
    IF _target_currency IS NULL THEN
        RETURN QUERY
        SELECT
            sa.role,
            sa.country,
            sa.seniority,
            sa.currency,
            SUM(sa.sample_size)::INT AS sample_size,
            ROUND(
                SUM(sa.p25 * sa.sample_size) / NULLIF(SUM(sa.sample_size), 0),
                0
            ) AS p25,
            ROUND(
                SUM(sa.p50 * sa.sample_size) / NULLIF(SUM(sa.sample_size), 0),
                0
            ) AS p50,
            ROUND(
                SUM(sa.p75 * sa.sample_size) / NULLIF(SUM(sa.sample_size), 0),
                0
            ) AS p75,
            ROUND(
                SUM(sa.avg_salary * sa.sample_size) / NULLIF(SUM(sa.sample_size), 0),
                0
            ) AS avg_salary
        FROM salary_aggregates_v2 sa
        WHERE sa.role = _role
          AND (_country   IS NULL OR sa.country   = _country)
          AND (_seniority IS NULL OR sa.seniority = _seniority)
          AND (_currency  IS NULL OR sa.currency  = _currency)
          AND (_remote_only = FALSE OR sa.is_remote = TRUE)
        GROUP BY sa.role, sa.country, sa.seniority, sa.currency
        ORDER BY sample_size DESC;

    ELSE
        RETURN QUERY
        WITH base AS (
            SELECT
                COALESCE(
                    NULLIF(BTRIM(jc.specialty), ''),
                    NULLIF(BTRIM(jc.title_normalized), ''),
                    NULLIF(BTRIM(jc.title), ''),
                    'unknown'
                ) AS role,
                COALESCE(COALESCE(jc.country_normalized, jc.country), 'unknown') AS country,
                COALESCE(jc.seniority_normalized, 'unknown') AS seniority,
                CASE
                    WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                        (
                            convert_salary(
                                jc.salary_from::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                            +
                            convert_salary(
                                jc.salary_to::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                        ) / 2.0
                    WHEN jc.salary_from IS NOT NULL THEN
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    WHEN jc.salary_to IS NOT NULL THEN
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    ELSE NULL
                END AS salary_mid
            FROM v_jobs_analytics_base jc
            WHERE COALESCE(
                      NULLIF(BTRIM(jc.specialty), ''),
                      NULLIF(BTRIM(jc.title_normalized), ''),
                      NULLIF(BTRIM(jc.title), ''),
                      'unknown'
                  ) = _role
              AND (_country   IS NULL OR COALESCE(jc.country_normalized, jc.country) = _country)
              AND (_seniority IS NULL OR jc.seniority_normalized = _seniority)
              AND (_currency  IS NULL OR jc.currency = _currency)
              AND (
                    _remote_only = FALSE
                    OR jc.remote = TRUE
                    OR jc.remote_type IN ('remote', 'hybrid')
              )
        )
        SELECT
            _role AS role,
            COALESCE(_country, 'all') AS country,
            COALESCE(_seniority, 'all') AS seniority,
            UPPER(_target_currency) AS currency,
            COUNT(*)::INT AS sample_size,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_mid)::NUMERIC AS p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_mid)::NUMERIC AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_mid)::NUMERIC AS p75,
            ROUND(AVG(salary_mid), 0) AS avg_salary
        FROM base
        WHERE salary_mid IS NOT NULL;
    END IF;
END;
$$ LANGUAGE plpgsql STABLE;


-- Обзор рынка по стране.
CREATE OR REPLACE FUNCTION fn_country_market_overview(
    _country          TEXT,
    _limit            INT  DEFAULT 30,
    _target_currency  TEXT DEFAULT NULL
)
RETURNS TABLE (
    role               TEXT,
    seniority          TEXT,
    jobs_count         INT,
    median_salary      NUMERIC,
    avg_salary         NUMERIC,
    competition_proxy  NUMERIC,
    top_skills         TEXT[],
    salary_currency    TEXT
) AS $$
BEGIN
    IF _target_currency IS NULL THEN
        RETURN QUERY
        WITH role_stats AS (
            SELECT
                mrs.role,
                mrs.seniority,
                SUM(mrs.jobs_count)::INT AS jobs_count,
                ROUND(
                    SUM(
                        CASE
                            WHEN mrs.median_salary IS NOT NULL
                            THEN mrs.median_salary * mrs.jobs_count
                            ELSE 0
                        END
                    ) / NULLIF(
                        SUM(
                            CASE
                                WHEN mrs.median_salary IS NOT NULL
                                THEN mrs.jobs_count
                                ELSE 0
                            END
                        ),
                        0
                    ),
                    0
                ) AS median_salary,
                ROUND(
                    SUM(
                        CASE
                            WHEN mrs.avg_salary IS NOT NULL
                            THEN mrs.avg_salary * mrs.jobs_count
                            ELSE 0
                        END
                    ) / NULLIF(
                        SUM(
                            CASE
                                WHEN mrs.avg_salary IS NOT NULL
                                THEN mrs.jobs_count
                                ELSE 0
                            END
                        ),
                        0
                    ),
                    0
                ) AS avg_salary,
                ROUND(
                    SUM(mrs.competition_proxy * mrs.jobs_count)
                    / NULLIF(SUM(mrs.jobs_count), 0),
                    2
                ) AS competition_proxy
            FROM market_role_stats_v2 mrs
            WHERE mrs.country = _country
              AND mrs.jobs_count >= 1
            GROUP BY mrs.role, mrs.seniority
        )
        SELECT
            rs.role,
            rs.seniority,
            rs.jobs_count,
            rs.median_salary,
            rs.avg_salary,
            rs.competition_proxy,
            (
                SELECT ARRAY_AGG(t.skill_name ORDER BY t.share_pct DESC, t.skill_name)
                FROM (
                    SELECT *
                    FROM fn_top_skills_for_market(_country, rs.role, rs.seniority, 5)
                ) t
            ) AS top_skills,
            'RAW'::TEXT AS salary_currency
        FROM role_stats rs
        ORDER BY rs.jobs_count DESC, rs.role, rs.seniority
        LIMIT _limit;

    ELSE
        RETURN QUERY
        WITH base AS (
            SELECT
                COALESCE(
                    NULLIF(BTRIM(jc.specialty), ''),
                    NULLIF(BTRIM(jc.title_normalized), ''),
                    NULLIF(BTRIM(jc.title), ''),
                    'unknown'
                ) AS role,
                COALESCE(jc.seniority_normalized, 'unknown') AS seniority,
                COALESCE(jc.years_experience_min, jc.years_experience_max, 1) AS years_exp_proxy,
                CASE
                    WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                        (
                            convert_salary(
                                jc.salary_from::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                            +
                            convert_salary(
                                jc.salary_to::numeric,
                                jc.currency,
                                _target_currency,
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                        ) / 2.0
                    WHEN jc.salary_from IS NOT NULL THEN
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    WHEN jc.salary_to IS NOT NULL THEN
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    ELSE NULL
                END AS salary_mid
            FROM v_jobs_analytics_base jc
            WHERE COALESCE(jc.country_normalized, jc.country) = _country
        ),
        role_stats AS (
            SELECT
                b.role,
                b.seniority,
                COUNT(*)::INT AS jobs_count,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.salary_mid)::NUMERIC AS median_salary,
                ROUND(AVG(b.salary_mid), 0) AS avg_salary,
                ROUND(
                    COUNT(*)::NUMERIC / GREATEST(AVG(NULLIF(b.years_exp_proxy, 0)), 1),
                    2
                ) AS competition_proxy
            FROM base b
            GROUP BY b.role, b.seniority
        )
        SELECT
            rs.role,
            rs.seniority,
            rs.jobs_count,
            rs.median_salary,
            rs.avg_salary,
            rs.competition_proxy,
            (
                SELECT ARRAY_AGG(t.skill_name ORDER BY t.share_pct DESC, t.skill_name)
                FROM (
                    SELECT *
                    FROM fn_top_skills_for_market(_country, rs.role, rs.seniority, 5)
                ) t
            ) AS top_skills,
            UPPER(_target_currency) AS salary_currency
        FROM role_stats rs
        ORDER BY rs.jobs_count DESC, rs.role, rs.seniority
        LIMIT _limit;
    END IF;
END;
$$ LANGUAGE plpgsql STABLE;


-- Лучшие страны для набора навыков пользователя.
CREATE OR REPLACE FUNCTION fn_best_countries_for_skills(
    _user_skills      TEXT[],
    _min_match        INT  DEFAULT 2,
    _limit            INT  DEFAULT 20,
    _target_currency  TEXT DEFAULT 'USD'
)
RETURNS TABLE (
    country                    TEXT,
    matching_jobs              BIGINT,
    avg_user_skill_coverage_pct NUMERIC,
    avg_job_fit_pct            NUMERIC,
    median_salary_mid          NUMERIC,
    salary_currency            TEXT,
    top_matched_skills         TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    WITH user_skill_ids AS (
        SELECT sd.skill_id, sd.canonical_name
        FROM skills_dictionary sd
        WHERE lower(sd.canonical_name) = ANY (
            ARRAY(SELECT lower(skill) FROM unnest(_user_skills) AS t(skill))
        )
    ),
    job_total_skills AS (
        SELECT js.job_id, COUNT(DISTINCT js.skill_id) AS total_job_skills
        FROM job_skills js
        GROUP BY js.job_id
    ),
    job_matches AS (
        SELECT
            js.job_id,
            COUNT(DISTINCT js.skill_id)::INT AS matched_count,
            ARRAY_AGG(DISTINCT usi.canonical_name ORDER BY usi.canonical_name) AS matched_skills
        FROM job_skills js
        JOIN user_skill_ids usi ON usi.skill_id = js.skill_id
        GROUP BY js.job_id
        HAVING COUNT(DISTINCT js.skill_id) >= _min_match
    )
    SELECT
        COALESCE(jc.country_normalized, jc.country) AS country,
        COUNT(*)::BIGINT AS matching_jobs,
        ROUND(
            AVG(
                100.0 * jm.matched_count
                / GREATEST(COALESCE(array_length(_user_skills, 1), 0), 1)
            ),
            1
        ) AS avg_user_skill_coverage_pct,
        ROUND(
            AVG(
                100.0 * jm.matched_count
                / GREATEST(COALESCE(jts.total_job_skills, 0), 1)
            ),
            1
        ) AS avg_job_fit_pct,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY CASE
                WHEN _target_currency IS NULL THEN
                    CASE
                        WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL
                            THEN (jc.salary_from + jc.salary_to) / 2.0
                        WHEN jc.salary_from IS NOT NULL THEN jc.salary_from::numeric
                        WHEN jc.salary_to   IS NOT NULL THEN jc.salary_to::numeric
                        ELSE NULL
                    END
                WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                    (
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                        +
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    ) / 2.0
                WHEN jc.salary_from IS NOT NULL THEN
                    convert_salary(
                        jc.salary_from::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    )
                WHEN jc.salary_to IS NOT NULL THEN
                    convert_salary(
                        jc.salary_to::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    )
                ELSE NULL
            END
        )::NUMERIC AS median_salary_mid,
        COALESCE(UPPER(_target_currency), 'RAW') AS salary_currency,
        (
            SELECT ARRAY_AGG(sub.s ORDER BY sub.cnt DESC, sub.s)
            FROM (
                SELECT
                    unnest(jm2.matched_skills) AS s,
                    COUNT(*) AS cnt
                FROM job_matches jm2
                JOIN v_jobs_analytics_base jc2 ON jc2.job_id = jm2.job_id
                WHERE COALESCE(jc2.country_normalized, jc2.country)
                    = COALESCE(jc.country_normalized, jc.country)
                GROUP BY s
                ORDER BY cnt DESC, s
                LIMIT 5
            ) sub
        ) AS top_matched_skills
    FROM job_matches jm
    JOIN v_jobs_analytics_base jc ON jc.job_id = jm.job_id
    LEFT JOIN job_total_skills jts ON jts.job_id = jm.job_id
    WHERE COALESCE(jc.country_normalized, jc.country) IS NOT NULL
    GROUP BY COALESCE(jc.country_normalized, jc.country)
    ORDER BY matching_jobs DESC, avg_job_fit_pct DESC, country
    LIMIT _limit;
END;
$$ LANGUAGE plpgsql STABLE;


-- Сравнение remote/hybrid и on-site.
CREATE OR REPLACE FUNCTION fn_remote_comparison(
    _user_skills      TEXT[],
    _min_match        INT  DEFAULT 2,
    _country          TEXT DEFAULT NULL,
    _target_currency  TEXT DEFAULT NULL
)
RETURNS TABLE (
    work_mode                  TEXT,
    matching_jobs              BIGINT,
    median_salary_mid          NUMERIC,
    salary_currency            TEXT,
    avg_user_skill_coverage_pct NUMERIC,
    avg_job_fit_pct            NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH user_skill_ids AS (
        SELECT sd.skill_id
        FROM skills_dictionary sd
        WHERE lower(sd.canonical_name) = ANY (
            ARRAY(SELECT lower(skill) FROM unnest(_user_skills) AS t(skill))
        )
    ),
    job_total_skills AS (
        SELECT js.job_id, COUNT(DISTINCT js.skill_id) AS total_job_skills
        FROM job_skills js
        GROUP BY js.job_id
    ),
    job_matches AS (
        SELECT
            js.job_id,
            COUNT(DISTINCT js.skill_id)::INT AS matched_count
        FROM job_skills js
        JOIN user_skill_ids usi ON usi.skill_id = js.skill_id
        GROUP BY js.job_id
        HAVING COUNT(DISTINCT js.skill_id) >= _min_match
    )
    SELECT
        CASE
            WHEN jc.remote = TRUE OR jc.remote_type IN ('remote', 'hybrid')
                THEN 'remote/hybrid'
            ELSE 'on-site'
        END AS work_mode,
        COUNT(*)::BIGINT AS matching_jobs,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY CASE
                WHEN _target_currency IS NULL THEN
                    CASE
                        WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL
                            THEN (jc.salary_from + jc.salary_to) / 2.0
                        WHEN jc.salary_from IS NOT NULL THEN jc.salary_from::numeric
                        WHEN jc.salary_to   IS NOT NULL THEN jc.salary_to::numeric
                        ELSE NULL
                    END
                WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                    (
                        convert_salary(
                            jc.salary_from::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                        +
                        convert_salary(
                            jc.salary_to::numeric,
                            jc.currency,
                            _target_currency,
                            COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                        )
                    ) / 2.0
                WHEN jc.salary_from IS NOT NULL THEN
                    convert_salary(
                        jc.salary_from::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    )
                WHEN jc.salary_to IS NOT NULL THEN
                    convert_salary(
                        jc.salary_to::numeric,
                        jc.currency,
                        _target_currency,
                        COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                    )
                ELSE NULL
            END
        )::NUMERIC AS median_salary_mid,
        COALESCE(UPPER(_target_currency), 'RAW') AS salary_currency,
        ROUND(
            AVG(
                100.0 * jm.matched_count
                / GREATEST(COALESCE(array_length(_user_skills, 1), 0), 1)
            ),
            1
        ) AS avg_user_skill_coverage_pct,
        ROUND(
            AVG(
                100.0 * jm.matched_count
                / GREATEST(COALESCE(jts.total_job_skills, 0), 1)
            ),
            1
        ) AS avg_job_fit_pct
    FROM job_matches jm
    JOIN v_jobs_analytics_base jc ON jc.job_id = jm.job_id
    LEFT JOIN job_total_skills jts ON jts.job_id = jm.job_id
    WHERE (_country IS NULL OR COALESCE(jc.country_normalized, jc.country) = _country)
    GROUP BY
        CASE
            WHEN jc.remote = TRUE OR jc.remote_type IN ('remote', 'hybrid')
                THEN 'remote/hybrid'
            ELSE 'on-site'
        END
    ORDER BY matching_jobs DESC;
END;
$$ LANGUAGE plpgsql STABLE;


-- Справочные view для UI.
CREATE OR REPLACE VIEW v_available_countries AS
SELECT
    COALESCE(country_normalized, country) AS country,
    COUNT(*) AS jobs_count
FROM v_jobs_analytics_base
WHERE COALESCE(country_normalized, country) IS NOT NULL
  AND BTRIM(COALESCE(country_normalized, country)) <> ''
GROUP BY COALESCE(country_normalized, country)
ORDER BY jobs_count DESC, country;

CREATE OR REPLACE VIEW v_available_roles AS
SELECT
    COALESCE(NULLIF(BTRIM(role_family), ''), 'other') AS role,
    COUNT(*) AS jobs_count
FROM v_jobs_analytics_base
GROUP BY COALESCE(NULLIF(BTRIM(role_family), ''), 'other')
ORDER BY jobs_count DESC, role;

CREATE OR REPLACE VIEW v_available_seniorities AS
SELECT
    seniority_normalized AS seniority,
    COUNT(*) AS jobs_count
FROM v_jobs_analytics_base
WHERE seniority_normalized IS NOT NULL
  AND seniority_normalized <> 'unknown'
GROUP BY seniority_normalized
ORDER BY
    CASE seniority_normalized
        WHEN 'intern'     THEN 1
        WHEN 'junior'     THEN 2
        WHEN 'middle'     THEN 3
        WHEN 'senior'     THEN 4
        WHEN 'lead'       THEN 5
        WHEN 'principal'  THEN 6
        WHEN 'manager'    THEN 7
        WHEN 'director'   THEN 8
        ELSE 999
    END,
    seniority_normalized;

CREATE OR REPLACE VIEW v_available_skills AS
SELECT
    sd.canonical_name AS skill_name,
    sd.category,
    COUNT(DISTINCT js.job_id) AS jobs_count
FROM skills_dictionary sd
LEFT JOIN job_skills js
       ON js.skill_id = sd.skill_id
LEFT JOIN v_jobs_analytics_base jb
       ON jb.job_id = js.job_id
WHERE sd.is_active = TRUE
  AND (jb.job_id IS NOT NULL OR js.job_id IS NULL)
GROUP BY sd.canonical_name, sd.category
ORDER BY jobs_count DESC, sd.canonical_name;

-- Общая покрываемость данных для Streamlit.
CREATE OR REPLACE VIEW v_streamlit_data_coverage_overview AS
WITH skill_jobs AS (
    SELECT DISTINCT js.job_id
    FROM job_skills js
    JOIN v_jobs_analytics_base jb ON jb.job_id = js.job_id
)
SELECT
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (
        WHERE salary_from_rub IS NOT NULL OR salary_to_rub IS NOT NULL
    ) AS jobs_with_salary,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE salary_from_rub IS NOT NULL OR salary_to_rub IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_salary,
    COUNT(*) FILTER (
        WHERE sj.job_id IS NOT NULL
    ) AS jobs_with_skills,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE sj.job_id IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_skills,
    COUNT(*) FILTER (
        WHERE (salary_from_rub IS NOT NULL OR salary_to_rub IS NOT NULL)
          AND sj.job_id IS NOT NULL
    ) AS jobs_with_salary_and_skills,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE (salary_from_rub IS NOT NULL OR salary_to_rub IS NOT NULL)
              AND sj.job_id IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_salary_and_skills,
    'RUB'::TEXT AS salary_currency
FROM v_jobs_analytics_base jb
LEFT JOIN skill_jobs sj ON sj.job_id = jb.job_id;

-- Покрываемость данных по источникам.
CREATE OR REPLACE VIEW v_streamlit_data_coverage_by_source AS
WITH skill_jobs AS (
    SELECT DISTINCT js.job_id
    FROM job_skills js
    JOIN v_jobs_analytics_base jb ON jb.job_id = js.job_id
)
SELECT
    jb.source,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (
        WHERE jb.salary_from_rub IS NOT NULL OR jb.salary_to_rub IS NOT NULL
    ) AS jobs_with_salary,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE jb.salary_from_rub IS NOT NULL OR jb.salary_to_rub IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_salary,
    COUNT(*) FILTER (
        WHERE sj.job_id IS NOT NULL
    ) AS jobs_with_skills,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE sj.job_id IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_skills,
    COUNT(*) FILTER (
        WHERE (jb.salary_from_rub IS NOT NULL OR jb.salary_to_rub IS NOT NULL)
          AND sj.job_id IS NOT NULL
    ) AS jobs_with_salary_and_skills,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE (jb.salary_from_rub IS NOT NULL OR jb.salary_to_rub IS NOT NULL)
              AND sj.job_id IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_salary_and_skills,
    'RUB'::TEXT AS salary_currency
FROM v_jobs_analytics_base jb
LEFT JOIN skill_jobs sj ON sj.job_id = jb.job_id
GROUP BY jb.source
ORDER BY total_jobs DESC, jb.source;

-- Самые частые навыки в активной базе.
CREATE OR REPLACE VIEW v_streamlit_top_skills AS
WITH total AS (
    SELECT COUNT(*)::numeric AS total_jobs
    FROM v_jobs_analytics_base
)
SELECT
    sd.canonical_name AS skill_name,
    sd.category,
    COUNT(DISTINCT js.job_id) AS jobs_count,
    ROUND(
        100.0 * COUNT(DISTINCT js.job_id)::numeric
        / NULLIF((SELECT total_jobs FROM total), 0),
        1
    ) AS pct_of_jobs
FROM job_skills js
JOIN v_jobs_analytics_base jb ON jb.job_id = js.job_id
JOIN skills_dictionary sd ON sd.skill_id = js.skill_id
GROUP BY sd.canonical_name, sd.category
ORDER BY jobs_count DESC, sd.canonical_name;

-- Индексы для аналитики.
CREATE INDEX IF NOT EXISTS idx_skills_dictionary_lower_name
    ON skills_dictionary (lower(canonical_name));

CREATE INDEX IF NOT EXISTS idx_job_skills_skill_job
    ON job_skills (skill_id, job_id);

COMMIT;