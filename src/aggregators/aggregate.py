from __future__ import annotations
from typing import Optional

import logging

logger = logging.getLogger(__name__)

COUNTRY_SENTINEL = "__ALL__"
SENIORITY_SENTINEL = "unknown"

# Полный пересчёт рыночных агрегатов после загрузки вакансий
def run_aggregate_step(etl_run_id: Optional[int] = None) -> dict:
    """
    Полностью пересчитывает:
    - market_skill_stats
    - salary_aggregates
    - market_role_stats

    Источник: jobs_curated

    ВАЖНО:
    Конвертация зарплат идёт по курсу на дату вакансии:
    published_at -> parsed_at -> CURRENT_DATE
    """
    from src.loaders.db_loader import get_connection, update_etl_run_progress

    summary = {
        "skill_stats_updated": 0,
        "salary_aggregates_updated": 0,
        "role_stats_updated": 0,
        "status": "success",
    }

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Полная очистка агрегатов перед пересборкой
        cur.execute(
            """
            TRUNCATE TABLE
                market_skill_stats,
                salary_aggregates,
                market_role_stats;
            """
        )
        logger.info("Aggregate tables truncated before rebuild")

        # Пересчёт агрегатов по навыкам
        cur.execute(
            """
            WITH job_skill_rows AS (
                SELECT DISTINCT
                    jc.job_id,
                    jc.title_normalized AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(jc.country_normalized, jc.country)), ''), %s) AS country,
                    COALESCE(NULLIF(BTRIM(jc.seniority_normalized), ''), %s) AS seniority,
                    skill,
                    CASE
                        WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL
                            THEN (
                                convert_salary(
                                    jc.salary_from::numeric,
                                    jc.currency,
                                    'USD',
                                    COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                                )
                                +
                                convert_salary(
                                    jc.salary_to::numeric,
                                    jc.currency,
                                    'USD',
                                    COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                                )
                            ) / 2.0
                        WHEN jc.salary_from IS NOT NULL
                            THEN convert_salary(
                                jc.salary_from::numeric,
                                jc.currency,
                                'USD',
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                        WHEN jc.salary_to IS NOT NULL
                            THEN convert_salary(
                                jc.salary_to::numeric,
                                jc.currency,
                                'USD',
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                    END AS salary_mid_usd
                FROM jobs_curated jc
                CROSS JOIN LATERAL unnest(COALESCE(jc.skills_normalized, '{}'::text[])) AS skill
                WHERE jc.title_normalized IS NOT NULL
                  AND skill IS NOT NULL
                  AND BTRIM(skill) <> ''
            ),
            group_totals AS (
                SELECT
                    title_normalized AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s) AS country,
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s) AS seniority,
                    COUNT(DISTINCT job_id) AS total_jobs
                FROM jobs_curated
                WHERE title_normalized IS NOT NULL
                GROUP BY
                    title_normalized,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s),
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s)
            )
            INSERT INTO market_skill_stats (
                role, country, seniority, skill_name, share_pct, avg_salary, job_count, updated_at
            )
            SELECT
                js.role,
                js.country,
                js.seniority,
                js.skill AS skill_name,
                ROUND(COUNT(DISTINCT js.job_id)::numeric / NULLIF(gt.total_jobs, 0) * 100, 2) AS share_pct,
                ROUND(AVG(js.salary_mid_usd))::int AS avg_salary,
                COUNT(DISTINCT js.job_id) AS job_count,
                NOW()
            FROM job_skill_rows js
            JOIN group_totals gt
              ON gt.role = js.role
             AND gt.country = js.country
             AND gt.seniority = js.seniority
            GROUP BY js.role, js.country, js.seniority, js.skill, gt.total_jobs
            ON CONFLICT (role, country, seniority, skill_name)
            DO UPDATE SET
                share_pct = EXCLUDED.share_pct,
                avg_salary = EXCLUDED.avg_salary,
                job_count = EXCLUDED.job_count,
                updated_at = NOW()
            """,
            (
                COUNTRY_SENTINEL,
                SENIORITY_SENTINEL,
                COUNTRY_SENTINEL,
                SENIORITY_SENTINEL,
                COUNTRY_SENTINEL,
                SENIORITY_SENTINEL,
            ),
        )
        summary["skill_stats_updated"] = cur.rowcount
        logger.info("Skill stats: %s rows upserted", cur.rowcount)

        
        # Пересчёт зарплатных агрегатов по валютам
        total_salary_rows = 0

        for target_cur in ("USD", "EUR", "RUB"):
            cur.execute(
                """
                INSERT INTO salary_aggregates (
                    role, country, seniority, is_remote,
                    p10, p25, p50, p75, p90,
                    avg_salary, min_salary, max_salary, sample_size,
                    currency, updated_at
                )
                SELECT
                    title_normalized AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s) AS country,
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s) AS seniority,
                    COALESCE(remote, false) AS is_remote,
                    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY mid)::int AS p10,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mid)::int AS p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mid)::int AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mid)::int AS p75,
                    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mid)::int AS p90,
                    ROUND(AVG(mid))::int AS avg_salary,
                    MIN(mid)::int AS min_salary,
                    MAX(mid)::int AS max_salary,
                    COUNT(*) AS sample_size,
                    %s AS currency,
                    NOW()
                FROM (
                    SELECT
                        title_normalized,
                        country,
                        country_normalized,
                        seniority_normalized,
                        remote,
                        CASE
                            WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                                THEN (
                                    convert_salary(
                                        salary_from::numeric,
                                        currency,
                                        %s,
                                        COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                    )
                                    +
                                    convert_salary(
                                        salary_to::numeric,
                                        currency,
                                        %s,
                                        COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                    )
                                ) / 2.0
                            WHEN salary_from IS NOT NULL
                                THEN convert_salary(
                                    salary_from::numeric,
                                    currency,
                                    %s,
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                            WHEN salary_to IS NOT NULL
                                THEN convert_salary(
                                    salary_to::numeric,
                                    currency,
                                    %s,
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                        END AS mid
                    FROM jobs_curated
                    WHERE (salary_from IS NOT NULL OR salary_to IS NOT NULL)
                      AND title_normalized IS NOT NULL
                ) sub
                WHERE mid IS NOT NULL AND mid > 0
                GROUP BY
                    title_normalized,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s),
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s),
                    COALESCE(remote, false)
                HAVING COUNT(*) >= 3
                ON CONFLICT (role, country, seniority, is_remote, currency)
                DO UPDATE SET
                    p10 = EXCLUDED.p10,
                    p25 = EXCLUDED.p25,
                    p50 = EXCLUDED.p50,
                    p75 = EXCLUDED.p75,
                    p90 = EXCLUDED.p90,
                    avg_salary = EXCLUDED.avg_salary,
                    min_salary = EXCLUDED.min_salary,
                    max_salary = EXCLUDED.max_salary,
                    sample_size = EXCLUDED.sample_size,
                    currency = EXCLUDED.currency,
                    updated_at = NOW()
                """,
                (
                    COUNTRY_SENTINEL,
                    SENIORITY_SENTINEL,
                    target_cur,
                    target_cur,
                    target_cur,
                    target_cur,
                    target_cur,
                    COUNTRY_SENTINEL,
                    SENIORITY_SENTINEL,
                ),
            )
            total_salary_rows += cur.rowcount
            logger.info("Salary aggregates (%s): %s rows", target_cur, cur.rowcount)

        summary["salary_aggregates_updated"] = total_salary_rows
        logger.info("Salary aggregates total: %s rows upserted", total_salary_rows)

        
        # Пересчёт агрегатов по ролям
        cur.execute(
            """
            INSERT INTO market_role_stats (
                role, country, seniority, total_jobs, avg_experience,
                remote_pct, avg_salary, competition, updated_at
            )
            SELECT
                title_normalized AS role,
                COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s) AS country,
                COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s) AS seniority,
                COUNT(*) AS total_jobs,
                ROUND(AVG(COALESCE(years_experience_min, 0)), 2) AS avg_experience,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE COALESCE(remote, false) = true) / NULLIF(COUNT(*), 0),
                    2
                ) AS remote_pct,
                ROUND(AVG(
                    CASE
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                            THEN (
                                convert_salary(
                                    salary_from::numeric,
                                    currency,
                                    'USD',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                                +
                                convert_salary(
                                    salary_to::numeric,
                                    currency,
                                    'USD',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                            ) / 2.0
                        WHEN salary_from IS NOT NULL
                            THEN convert_salary(
                                salary_from::numeric,
                                currency,
                                'USD',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                        WHEN salary_to IS NOT NULL
                            THEN convert_salary(
                                salary_to::numeric,
                                currency,
                                'USD',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                    END
                ))::int AS avg_salary,
                CASE
                    WHEN COUNT(*) >= 100 THEN 'high'
                    WHEN COUNT(*) >= 30 THEN 'medium'
                    ELSE 'low'
                END AS competition,
                NOW()
            FROM jobs_curated
            WHERE title_normalized IS NOT NULL
            GROUP BY
                title_normalized,
                COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), %s),
                COALESCE(NULLIF(BTRIM(seniority_normalized), ''), %s)
            ON CONFLICT (role, country, seniority)
            DO UPDATE SET
                total_jobs = EXCLUDED.total_jobs,
                avg_experience = EXCLUDED.avg_experience,
                remote_pct = EXCLUDED.remote_pct,
                avg_salary = EXCLUDED.avg_salary,
                competition = EXCLUDED.competition,
                updated_at = NOW()
            """,
            (
                COUNTRY_SENTINEL,
                SENIORITY_SENTINEL,
                COUNTRY_SENTINEL,
                SENIORITY_SENTINEL,
            ),
        )
        summary["role_stats_updated"] = cur.rowcount
        logger.info("Role stats: %s rows upserted", cur.rowcount)

        conn.commit()

        # Обновление статуса etl_run после успешного пересчёта
        if etl_run_id is not None:
            with get_connection() as meta_conn:
                update_etl_run_progress(
                    meta_conn,
                    etl_run_id,
                    aggregates_updated=True,
                )
                meta_conn.commit()

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
        logger.error("Aggregate step failed: %s", e)

        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass

        # Обновление статуса etl_run при ошибке
        if etl_run_id is not None:
            try:
                with get_connection() as err_conn:
                    update_etl_run_progress(
                        err_conn,
                        etl_run_id,
                        status="failed",
                        error_message=str(e),
                        finalize=True,
                    )
                    err_conn.commit()
            except Exception as log_exc:
                logger.error("Failed to update etl_runs after aggregate error: %s", log_exc)

        raise

    finally:
        # Закрытие курсора и соединения
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    logger.info("Aggregate step complete: %s", summary)
    return summary


# Полный пересчёт слоя job_skills для аналитики и Streamlit
# Источник: jobs_curated + skills_dictionary + skill_match_rules
# Логика:
# - normalized_array: skills_normalized
# - text_extract: skills_extracted
# - key_skill: key_skills
#
# Для каждой строки пытаемся сначала найти exact canonical match,
# а затем fallback через synonym rules.
def run_refresh_job_skills_step(etl_run_id: Optional[int] = None) -> dict:
    from src.loaders.db_loader import get_connection, update_etl_run_progress

    summary = {
        "job_skills_rows": 0,
        "normalized_array_rows": 0,
        "text_extract_rows": 0,
        "key_skill_rows": 0,
        "status": "success",
    }

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE job_skills;")
        logger.info("job_skills truncated before rebuild")

        cur.execute(
            """
            INSERT INTO job_skills (
                job_id,
                skill_id,
                source_type,
                confidence,
                is_required
            )
            WITH source_arrays AS (
                SELECT
                    jc.job_id,
                    BTRIM(skill_name) AS skill_name,
                    'normalized_array'::text AS source_type,
                    1.0000::numeric AS confidence,
                    NULL::boolean AS is_required
                FROM jobs_curated jc
                CROSS JOIN LATERAL unnest(COALESCE(jc.skills_normalized, '{}'::text[])) AS skill_name

                UNION ALL

                SELECT
                    jc.job_id,
                    BTRIM(skill_name) AS skill_name,
                    'text_extract'::text AS source_type,
                    0.8500::numeric AS confidence,
                    NULL::boolean AS is_required
                FROM jobs_curated jc
                CROSS JOIN LATERAL unnest(COALESCE(jc.skills_extracted, '{}'::text[])) AS skill_name

                UNION ALL

                SELECT
                    jc.job_id,
                    BTRIM(skill_name) AS skill_name,
                    'key_skill'::text AS source_type,
                    1.0000::numeric AS confidence,
                    TRUE AS is_required
                FROM jobs_curated jc
                CROSS JOIN LATERAL unnest(COALESCE(jc.key_skills, '{}'::text[])) AS skill_name
            ),
            resolved AS (
                SELECT DISTINCT
                    sa.job_id,
                    COALESCE(sd.skill_id, smr.skill_id) AS skill_id,
                    sa.source_type,
                    sa.confidence,
                    sa.is_required
                FROM source_arrays sa
                LEFT JOIN skills_dictionary sd
                    ON lower(sd.canonical_name) = lower(sa.skill_name)
                LEFT JOIN skill_match_rules smr
                    ON lower(smr.synonym) = lower(sa.skill_name)
                WHERE sa.skill_name IS NOT NULL
                  AND sa.skill_name <> ''
                  AND COALESCE(sd.skill_id, smr.skill_id) IS NOT NULL
            )
            SELECT
                job_id,
                skill_id,
                source_type,
                confidence,
                is_required
            FROM resolved
            ON CONFLICT (job_id, skill_id, source_type) DO NOTHING;
            """
        )
        summary["job_skills_rows"] = cur.rowcount
        logger.info("job_skills rebuilt: %s rows inserted", cur.rowcount)

        cur.execute(
            """
            SELECT source_type, COUNT(*)
            FROM job_skills
            GROUP BY source_type;
            """
        )
        for source_type, row_count in cur.fetchall():
            if source_type == "normalized_array":
                summary["normalized_array_rows"] = row_count
            elif source_type == "text_extract":
                summary["text_extract_rows"] = row_count
            elif source_type == "key_skill":
                summary["key_skill_rows"] = row_count

        conn.commit()

        if etl_run_id is not None:
            with get_connection() as meta_conn:
                update_etl_run_progress(
                    meta_conn,
                    etl_run_id,
                    aggregates_updated=True,
                )
                meta_conn.commit()

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
        logger.error("job_skills refresh failed: %s", e)

        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass

        raise

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    logger.info("job_skills refresh complete: %s", summary)
    return summary


# Полный пересчёт v2-аналитики для Streamlit.
# Источник: v_jobs_analytics_base + job_skills.
#
# ВАЖНО:
# - v2-аналитика агрегируется по role_family.
# - salary_aggregates_v2, market_role_stats_v2 и market_skill_stats_v2
#   считают зарплаты в RUB.
# - market_skill_stats_v2 использует DISTINCT по (job_id, skill_id),
#   чтобы один и тот же навык, пришедший из нескольких source_type,
#   не завышал медианы и доли.
def run_aggregate_v2_step(etl_run_id: Optional[int] = None) -> dict:
    from src.loaders.db_loader import get_connection, update_etl_run_progress

    summary = {
        "salary_aggregates_v2_updated": 0,
        "market_role_stats_v2_updated": 0,
        "market_skill_stats_v2_updated": 0,
        "status": "success",
    }

    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            TRUNCATE TABLE
                salary_aggregates_v2,
                market_role_stats_v2,
                market_skill_stats_v2;
            """
        )
        logger.info("v2 aggregate tables truncated before rebuild")

        # 1) salary_aggregates_v2
        # Храним агрегаты в единой валюте RUB и агрегируем по role_family,
        # чтобы не дробить рынок на тысячи specialty/title.
        cur.execute(
            """
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(BTRIM(role_family), ''), 'other') AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), 'unknown') AS country,
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), 'unknown') AS seniority,
                    COALESCE(remote, remote_type IN ('remote', 'hybrid')) AS is_remote,
                    CASE
                        WHEN salary_from_rub IS NOT NULL AND salary_to_rub IS NOT NULL THEN (salary_from_rub + salary_to_rub) / 2.0
                        WHEN salary_from_rub IS NOT NULL THEN salary_from_rub::numeric
                        WHEN salary_to_rub IS NOT NULL THEN salary_to_rub::numeric
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN
                            (
                                convert_salary(
                                    salary_from::numeric,
                                    currency,
                                    'RUB',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                                +
                                convert_salary(
                                    salary_to::numeric,
                                    currency,
                                    'RUB',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                            ) / 2.0
                        WHEN salary_from IS NOT NULL THEN
                            convert_salary(
                                salary_from::numeric,
                                currency,
                                'RUB',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                        WHEN salary_to IS NOT NULL THEN
                            convert_salary(
                                salary_to::numeric,
                                currency,
                                'RUB',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                        ELSE NULL
                    END AS salary_mid_rub
                FROM v_jobs_analytics_base
            )
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
                role,
                country,
                seniority,
                is_remote,
                'RUB' AS currency,
                COUNT(*) AS sample_size,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_mid_rub) AS p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_mid_rub) AS p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_mid_rub) AS p75,
                ROUND(AVG(salary_mid_rub), 2) AS avg_salary,
                NOW() AS updated_at
            FROM base
            WHERE salary_mid_rub IS NOT NULL
              AND salary_mid_rub > 0
            GROUP BY role, country, seniority, is_remote
            HAVING COUNT(*) >= 10;
            """
        )
        summary["salary_aggregates_v2_updated"] = cur.rowcount
        logger.info("salary_aggregates_v2: %s rows inserted", cur.rowcount)

        # 2) market_role_stats_v2
        # Основная витрина для Streamlit: role_family + зарплата в RUB.
        cur.execute(
            """
            WITH base AS (
                SELECT
                    COALESCE(NULLIF(BTRIM(role_family), ''), 'other') AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(country_normalized, country)), ''), 'unknown') AS country,
                    COALESCE(NULLIF(BTRIM(seniority_normalized), ''), 'unknown') AS seniority,
                    COALESCE(remote, remote_type IN ('remote', 'hybrid')) AS is_remote,
                    CASE
                        WHEN salary_from_rub IS NOT NULL AND salary_to_rub IS NOT NULL THEN (salary_from_rub + salary_to_rub) / 2.0
                        WHEN salary_from_rub IS NOT NULL THEN salary_from_rub::numeric
                        WHEN salary_to_rub IS NOT NULL THEN salary_to_rub::numeric
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN
                            (
                                convert_salary(
                                    salary_from::numeric,
                                    currency,
                                    'RUB',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                                +
                                convert_salary(
                                    salary_to::numeric,
                                    currency,
                                    'RUB',
                                    COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                                )
                            ) / 2.0
                        WHEN salary_from IS NOT NULL THEN
                            convert_salary(
                                salary_from::numeric,
                                currency,
                                'RUB',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                        WHEN salary_to IS NOT NULL THEN
                            convert_salary(
                                salary_to::numeric,
                                currency,
                                'RUB',
                                COALESCE(published_at::date, parsed_at::date, CURRENT_DATE)
                            )
                        ELSE NULL
                    END AS salary_mid_rub,
                    COALESCE(years_experience_min, years_experience_max, 1) AS years_exp_proxy
                FROM v_jobs_analytics_base
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
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_mid_rub) AS median_salary,
                ROUND(AVG(salary_mid_rub), 2) AS avg_salary,
                ROUND(COUNT(*)::numeric / GREATEST(AVG(NULLIF(years_exp_proxy, 0)), 1), 2) AS competition_proxy,
                NOW() AS updated_at
            FROM base
            GROUP BY role, country, seniority, is_remote
            HAVING COUNT(*) >= 20;
            """
        )
        summary["market_role_stats_v2_updated"] = cur.rowcount
        logger.info("market_role_stats_v2: %s rows inserted", cur.rowcount)

        # 3) market_skill_stats_v2
        # Доли навыков считаем только на срезах с достаточным объёмом,
        # иначе таблица превращается в шум из групп по 5 вакансий.
        cur.execute(
            """
            WITH base_jobs AS (
                SELECT
                    jc.job_id,
                    COALESCE(NULLIF(BTRIM(jc.role_family), ''), 'other') AS role,
                    COALESCE(NULLIF(BTRIM(COALESCE(jc.country_normalized, jc.country)), ''), 'unknown') AS country,
                    COALESCE(NULLIF(BTRIM(jc.seniority_normalized), ''), 'unknown') AS seniority,
                    COALESCE(jc.remote, jc.remote_type IN ('remote', 'hybrid')) AS is_remote,
                    CASE
                        WHEN jc.salary_from_rub IS NOT NULL AND jc.salary_to_rub IS NOT NULL THEN (jc.salary_from_rub + jc.salary_to_rub) / 2.0
                        WHEN jc.salary_from_rub IS NOT NULL THEN jc.salary_from_rub::numeric
                        WHEN jc.salary_to_rub IS NOT NULL THEN jc.salary_to_rub::numeric
                        WHEN jc.salary_from IS NOT NULL AND jc.salary_to IS NOT NULL THEN
                            (
                                convert_salary(
                                    jc.salary_from::numeric,
                                    jc.currency,
                                    'RUB',
                                    COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                                )
                                +
                                convert_salary(
                                    jc.salary_to::numeric,
                                    jc.currency,
                                    'RUB',
                                    COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                                )
                            ) / 2.0
                        WHEN jc.salary_from IS NOT NULL THEN
                            convert_salary(
                                jc.salary_from::numeric,
                                jc.currency,
                                'RUB',
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                        WHEN jc.salary_to IS NOT NULL THEN
                            convert_salary(
                                jc.salary_to::numeric,
                                jc.currency,
                                'RUB',
                                COALESCE(jc.published_at::date, jc.parsed_at::date, CURRENT_DATE)
                            )
                        ELSE NULL
                    END AS salary_mid_rub
                FROM v_jobs_analytics_base jc
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
                HAVING COUNT(*) >= 20
            ),
            jobs_with_skills AS (
                SELECT DISTINCT
                    bj.role,
                    bj.country,
                    bj.seniority,
                    bj.is_remote,
                    js.skill_id,
                    bj.job_id,
                    bj.salary_mid_rub
                FROM base_jobs bj
                JOIN market_slices ms
                  ON ms.role = bj.role
                 AND ms.country = bj.country
                 AND ms.seniority = bj.seniority
                 AND ms.is_remote = bj.is_remote
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
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_mid_rub) AS salary_median_with_skill
                FROM jobs_with_skills
                GROUP BY role, country, seniority, is_remote, skill_id
                HAVING COUNT(DISTINCT job_id) >= 3
            ),
            without_skill AS (
                SELECT
                    bj.role,
                    bj.country,
                    bj.seniority,
                    bj.is_remote,
                    s.skill_id,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bj.salary_mid_rub) AS salary_median_without_skill
                FROM base_jobs bj
                JOIN (
                    SELECT DISTINCT role, country, seniority, is_remote, skill_id
                    FROM with_skill
                ) s
                  ON s.role = bj.role
                 AND s.country = bj.country
                 AND s.seniority = bj.seniority
                 AND s.is_remote = bj.is_remote
                JOIN market_slices ms
                  ON ms.role = bj.role
                 AND ms.country = bj.country
                 AND ms.seniority = bj.seniority
                 AND ms.is_remote = bj.is_remote
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
            """
        )
        summary["market_skill_stats_v2_updated"] = cur.rowcount
        logger.info("market_skill_stats_v2: %s rows inserted", cur.rowcount)

        conn.commit()

        if etl_run_id is not None:
            with get_connection() as meta_conn:
                update_etl_run_progress(
                    meta_conn,
                    etl_run_id,
                    aggregates_updated=True,
                )
                meta_conn.commit()

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
        logger.exception("run_aggregate_v2_step failed: %s", e)
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return summary

# Локальный запуск для ручной проверки
if __name__ == "__main__":
    result = run_aggregate_v2_step()
    print(result)
    