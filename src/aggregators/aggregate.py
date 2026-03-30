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

# Локальный запуск для ручной проверки
if __name__ == "__main__":
    result = run_aggregate_step()
    print(result)