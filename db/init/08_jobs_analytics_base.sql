BEGIN;

CREATE OR REPLACE VIEW v_jobs_analytics_base AS
SELECT jc.*
FROM jobs_curated jc
JOIN job_registry jr
  ON jr.job_id = jc.job_id
WHERE jr.is_active = TRUE
  AND jc.title IS NOT NULL
  AND BTRIM(jc.title) <> '';

COMMIT;
