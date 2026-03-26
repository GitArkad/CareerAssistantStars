-- ###########################################################
-- migrate_add_fields.sql
-- Расширяет jobs_curated новыми полями и ставит защиту от дублей.
-- Скрипт идемпотентный: безопасно запускать повторно.
-- ###########################################################

BEGIN;

-- Хелпер: добавляет колонку только если её ещё нет.
-- Нужен для мягкой миграции без падений на existing schema.
CREATE OR REPLACE FUNCTION _add_col_if_not_exists(
    _table TEXT, _column TEXT, _type TEXT, _default TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = _table
          AND column_name = _column
    ) THEN
        -- Если передан default, добавляем колонку сразу с ним.
        IF _default IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD COLUMN %I %s DEFAULT %s',
                _table, _column, _type, _default
            );
        ELSE
            EXECUTE format(
                'ALTER TABLE public.%I ADD COLUMN %I %s',
                _table, _column, _type
            );
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Новые атрибуты вакансии для enriched/analytics слоя.
SELECT _add_col_if_not_exists('jobs_curated', 'department', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'salary_period', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'visa_sponsorship', 'BOOLEAN');
SELECT _add_col_if_not_exists('jobs_curated', 'relocation', 'BOOLEAN');
SELECT _add_col_if_not_exists('jobs_curated', 'benefits', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'education', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'certifications', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'spoken_languages', 'TEXT[]', '''{}''');
SELECT _add_col_if_not_exists('jobs_curated', 'equity_bonus', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'security_clearance', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'role_family', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'country_normalized', 'TEXT');

-- Булевы флаги для быстрых фильтров и агрегатов.
SELECT _add_col_if_not_exists('jobs_curated', 'is_data_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_ml_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_python_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_analyst_role', 'BOOLEAN', 'FALSE');

-- Обычные индексы для частых фильтров/группировок.
CREATE INDEX IF NOT EXISTS idx_jobs_role_family ON jobs_curated (role_family);
CREATE INDEX IF NOT EXISTS idx_jobs_country_normalized ON jobs_curated (country_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_department ON jobs_curated (department);
CREATE INDEX IF NOT EXISTS idx_jobs_salary_period ON jobs_curated (salary_period);

-- Частичные индексы: ускоряют выборки только по TRUE.
CREATE INDEX IF NOT EXISTS idx_jobs_is_data_role ON jobs_curated (is_data_role) WHERE is_data_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_is_ml_role ON jobs_curated (is_ml_role) WHERE is_ml_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_is_python_role ON jobs_curated (is_python_role) WHERE is_python_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_visa ON jobs_curated (visa_sponsorship) WHERE visa_sponsorship = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_relocation ON jobs_curated (relocation) WHERE relocation = TRUE;

-- GIN для поиска/фильтрации по массиву языков.
CREATE INDEX IF NOT EXISTS idx_jobs_spoken_languages ON jobs_curated USING GIN (spoken_languages);

-- Глобальная уникальность внутреннего job_id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_job_id
    ON jobs_curated (job_id);

-- Защита от дублей по source + source_job_id, если source_job_id есть.
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_source_source_job_id
    ON jobs_curated (source, source_job_id)
    WHERE source_job_id IS NOT NULL AND btrim(source_job_id) <> '';

-- Защита от дублей по source + url, если url есть.
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_source_url
    ON jobs_curated (source, url)
    WHERE url IS NOT NULL AND btrim(url) <> '';

-- Удаляем временный хелпер, чтобы не оставлять служебную функцию в схеме.
DROP FUNCTION IF EXISTS _add_col_if_not_exists;

COMMIT;