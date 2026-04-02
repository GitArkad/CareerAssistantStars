BEGIN;

-- Хелпер для идемпотентного добавления колонки
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

-- Новые поля вакансии
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

-- Булевы флаги для фильтров
SELECT _add_col_if_not_exists('jobs_curated', 'is_data_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_ml_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_python_role', 'BOOLEAN', 'FALSE');
SELECT _add_col_if_not_exists('jobs_curated', 'is_analyst_role', 'BOOLEAN', 'FALSE');

-- Индексы по основным полям
CREATE INDEX IF NOT EXISTS idx_jobs_role_family ON jobs_curated (role_family);
CREATE INDEX IF NOT EXISTS idx_jobs_country_normalized ON jobs_curated (country_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_department ON jobs_curated (department);
CREATE INDEX IF NOT EXISTS idx_jobs_salary_period ON jobs_curated (salary_period);

-- Частичные индексы по TRUE
CREATE INDEX IF NOT EXISTS idx_jobs_is_data_role ON jobs_curated (is_data_role) WHERE is_data_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_is_ml_role ON jobs_curated (is_ml_role) WHERE is_ml_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_is_python_role ON jobs_curated (is_python_role) WHERE is_python_role = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_visa ON jobs_curated (visa_sponsorship) WHERE visa_sponsorship = TRUE;
CREATE INDEX IF NOT EXISTS idx_jobs_relocation ON jobs_curated (relocation) WHERE relocation = TRUE;

-- Индекс по массиву языков
CREATE INDEX IF NOT EXISTS idx_jobs_spoken_languages ON jobs_curated USING GIN (spoken_languages);

-- Уникальный внутренний job_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_job_id
    ON jobs_curated (job_id);

-- Защита от дублей по source_job_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_source_source_job_id
    ON jobs_curated (source, source_job_id)
    WHERE source_job_id IS NOT NULL AND btrim(source_job_id) <> '';

-- Защита от дублей по URL
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_curated_source_url
    ON jobs_curated (source, url)
    WHERE url IS NOT NULL AND btrim(url) <> '';

-- Поля зарплаты в RUB
SELECT _add_col_if_not_exists('jobs_curated', 'salary_from_rub', 'INTEGER');
SELECT _add_col_if_not_exists('jobs_curated', 'salary_to_rub', 'INTEGER');

-- Поля enrichment
SELECT _add_col_if_not_exists('jobs_curated', 'specialty', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'specialty_category', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'salary_text', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'experience_text', 'TEXT');
SELECT _add_col_if_not_exists('jobs_curated', 'posting_language', 'TEXT');

-- Удаляем временный хелпер
DROP FUNCTION IF EXISTS _add_col_if_not_exists;

COMMIT;