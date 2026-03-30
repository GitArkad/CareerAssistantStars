BEGIN;

-- Добавляем currency в salary_aggregates
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'salary_aggregates' AND column_name = 'currency'
    ) THEN
        ALTER TABLE salary_aggregates ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD';
    END IF;
END $$;

-- Удаляем старую уникальность без currency
ALTER TABLE salary_aggregates
    DROP CONSTRAINT IF EXISTS salary_aggregates_role_country_seniority_is_remote_key;

-- Новая уникальность с учётом currency
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'salary_aggregates_role_country_seniority_remote_cur_key'
    ) THEN
        ALTER TABLE salary_aggregates
            ADD CONSTRAINT salary_aggregates_role_country_seniority_remote_cur_key
            UNIQUE (role, country, seniority, is_remote, currency);
    END IF;
END $$;

-- Индекс по валюте
CREATE INDEX IF NOT EXISTS idx_salary_aggregates_currency
    ON salary_aggregates (currency);

COMMIT;