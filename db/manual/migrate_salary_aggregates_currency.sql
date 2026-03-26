-- ###########################################################
-- migrate_salary_aggregates_currency.sql
--
-- Добавляет валюту в salary_aggregates и меняет уникальность
-- так, чтобы агрегаты можно было хранить отдельно по USD/EUR/RUB.
-- Идемпотентный скрипт: повторный запуск допустим.
-- ###########################################################

BEGIN;

-- Добавляем currency, если колонки ещё нет.
-- DEFAULT 'USD' нужен для уже существующих строк.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'salary_aggregates' AND column_name = 'currency'
    ) THEN
        ALTER TABLE salary_aggregates ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD';
    END IF;
END $$;

-- Удаляем старую уникальность без currency.
ALTER TABLE salary_aggregates
    DROP CONSTRAINT IF EXISTS salary_aggregates_role_country_seniority_is_remote_key;

-- Новая уникальность: отдельная запись на ту же комбинацию,
-- но уже с учётом конкретной валюты.
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

-- Индекс для фильтрации и выборок по валюте.
CREATE INDEX IF NOT EXISTS idx_salary_aggregates_currency
    ON salary_aggregates (currency);

COMMIT;