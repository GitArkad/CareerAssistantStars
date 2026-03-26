-- =========================================================
-- migrate_salary_aggregates_currency.sql
--
-- Adds 'currency' column to salary_aggregates table.
-- Updates the unique constraint to include currency,
-- so we can store aggregates in USD, EUR, and RUB separately.
--
-- Run ONCE after init.sql, before exchange_rates_init.sql.
-- Safe to run multiple times (IF NOT EXISTS / IF EXISTS).
-- =========================================================

BEGIN;

-- Add currency column if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'salary_aggregates' AND column_name = 'currency'
    ) THEN
        ALTER TABLE salary_aggregates ADD COLUMN currency TEXT NOT NULL DEFAULT 'USD';
    END IF;
END $$;

-- Drop old unique constraint and create new one with currency
ALTER TABLE salary_aggregates
    DROP CONSTRAINT IF EXISTS salary_aggregates_role_country_seniority_is_remote_key;

-- New unique: one row per (role, country, seniority, is_remote, currency)
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

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_currency
    ON salary_aggregates (currency);

COMMIT;
