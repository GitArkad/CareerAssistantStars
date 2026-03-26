-- =========================================================
-- exchange_rates_init.sql
--
-- Run AFTER init.sql. Creates:
-- 1. exchange_rates table (daily currency pairs)
-- 2. convert_salary() function (any-to-any conversion)
-- 3. v_salary_converted view (jobs with converted salaries)
-- =========================================================

BEGIN;

-- =========================================================
-- 1. EXCHANGE RATES TABLE
-- =========================================================

CREATE TABLE IF NOT EXISTS exchange_rates (
    id BIGSERIAL,
    rate_date DATE NOT NULL,
    base_currency TEXT NOT NULL,
    target_currency TEXT NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (rate_date, base_currency, target_currency)
);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_date
    ON exchange_rates (rate_date DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_pair
    ON exchange_rates (base_currency, target_currency);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_target
    ON exchange_rates (target_currency, rate_date DESC);

-- =========================================================
-- 2. CONVERSION FUNCTION
--
-- Usage:
--   SELECT convert_salary(50000, 'GBP', 'USD');           -- latest rate
--   SELECT convert_salary(50000, 'GBP', 'USD', '2025-01-15');  -- historical
--
-- Returns NULL if rate not found or amount is NULL.
-- =========================================================

CREATE OR REPLACE FUNCTION convert_salary(
    amount NUMERIC,
    from_currency TEXT,
    to_currency TEXT,
    target_date DATE DEFAULT NULL
)
RETURNS NUMERIC AS $$
DECLARE
    conversion_rate NUMERIC;
    lookup_date DATE;
BEGIN
    IF amount IS NULL OR from_currency IS NULL OR to_currency IS NULL THEN
        RETURN NULL;
    END IF;

    -- Same currency: no conversion needed
    IF UPPER(from_currency) = UPPER(to_currency) THEN
        RETURN amount;
    END IF;

    -- Use provided date or find the latest available
    IF target_date IS NOT NULL THEN
        -- Find the closest rate on or before the target date
        SELECT er.rate, er.rate_date INTO conversion_rate, lookup_date
        FROM exchange_rates er
        WHERE er.base_currency = UPPER(from_currency)
          AND er.target_currency = UPPER(to_currency)
          AND er.rate_date <= target_date
        ORDER BY er.rate_date DESC
        LIMIT 1;
    ELSE
        -- Latest available rate
        SELECT er.rate INTO conversion_rate
        FROM exchange_rates er
        WHERE er.base_currency = UPPER(from_currency)
          AND er.target_currency = UPPER(to_currency)
        ORDER BY er.rate_date DESC
        LIMIT 1;
    END IF;

    IF conversion_rate IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN ROUND(amount * conversion_rate, 2);
END;
$$ LANGUAGE plpgsql STABLE;


-- =========================================================
-- 3. VIEW: Salary converted to a chosen currency
--
-- To use:
--   SELECT * FROM v_salary_converted('USD');
--
-- Since views can't take parameters, we use a function
-- that returns a table.
-- =========================================================

CREATE OR REPLACE FUNCTION v_salary_converted(target TEXT DEFAULT 'USD')
RETURNS TABLE (
    job_id TEXT,
    source TEXT,
    title TEXT,
    company_name TEXT,
    country TEXT,
    seniority_normalized TEXT,
    original_currency TEXT,
    salary_from_original INTEGER,
    salary_to_original INTEGER,
    target_currency TEXT,
    salary_from_converted NUMERIC,
    salary_to_converted NUMERIC,
    salary_mid_converted NUMERIC,
    exchange_rate NUMERIC,
    rate_date DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        jc.job_id,
        jc.source,
        jc.title,
        jc.company_name,
        jc.country,
        jc.seniority_normalized,
        jc.currency AS original_currency,
        jc.salary_from AS salary_from_original,
        jc.salary_to AS salary_to_original,
        UPPER(target) AS target_currency,
        convert_salary(jc.salary_from::numeric, jc.currency, target) AS salary_from_converted,
        convert_salary(jc.salary_to::numeric, jc.currency, target) AS salary_to_converted,
        ROUND((
            COALESCE(
                convert_salary(jc.salary_from::numeric, jc.currency, target),
                convert_salary(jc.salary_to::numeric, jc.currency, target)
            ) +
            COALESCE(
                convert_salary(jc.salary_to::numeric, jc.currency, target),
                convert_salary(jc.salary_from::numeric, jc.currency, target)
            )
        ) / 2.0, 2) AS salary_mid_converted,
        er.rate AS exchange_rate,
        er.rate_date
    FROM jobs_curated jc
    LEFT JOIN LATERAL (
        SELECT e.rate, e.rate_date
        FROM exchange_rates e
        WHERE e.base_currency = UPPER(jc.currency)
          AND e.target_currency = UPPER(target)
        ORDER BY e.rate_date DESC
        LIMIT 1
    ) er ON TRUE
    WHERE jc.salary_from IS NOT NULL OR jc.salary_to IS NOT NULL;
END;
$$ LANGUAGE plpgsql STABLE;


-- =========================================================
-- 4. QUICK VIEWS: USD, EUR, RUB
--
-- One view per target currency for fast dashboard queries.
--   SELECT * FROM v_salary_usd WHERE country = 'UNITED KINGDOM';
--   SELECT * FROM v_salary_eur WHERE seniority_normalized = 'senior';
--   SELECT * FROM v_salary_rub WHERE source = 'hh.ru';
-- =========================================================

-- Helper to avoid repeating the same SELECT for each currency
CREATE OR REPLACE FUNCTION _salary_view_query(target TEXT)
RETURNS TABLE (
    job_id TEXT,
    source TEXT,
    title TEXT,
    company_name TEXT,
    country TEXT,
    city TEXT,
    seniority_normalized TEXT,
    remote_type TEXT,
    original_currency TEXT,
    salary_from_original INTEGER,
    salary_to_original INTEGER,
    salary_from_converted NUMERIC,
    salary_to_converted NUMERIC,
    salary_mid_converted NUMERIC,
    published_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        jc.job_id,
        jc.source,
        jc.title,
        jc.company_name,
        jc.country,
        jc.city,
        jc.seniority_normalized,
        jc.remote_type,
        jc.currency AS original_currency,
        jc.salary_from AS salary_from_original,
        jc.salary_to AS salary_to_original,
        convert_salary(jc.salary_from::numeric, jc.currency, target) AS salary_from_converted,
        convert_salary(jc.salary_to::numeric, jc.currency, target) AS salary_to_converted,
        ROUND((
            COALESCE(
                convert_salary(jc.salary_from::numeric, jc.currency, target),
                convert_salary(jc.salary_to::numeric, jc.currency, target)
            ) +
            COALESCE(
                convert_salary(jc.salary_to::numeric, jc.currency, target),
                convert_salary(jc.salary_from::numeric, jc.currency, target)
            )
        ) / 2.0, 2) AS salary_mid_converted,
        jc.published_at
    FROM jobs_curated jc
    WHERE jc.salary_from IS NOT NULL OR jc.salary_to IS NOT NULL;
END;
$$ LANGUAGE plpgsql STABLE;


CREATE OR REPLACE VIEW v_salary_usd AS
    SELECT * FROM _salary_view_query('USD');

CREATE OR REPLACE VIEW v_salary_eur AS
    SELECT * FROM _salary_view_query('EUR');

CREATE OR REPLACE VIEW v_salary_rub AS
    SELECT * FROM _salary_view_query('RUB');

COMMIT;
