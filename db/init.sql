BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =========================================================
-- 1. STAGING / RAW DATA
--    Сюда складывается сырой результат парсинга/API до очистки
-- =========================================================

CREATE TABLE IF NOT EXISTS jobs_raw (
    raw_job_id BIGSERIAL PRIMARY KEY,

    source TEXT NOT NULL,
    source_job_id TEXT,
    url TEXT,
    search_query TEXT,

    payload JSONB NOT NULL,

    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    parsed_at TIMESTAMP,

    processing_status TEXT NOT NULL DEFAULT 'new',
    processing_error TEXT,

    content_hash TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_jobs_raw_status
        CHECK (processing_status IN ('new', 'processed', 'failed', 'duplicate'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_raw_source_source_job_id
    ON jobs_raw (source, source_job_id)
    WHERE source_job_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_raw_source_url
    ON jobs_raw (source, url)
    WHERE url IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_raw_source
    ON jobs_raw (source);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_fetched_at
    ON jobs_raw (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_status
    ON jobs_raw (processing_status);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_payload_gin
    ON jobs_raw USING GIN (payload);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_search_query
    ON jobs_raw (search_query);

-- =========================================================
-- 2. CURATED / NORMALIZED JOBS
--    Сюда попадают уже очищенные и нормализованные записи
-- =========================================================

CREATE TABLE IF NOT EXISTS jobs_curated (
    job_id TEXT PRIMARY KEY,
    raw_job_id BIGINT,

    source TEXT NOT NULL,
    source_job_id TEXT,
    url TEXT,

    title TEXT,
    title_normalized TEXT,
    description TEXT,
    requirements TEXT,
    responsibilities TEXT,
    nice_to_have TEXT,

    salary_from INTEGER,
    salary_to INTEGER,
    currency TEXT,
    salary_period TEXT,

    experience_level TEXT,
    seniority_normalized TEXT,
    years_experience_min INTEGER,
    years_experience_max INTEGER,

    company_name TEXT,
    industry TEXT,
    company_size TEXT,

    key_skills TEXT[] DEFAULT '{}',
    skills_extracted TEXT[] DEFAULT '{}',
    skills_normalized TEXT[] DEFAULT '{}',
    tech_stack_tags TEXT[] DEFAULT '{}',
    tools TEXT[] DEFAULT '{}',
    methodologies TEXT[] DEFAULT '{}',

    location TEXT,
    country TEXT,
    region TEXT,
    city TEXT,
    remote BOOLEAN DEFAULT FALSE,
    remote_type TEXT,
    employment_type TEXT,

    search_query TEXT,

    published_at TIMESTAMP NULL,
    parsed_at TIMESTAMP NULL,

    embedding_status TEXT DEFAULT 'pending',

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_jobs_curated_raw_job
        FOREIGN KEY (raw_job_id)
        REFERENCES jobs_raw (raw_job_id)
        ON DELETE SET NULL,

    CONSTRAINT chk_salary_range
        CHECK (
            salary_from IS NULL
            OR salary_to IS NULL
            OR salary_from <= salary_to
        ),

    CONSTRAINT chk_years_experience_range
        CHECK (
            years_experience_min IS NULL
            OR years_experience_max IS NULL
            OR years_experience_min <= years_experience_max
        ),

    CONSTRAINT chk_seniority_normalized
        CHECK (
            seniority_normalized IS NULL
            OR seniority_normalized IN ('intern', 'junior', 'middle', 'senior', 'lead', 'principal', 'manager', 'director', 'unknown')
        ),

    CONSTRAINT chk_embedding_status
        CHECK (
            embedding_status IS NULL
            OR embedding_status IN ('pending', 'created', 'failed', 'skipped')
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_curated_source_url
    ON jobs_curated (source, url)
    WHERE url IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_curated_source_source_job_id
    ON jobs_curated (source, source_job_id)
    WHERE source_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_source
    ON jobs_curated (source);

CREATE INDEX IF NOT EXISTS idx_jobs_country
    ON jobs_curated (country);

CREATE INDEX IF NOT EXISTS idx_jobs_region
    ON jobs_curated (region);

CREATE INDEX IF NOT EXISTS idx_jobs_city
    ON jobs_curated (city);

CREATE INDEX IF NOT EXISTS idx_jobs_company_name
    ON jobs_curated (company_name);

CREATE INDEX IF NOT EXISTS idx_jobs_seniority
    ON jobs_curated (seniority_normalized);

CREATE INDEX IF NOT EXISTS idx_jobs_title_normalized
    ON jobs_curated (title_normalized);

CREATE INDEX IF NOT EXISTS idx_jobs_published_at
    ON jobs_curated (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_parsed_at
    ON jobs_curated (parsed_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_remote
    ON jobs_curated (remote);

CREATE INDEX IF NOT EXISTS idx_jobs_remote_type
    ON jobs_curated (remote_type);

CREATE INDEX IF NOT EXISTS idx_jobs_employment_type
    ON jobs_curated (employment_type);

CREATE INDEX IF NOT EXISTS idx_jobs_source_job_id
    ON jobs_curated (source_job_id);

CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm
    ON jobs_curated USING GIN (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobs_title_normalized_trgm
    ON jobs_curated USING GIN (title_normalized gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobs_description_trgm
    ON jobs_curated USING GIN (description gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_jobs_skills_extracted
    ON jobs_curated USING GIN (skills_extracted);

CREATE INDEX IF NOT EXISTS idx_jobs_skills_normalized
    ON jobs_curated USING GIN (skills_normalized);

CREATE INDEX IF NOT EXISTS idx_jobs_tech_stack_tags
    ON jobs_curated USING GIN (tech_stack_tags);

CREATE INDEX IF NOT EXISTS idx_jobs_tools
    ON jobs_curated USING GIN (tools);

CREATE INDEX IF NOT EXISTS idx_jobs_methodologies
    ON jobs_curated USING GIN (methodologies);

-- =========================================================
-- 3. SKILL NORMALIZATION DICTIONARY
-- =========================================================

CREATE TABLE IF NOT EXISTS skill_synonyms (
    synonym TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    category TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_skill_synonyms_canonical
    ON skill_synonyms (canonical_name);

CREATE INDEX IF NOT EXISTS idx_skill_synonyms_category
    ON skill_synonyms (category);

-- =========================================================
-- 4. MARKET AGGREGATES
-- =========================================================

CREATE TABLE IF NOT EXISTS market_skill_stats (
    id BIGSERIAL PRIMARY KEY,
    role TEXT NOT NULL,
    country TEXT,
    seniority TEXT,
    skill_name TEXT NOT NULL,
    demand_pct NUMERIC(6,2),
    avg_salary INTEGER,
    job_count INTEGER,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(role, country, seniority, skill_name)
);

CREATE INDEX IF NOT EXISTS idx_market_skill_stats_role
    ON market_skill_stats (role);

CREATE INDEX IF NOT EXISTS idx_market_skill_stats_country
    ON market_skill_stats (country);

CREATE INDEX IF NOT EXISTS idx_market_skill_stats_skill_name
    ON market_skill_stats (skill_name);

CREATE TABLE IF NOT EXISTS salary_aggregates (
    id BIGSERIAL PRIMARY KEY,
    role TEXT NOT NULL,
    country TEXT,
    seniority TEXT,
    is_remote BOOLEAN,
    p10 INTEGER,
    p25 INTEGER,
    p50 INTEGER,
    p75 INTEGER,
    p90 INTEGER,
    avg_salary INTEGER,
    min_salary INTEGER,
    max_salary INTEGER,
    sample_size INTEGER,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(role, country, seniority, is_remote)
);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_role
    ON salary_aggregates (role);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_country
    ON salary_aggregates (country);

CREATE TABLE IF NOT EXISTS market_role_stats (
    id BIGSERIAL PRIMARY KEY,
    role TEXT NOT NULL,
    country TEXT,
    seniority TEXT,
    total_jobs INTEGER,
    avg_experience NUMERIC(6,2),
    remote_pct NUMERIC(6,2),
    avg_salary INTEGER,
    competition TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(role, country, seniority)
);

CREATE INDEX IF NOT EXISTS idx_market_role_stats_role
    ON market_role_stats (role);

CREATE INDEX IF NOT EXISTS idx_market_role_stats_country
    ON market_role_stats (country);

-- =========================================================
-- 5. ETL / PIPELINE LOGS
-- =========================================================

CREATE TABLE IF NOT EXISTS etl_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL DEFAULT 'jobs_pipeline',
    dag_id TEXT,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    source TEXT,

    jobs_extracted INTEGER DEFAULT 0,
    jobs_new_raw INTEGER DEFAULT 0,
    jobs_processed_raw INTEGER DEFAULT 0,
    jobs_curated_inserted INTEGER DEFAULT 0,
    jobs_curated_updated INTEGER DEFAULT 0,
    jobs_duplicates INTEGER DEFAULT 0,
    embeddings_created INTEGER DEFAULT 0,
    aggregates_updated BOOLEAN DEFAULT FALSE,

    status TEXT NOT NULL DEFAULT 'running',
    error_message TEXT,

    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    duration_sec INTEGER,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_etl_status
        CHECK (status IN ('running', 'success', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_run_date
    ON etl_runs (run_date DESC);

CREATE INDEX IF NOT EXISTS idx_etl_runs_status
    ON etl_runs (status);

CREATE INDEX IF NOT EXISTS idx_etl_runs_source
    ON etl_runs (source);

CREATE INDEX IF NOT EXISTS idx_etl_runs_pipeline_name
    ON etl_runs (pipeline_name);

CREATE INDEX IF NOT EXISTS idx_etl_runs_dag_id
    ON etl_runs (dag_id);

-- =========================================================
-- 6. UPDATED_AT TRIGGERS
-- =========================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_raw_set_updated_at ON jobs_raw;
CREATE TRIGGER trg_jobs_raw_set_updated_at
BEFORE UPDATE ON jobs_raw
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_curated_set_updated_at ON jobs_curated;
CREATE TRIGGER trg_jobs_curated_set_updated_at
BEFORE UPDATE ON jobs_curated
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;