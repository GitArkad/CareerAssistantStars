BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =========================================================
-- 1. CURATED / NORMALIZED JOBS
-- =========================================================

CREATE TABLE IF NOT EXISTS jobs_curated (
    job_id TEXT PRIMARY KEY,

    run_id TEXT,
    raw_s3_key TEXT,
    clean_s3_key TEXT,
    content_hash TEXT,

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
    department TEXT,

    key_skills TEXT[] DEFAULT '{}',
    skills_extracted TEXT[] DEFAULT '{}',
    skills_normalized TEXT[] DEFAULT '{}',
    tech_stack_tags TEXT[] DEFAULT '{}',
    tools TEXT[] DEFAULT '{}',
    methodologies TEXT[] DEFAULT '{}',

    visa_sponsorship BOOLEAN DEFAULT FALSE,
    relocation BOOLEAN DEFAULT FALSE,
    benefits TEXT,
    education TEXT,
    certifications TEXT,
    spoken_languages TEXT[] DEFAULT '{}',
    equity_bonus TEXT,
    security_clearance TEXT,
    role_family TEXT,

    location TEXT,
    country TEXT,
    country_normalized TEXT,
    region TEXT,
    city TEXT,
    remote BOOLEAN DEFAULT FALSE,
    remote_type TEXT,
    employment_type TEXT,

    is_data_role BOOLEAN DEFAULT FALSE,
    is_ml_role BOOLEAN DEFAULT FALSE,
    is_python_role BOOLEAN DEFAULT FALSE,
    is_analyst_role BOOLEAN DEFAULT FALSE,

    search_query TEXT,

    published_at TIMESTAMP NULL,
    parsed_at TIMESTAMP NULL,

    embedding_status TEXT DEFAULT 'pending',

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

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
            OR seniority_normalized IN (
                'intern', 'junior', 'middle', 'senior', 'lead',
                'principal', 'manager', 'director', 'unknown'
            )
        ),

    CONSTRAINT chk_embedding_status
        CHECK (
            embedding_status IS NULL
            OR embedding_status IN ('pending', 'created', 'failed', 'skipped')
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_curated_source_url
    ON jobs_curated (source, url)
    WHERE url IS NOT NULL AND btrim(url) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_curated_source_source_job_id
    ON jobs_curated (source, source_job_id)
    WHERE source_job_id IS NOT NULL AND btrim(source_job_id) <> '';

CREATE INDEX IF NOT EXISTS idx_jobs_run_id
    ON jobs_curated (run_id);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_s3_key
    ON jobs_curated (raw_s3_key);

CREATE INDEX IF NOT EXISTS idx_jobs_clean_s3_key
    ON jobs_curated (clean_s3_key);

CREATE INDEX IF NOT EXISTS idx_jobs_content_hash
    ON jobs_curated (content_hash);

CREATE INDEX IF NOT EXISTS idx_jobs_source
    ON jobs_curated (source);

CREATE INDEX IF NOT EXISTS idx_jobs_country
    ON jobs_curated (country);

CREATE INDEX IF NOT EXISTS idx_jobs_country_normalized
    ON jobs_curated (country_normalized);

CREATE INDEX IF NOT EXISTS idx_jobs_region
    ON jobs_curated (region);

CREATE INDEX IF NOT EXISTS idx_jobs_city
    ON jobs_curated (city);

CREATE INDEX IF NOT EXISTS idx_jobs_company_name
    ON jobs_curated (company_name);

CREATE INDEX IF NOT EXISTS idx_jobs_department
    ON jobs_curated (department);

CREATE INDEX IF NOT EXISTS idx_jobs_role_family
    ON jobs_curated (role_family);

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

CREATE INDEX IF NOT EXISTS idx_jobs_salary_period
    ON jobs_curated (salary_period);

CREATE INDEX IF NOT EXISTS idx_jobs_source_job_id
    ON jobs_curated (source_job_id);

CREATE INDEX IF NOT EXISTS idx_jobs_embedding_status
    ON jobs_curated (embedding_status);

CREATE INDEX IF NOT EXISTS idx_jobs_is_data_role
    ON jobs_curated (is_data_role)
    WHERE is_data_role = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_is_ml_role
    ON jobs_curated (is_ml_role)
    WHERE is_ml_role = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_is_python_role
    ON jobs_curated (is_python_role)
    WHERE is_python_role = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_is_analyst_role
    ON jobs_curated (is_analyst_role)
    WHERE is_analyst_role = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_visa_sponsorship
    ON jobs_curated (visa_sponsorship)
    WHERE visa_sponsorship = TRUE;

CREATE INDEX IF NOT EXISTS idx_jobs_relocation
    ON jobs_curated (relocation)
    WHERE relocation = TRUE;

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

CREATE INDEX IF NOT EXISTS idx_jobs_spoken_languages
    ON jobs_curated USING GIN (spoken_languages);


-- =========================================================
-- 2. FILE-LEVEL INGESTION MANIFEST
-- =========================================================

CREATE TABLE IF NOT EXISTS ingestion_manifest (
    id BIGSERIAL PRIMARY KEY,

    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_s3_key TEXT NOT NULL,
    clean_s3_key TEXT,
    raw_file_hash TEXT,

    raw_row_count INTEGER,
    clean_row_count INTEGER,
    loaded_row_count INTEGER,

    fetched_at TIMESTAMP,
    parsed_at TIMESTAMP,
    cleaned_at TIMESTAMP,
    loaded_at TIMESTAMP,

    status TEXT NOT NULL DEFAULT 'parsed',
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_manifest_status
        CHECK (status IN ('parsed', 'cleaned', 'loaded', 'failed', 'skipped'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_manifest_raw_s3_key
    ON ingestion_manifest (raw_s3_key);

CREATE INDEX IF NOT EXISTS idx_manifest_run_id
    ON ingestion_manifest (run_id);

CREATE INDEX IF NOT EXISTS idx_manifest_source
    ON ingestion_manifest (source);

CREATE INDEX IF NOT EXISTS idx_manifest_clean_s3_key
    ON ingestion_manifest (clean_s3_key);

CREATE INDEX IF NOT EXISTS idx_manifest_status
    ON ingestion_manifest (status);

CREATE INDEX IF NOT EXISTS idx_manifest_parsed_at
    ON ingestion_manifest (parsed_at DESC);

CREATE INDEX IF NOT EXISTS idx_manifest_loaded_at
    ON ingestion_manifest (loaded_at DESC);


-- =========================================================
-- 3. JOB REGISTRY + JOB AUDIT
-- =========================================================

CREATE TABLE IF NOT EXISTS job_registry (
    job_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_job_id TEXT,
    url TEXT,

    first_seen_run_id TEXT NOT NULL,
    last_seen_run_id TEXT NOT NULL,
    first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),

    last_raw_s3_key TEXT,
    last_clean_s3_key TEXT,
    content_hash TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_registry_source_source_job_id
    ON job_registry (source, source_job_id)
    WHERE source_job_id IS NOT NULL AND btrim(source_job_id) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_registry_source_url
    ON job_registry (source, url)
    WHERE url IS NOT NULL AND btrim(url) <> '';

CREATE INDEX IF NOT EXISTS idx_job_registry_source
    ON job_registry (source);

CREATE INDEX IF NOT EXISTS idx_job_registry_last_seen_at
    ON job_registry (last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_registry_content_hash
    ON job_registry (content_hash);


CREATE TABLE IF NOT EXISTS job_audit (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_job_id TEXT,
    url TEXT,
    title TEXT,
    company_name TEXT,
    raw_s3_key TEXT,
    clean_s3_key TEXT,
    content_hash TEXT,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    message TEXT,
    seen_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_job_audit_job_id
        FOREIGN KEY (job_id) REFERENCES job_registry(job_id) ON DELETE CASCADE,

    CONSTRAINT chk_job_audit_action
        CHECK (action IN ('inserted', 'updated', 'unchanged', 'failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_audit_run_job
    ON job_audit (run_id, job_id);

CREATE INDEX IF NOT EXISTS idx_job_audit_job_id
    ON job_audit (job_id);

CREATE INDEX IF NOT EXISTS idx_job_audit_run_id
    ON job_audit (run_id);

CREATE INDEX IF NOT EXISTS idx_job_audit_source
    ON job_audit (source);

CREATE INDEX IF NOT EXISTS idx_job_audit_seen_at
    ON job_audit (seen_at DESC);


-- =========================================================
-- 4. SKILL NORMALIZATION DICTIONARY
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
-- 5. MARKET AGGREGATES
-- =========================================================

CREATE TABLE IF NOT EXISTS market_skill_stats (
    id BIGSERIAL PRIMARY KEY,
    role TEXT NOT NULL,
    country TEXT,
    seniority TEXT,
    skill_name TEXT NOT NULL,
    share_pct NUMERIC(6,2),
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
    currency TEXT NOT NULL DEFAULT 'USD',
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT salary_aggregates_role_country_seniority_remote_cur_key
        UNIQUE(role, country, seniority, is_remote, currency)
);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_role
    ON salary_aggregates (role);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_country
    ON salary_aggregates (country);

CREATE INDEX IF NOT EXISTS idx_salary_aggregates_currency
    ON salary_aggregates (currency);

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
-- 6. ETL / PIPELINE LOGS
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
-- 7. UPDATED_AT TRIGGERS
-- =========================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_curated_set_updated_at ON jobs_curated;
CREATE TRIGGER trg_jobs_curated_set_updated_at
BEFORE UPDATE ON jobs_curated
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_ingestion_manifest_set_updated_at ON ingestion_manifest;
CREATE TRIGGER trg_ingestion_manifest_set_updated_at
BEFORE UPDATE ON ingestion_manifest
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_job_registry_set_updated_at ON job_registry;
CREATE TRIGGER trg_job_registry_set_updated_at
BEFORE UPDATE ON job_registry
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
