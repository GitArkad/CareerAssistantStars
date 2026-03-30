BEGIN;


-- Словарь синонимов специальностей
CREATE TABLE IF NOT EXISTS specialty_synonyms (
    synonym TEXT PRIMARY KEY,
    canonical_specialty TEXT NOT NULL,
    specialty_category TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_specialty_synonyms_canonical
    ON specialty_synonyms (canonical_specialty);

CREATE INDEX IF NOT EXISTS idx_specialty_synonyms_category
    ON specialty_synonyms (specialty_category);

-- Реестр компаний
CREATE TABLE IF NOT EXISTS company_registry (
    company_id BIGSERIAL PRIMARY KEY,
    company_name_raw TEXT NOT NULL,
    company_name_canonical TEXT NOT NULL,
    country TEXT,
    industry TEXT,
    company_size TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_registry_raw
    ON company_registry (company_name_raw);

CREATE INDEX IF NOT EXISTS idx_company_registry_canonical
    ON company_registry (company_name_canonical);

CREATE INDEX IF NOT EXISTS idx_company_registry_country
    ON company_registry (country);

CREATE INDEX IF NOT EXISTS idx_company_registry_source
    ON company_registry (source);

CREATE UNIQUE INDEX IF NOT EXISTS ux_company_registry_source_raw
    ON company_registry (source, company_name_raw)
    WHERE source IS NOT NULL AND company_name_raw IS NOT NULL;


-- Лог синхронизации с Qdrant
CREATE TABLE IF NOT EXISTS vector_sync_log (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    content_hash TEXT,
    qdrant_point_id TEXT,
    model_name TEXT,
    embedded_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_vector_sync_log_job_id
        FOREIGN KEY (job_id) REFERENCES jobs_curated(job_id) ON DELETE CASCADE,

    CONSTRAINT chk_vector_sync_status
        CHECK (status IN ('pending', 'created', 'failed', 'deleted', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_vector_sync_log_job_id
    ON vector_sync_log (job_id);

CREATE INDEX IF NOT EXISTS idx_vector_sync_log_status
    ON vector_sync_log (status);

CREATE INDEX IF NOT EXISTS idx_vector_sync_log_model
    ON vector_sync_log (model_name);

CREATE INDEX IF NOT EXISTS idx_vector_sync_log_embedded_at
    ON vector_sync_log (embedded_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_vector_sync_log_job_hash_model
    ON vector_sync_log (job_id, content_hash, model_name)
    WHERE content_hash IS NOT NULL AND model_name IS NOT NULL;


-- История зарплат по вакансии
CREATE TABLE IF NOT EXISTS job_salary_history (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    run_id TEXT,
    seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    salary_from INTEGER,
    salary_to INTEGER,
    currency TEXT,
    salary_from_rub INTEGER,
    salary_to_rub INTEGER,
    salary_text TEXT,
    content_hash TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_job_salary_history_job_id
        FOREIGN KEY (job_id) REFERENCES jobs_curated(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_salary_history_job_id
    ON job_salary_history (job_id);

CREATE INDEX IF NOT EXISTS idx_job_salary_history_seen_at
    ON job_salary_history (seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_salary_history_currency
    ON job_salary_history (currency);

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_salary_history_job_run
    ON job_salary_history (job_id, run_id)
    WHERE run_id IS NOT NULL;


-- Снапшоты контента вакансии
CREATE TABLE IF NOT EXISTS job_content_snapshots (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    run_id TEXT,
    seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    title TEXT,
    title_normalized TEXT,
    description_hash TEXT,
    skills_normalized TEXT[] DEFAULT '{}',
    specialty TEXT,
    specialty_category TEXT,
    role_family TEXT,
    posting_language TEXT,
    content_hash TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_job_content_snapshots_job_id
        FOREIGN KEY (job_id) REFERENCES jobs_curated(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_job_id
    ON job_content_snapshots (job_id);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_seen_at
    ON job_content_snapshots (seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_title_normalized
    ON job_content_snapshots (title_normalized);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_specialty
    ON job_content_snapshots (specialty);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_specialty_category
    ON job_content_snapshots (specialty_category);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_role_family
    ON job_content_snapshots (role_family);

CREATE INDEX IF NOT EXISTS idx_job_content_snapshots_skills
    ON job_content_snapshots USING GIN (skills_normalized);

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_content_snapshots_job_run
    ON job_content_snapshots (job_id, run_id)
    WHERE run_id IS NOT NULL;


-- Триггеры для updated_at
CREATE OR REPLACE FUNCTION set_updated_at_support_tables()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_company_registry_set_updated_at ON company_registry;
CREATE TRIGGER trg_company_registry_set_updated_at
BEFORE UPDATE ON company_registry
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_support_tables();

DROP TRIGGER IF EXISTS trg_vector_sync_log_set_updated_at ON vector_sync_log;
CREATE TRIGGER trg_vector_sync_log_set_updated_at
BEFORE UPDATE ON vector_sync_log
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_support_tables();

COMMIT;
