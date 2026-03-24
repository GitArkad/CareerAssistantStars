-- ============================================================
-- AI Career Market Analyzer — Инициализация БД
-- Схема точно соответствует выходу parsing.py (pars.py)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ============================================================
-- 1. ВАКАНСИИ — по create_vacancy_record() из парсера
-- ============================================================

CREATE TABLE IF NOT EXISTS jobs_curated (
    id                   SERIAL PRIMARY KEY,
    ob_id                VARCHAR(64) UNIQUE NOT NULL,

    title                TEXT NOT NULL,
    description          TEXT,
    requirements         TEXT,
    responsibilities     TEXT,
    nice_to_have         TEXT,

    salary_from          INTEGER,
    salary_to            INTEGER,
    currency             VARCHAR(10),

    experience_level     TEXT,
    seniority_normalized VARCHAR(20),
    years_experience_min INTEGER,
    years_experience_max INTEGER,

    company_name         TEXT,
    industry             TEXT,
    company_size         TEXT,

    key_skills           TEXT[],
    skills_extracted     TEXT[],
    skills_normalized    TEXT[],
    tech_stack_tags      TEXT[],
    tools                TEXT[],
    methodologies        TEXT[],

    location             TEXT,
    country              TEXT,
    remote               BOOLEAN DEFAULT FALSE,
    employment_type      TEXT,

    source               VARCHAR(50) NOT NULL,
    url                  TEXT,
    search_query         TEXT,

    published_at         TEXT,
    parsed_at            TIMESTAMP,
    created_at           TIMESTAMP DEFAULT NOW(),
    updated_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_ob_id ON jobs_curated(ob_id);
CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs_curated(source);
CREATE INDEX IF NOT EXISTS idx_jobs_country ON jobs_curated(country);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs_curated(seniority_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_remote ON jobs_curated(remote);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs_curated(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm ON jobs_curated USING gin(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_jobs_skills ON jobs_curated USING gin(skills_extracted);
CREATE INDEX IF NOT EXISTS idx_jobs_tech ON jobs_curated USING gin(tech_stack_tags);
CREATE INDEX IF NOT EXISTS idx_jobs_key_skills ON jobs_curated USING gin(key_skills);

-- ============================================================
-- 2. СЛОВАРЬ НОРМАЛИЗАЦИИ НАВЫКОВ
-- ============================================================

CREATE TABLE IF NOT EXISTS skill_synonyms (
    id              SERIAL PRIMARY KEY,
    canonical_name  VARCHAR(100) NOT NULL,
    synonym         VARCHAR(200) NOT NULL,
    category        VARCHAR(50),
    UNIQUE(synonym)
);

CREATE INDEX IF NOT EXISTS idx_synonyms_synonym ON skill_synonyms(synonym);
CREATE INDEX IF NOT EXISTS idx_synonyms_canonical ON skill_synonyms(canonical_name);

INSERT INTO skill_synonyms (canonical_name, synonym, category) VALUES
    ('Python','python','programming'),('Java','java','programming'),
    ('JavaScript','javascript','programming'),('TypeScript','typescript','programming'),
    ('Scala','scala','programming'),('Go','go','programming'),('Go','golang','programming'),
    ('Rust','rust','programming'),('R','r','programming'),('C++','c++','programming'),
    ('SQL','sql','database'),('PostgreSQL','postgresql','database'),('PostgreSQL','postgres','database'),
    ('MySQL','mysql','database'),('MongoDB','mongodb','database'),('Redis','redis','database'),
    ('Elasticsearch','elasticsearch','database'),
    ('AWS','aws','cloud'),('Azure','azure','cloud'),('GCP','gcp','cloud'),('Cloud','cloud','cloud'),
    ('Docker','docker','devops'),('Kubernetes','kubernetes','devops'),('Kubernetes','k8s','devops'),
    ('Terraform','terraform','devops'),('Ansible','ansible','devops'),('Jenkins','jenkins','devops'),
    ('GitLab','gitlab','devops'),('GitHub','github','devops'),('Linux','linux','devops'),
    ('Bash','bash','devops'),('Shell','shell','devops'),
    ('Apache Spark','spark','bigdata'),('Apache Kafka','kafka','bigdata'),
    ('Apache Airflow','airflow','bigdata'),('dbt','dbt','bigdata'),
    ('Snowflake','snowflake','bigdata'),('BigQuery','bigquery','bigdata'),('Redshift','redshift','bigdata'),
    ('PyTorch','pytorch','ml'),('TensorFlow','tensorflow','ml'),('Scikit-learn','sklearn','ml'),
    ('Pandas','pandas','ml'),('NumPy','numpy','ml'),('SciPy','scipy','ml'),
    ('MLflow','mlflow','ml'),('DVC','dvc','ml'),('W&B','wandb','ml'),
    ('FastAPI','fastapi','web'),('Flask','flask','web'),('Django','django','web'),
    ('React','react','web'),('Vue','vue','web'),
    ('Git','git','tool'),('Jira','jira','tool'),('Confluence','confluence','tool'),
    ('Slack','slack','tool'),('Notion','notion','tool'),('Figma','figma','tool'),('Postman','postman','tool'),
    ('Power BI','power bi','bi'),('Power BI','powerbi','bi'),('Tableau','tableau','bi'),
    ('Looker','looker','bi'),('Metabase','metabase','bi'),('Apache Superset','superset','bi'),
    ('Excel','excel','bi'),
    ('Agile','agile','methodology'),('Scrum','scrum','methodology'),('Kanban','kanban','methodology'),
    ('DevOps','devops','methodology'),('MLOps','mlops','methodology'),
    ('CI/CD','ci/cd','methodology'),('CI/CD','cicd','methodology'),
    ('Microservices','microservices','methodology')
ON CONFLICT (synonym) DO NOTHING;

-- ============================================================
-- 3. РЫНОЧНЫЕ АГРЕГАТЫ (Airflow считает после парсинга)
-- ============================================================

CREATE TABLE IF NOT EXISTS market_skill_stats (
    id         SERIAL PRIMARY KEY,
    role       VARCHAR(200) NOT NULL,
    country    TEXT,
    skill_name VARCHAR(100) NOT NULL,
    demand_pct FLOAT,
    avg_salary INTEGER,
    job_count  INTEGER,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(role, country, skill_name)
);

CREATE TABLE IF NOT EXISTS salary_aggregates (
    id          SERIAL PRIMARY KEY,
    role        VARCHAR(200) NOT NULL,
    country     TEXT,
    seniority   VARCHAR(20),
    is_remote   BOOLEAN,
    p10 INTEGER, p25 INTEGER, p50 INTEGER, p75 INTEGER, p90 INTEGER,
    sample_size INTEGER,
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(role, country, seniority, is_remote)
);

CREATE TABLE IF NOT EXISTS market_role_stats (
    id             SERIAL PRIMARY KEY,
    role           VARCHAR(200) NOT NULL,
    country        TEXT,
    total_jobs     INTEGER,
    avg_experience FLOAT,
    remote_pct     FLOAT,
    competition    VARCHAR(20),
    updated_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(role, country)
);

-- ============================================================
-- 4. СЕССИИ ПОЛЬЗОВАТЕЛЕЙ (FastAPI)
-- ============================================================

CREATE TABLE IF NOT EXISTS resume_sessions (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    s3_path      TEXT NOT NULL,
    filename     VARCHAR(255),
    profile_json JSONB,
    params_json  JSONB,
    results_json JSONB,
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 5. СИМУЛЯЦИИ (FastAPI)
-- ============================================================

CREATE TABLE IF NOT EXISTS simulation_runs (
    id           SERIAL PRIMARY KEY,
    session_id   UUID NOT NULL REFERENCES resume_sessions(id) ON DELETE CASCADE,
    changes_json JSONB NOT NULL,
    before_json  JSONB NOT NULL,
    after_json   JSONB NOT NULL,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 6. ИНТЕРВЬЮ (FastAPI)
-- ============================================================

CREATE TABLE IF NOT EXISTS interview_runs (
    id          SERIAL PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES resume_sessions(id) ON DELETE CASCADE,
    questions   JSONB,
    answers     JSONB,
    total_score FLOAT,
    max_score   FLOAT,
    score_pct   FLOAT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 7. ЛОГИ ETL
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_runs (
    id                 SERIAL PRIMARY KEY,
    dag_id             VARCHAR(100) NOT NULL,
    run_date           DATE NOT NULL,
    source             VARCHAR(50),
    jobs_extracted     INTEGER DEFAULT 0,
    jobs_new           INTEGER DEFAULT 0,
    jobs_duplicates    INTEGER DEFAULT 0,
    embeddings_created INTEGER DEFAULT 0,
    aggregates_updated BOOLEAN DEFAULT FALSE,
    status             VARCHAR(20) DEFAULT 'running',
    error_message      TEXT,
    started_at         TIMESTAMP DEFAULT NOW(),
    finished_at        TIMESTAMP,
    duration_sec       INTEGER
);

-- ============================================================
-- 8. ТРИГГЕРЫ
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated BEFORE UPDATE ON jobs_curated
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_sessions_updated BEFORE UPDATE ON resume_sessions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- 9. VIEWS
-- ============================================================

CREATE OR REPLACE VIEW v_parsing_stats AS
SELECT source, country, COUNT(*) as total,
    COUNT(*) FILTER (WHERE salary_from IS NOT NULL) as with_salary,
    COUNT(*) FILTER (WHERE remote) as remote_count,
    COUNT(*) FILTER (WHERE seniority_normalized != 'unknown') as with_seniority,
    COUNT(*) FILTER (WHERE array_length(skills_extracted, 1) > 0) as with_skills
FROM jobs_curated GROUP BY source, country;

CREATE OR REPLACE VIEW v_top_skills AS
SELECT skill, COUNT(*) as job_count,
    ROUND(COUNT(*)::numeric / NULLIF((SELECT COUNT(*) FROM jobs_curated), 0) * 100, 1) as pct
FROM jobs_curated, unnest(skills_extracted) as skill
GROUP BY skill ORDER BY job_count DESC;

CREATE OR REPLACE VIEW v_top_skills_by_country AS
SELECT country, skill, COUNT(*) as job_count
FROM jobs_curated, unnest(skills_extracted) as skill
WHERE country IS NOT NULL
GROUP BY country, skill ORDER BY country, job_count DESC;

CREATE OR REPLACE VIEW v_salary_overview AS
SELECT country, seniority_normalized as seniority, COUNT(*) as sample_size,
    ROUND(AVG(salary_from)) as avg_from, ROUND(AVG(salary_to)) as avg_to,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary_from) as median
FROM jobs_curated WHERE salary_from IS NOT NULL AND salary_from > 0
GROUP BY country, seniority_normalized HAVING COUNT(*) >= 3;

-- ============================================================
-- ГОТОВО
-- ============================================================

DO $$ DECLARE t INTEGER; s INTEGER;
BEGIN
    SELECT COUNT(*) INTO t FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';
    SELECT COUNT(*) INTO s FROM skill_synonyms;
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Таблиц: %, Синонимов навыков: %', t, s;
    RAISE NOTICE '==========================================';
    RAISE NOTICE ' jobs_curated       — вакансии из парсера';
    RAISE NOTICE ' skill_synonyms     — словарь нормализации';
    RAISE NOTICE ' market_skill_stats — спрос на навыки';
    RAISE NOTICE ' salary_aggregates  — зарплаты';
    RAISE NOTICE ' market_role_stats  — метрики ролей';
    RAISE NOTICE ' resume_sessions    — сессии';
    RAISE NOTICE ' simulation_runs    — симуляции';
    RAISE NOTICE ' interview_runs     — интервью';
    RAISE NOTICE ' etl_runs           — логи ETL';
    RAISE NOTICE '==========================================';
END $$;