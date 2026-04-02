BEGIN;

-- Справочник canonical skills
CREATE TABLE IF NOT EXISTS skills_dictionary (
    skill_id BIGSERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    category TEXT NOT NULL,
    parent_skill_id BIGINT NULL REFERENCES skills_dictionary(skill_id) ON DELETE SET NULL,
    description TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_skills_dictionary_canonical_name UNIQUE (canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_skills_dictionary_category
    ON skills_dictionary (category);

CREATE INDEX IF NOT EXISTS idx_skills_dictionary_parent_skill_id
    ON skills_dictionary (parent_skill_id);


-- Справочник canonical skills
CREATE TABLE IF NOT EXISTS skill_match_rules (
    synonym TEXT NOT NULL,
    skill_id BIGINT NOT NULL REFERENCES skills_dictionary(skill_id) ON DELETE CASCADE,
    match_type TEXT NOT NULL DEFAULT 'word_boundary',
    is_case_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    priority INT NOT NULL DEFAULT 100,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_skill_match_rules PRIMARY KEY (synonym),

    CONSTRAINT chk_skill_match_rules_match_type CHECK (
        match_type IN ('word_boundary', 'phrase', 'exact', 'contextual')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_skill_match_rules_lower_synonym
    ON skill_match_rules ((lower(btrim(synonym))));

CREATE INDEX IF NOT EXISTS idx_skill_match_rules_skill_id
    ON skill_match_rules (skill_id);

CREATE INDEX IF NOT EXISTS idx_skill_match_rules_priority
    ON skill_match_rules (priority);


-- Связь вакансий с навыками
CREATE TABLE IF NOT EXISTS job_skills (
    job_id TEXT NOT NULL REFERENCES jobs_curated(job_id) ON DELETE CASCADE,
    skill_id BIGINT NOT NULL REFERENCES skills_dictionary(skill_id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    confidence NUMERIC(5,4) NULL,
    is_required BOOLEAN NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_job_skills PRIMARY KEY (job_id, skill_id, source_type),

    CONSTRAINT chk_job_skills_source_type CHECK (
        source_type IN ('normalized_array', 'text_extract', 'key_skill', 'inferred')
    ),

    CONSTRAINT chk_job_skills_confidence CHECK (
        confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
    )
);

CREATE INDEX IF NOT EXISTS idx_job_skills_skill_id
    ON job_skills (skill_id);

CREATE INDEX IF NOT EXISTS idx_job_skills_job_id
    ON job_skills (job_id);

CREATE INDEX IF NOT EXISTS idx_job_skills_source_type
    ON job_skills (source_type);

CREATE INDEX IF NOT EXISTS idx_job_skills_is_required
    ON job_skills (is_required)
    WHERE is_required IS TRUE;


-- Триггер для updated_at
CREATE OR REPLACE FUNCTION set_updated_at_skill_layer()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_skills_dictionary_set_updated_at ON skills_dictionary;
CREATE TRIGGER trg_skills_dictionary_set_updated_at
BEFORE UPDATE ON skills_dictionary
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_skill_layer();

DROP TRIGGER IF EXISTS trg_skill_match_rules_set_updated_at ON skill_match_rules;
CREATE TRIGGER trg_skill_match_rules_set_updated_at
BEFORE UPDATE ON skill_match_rules
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_skill_layer();

DROP TRIGGER IF EXISTS trg_job_skills_set_updated_at ON job_skills;
CREATE TRIGGER trg_job_skills_set_updated_at
BEFORE UPDATE ON job_skills
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_skill_layer();


-- Начальный seed canonical skills
WITH seed(canonical_name, synonym, category, match_type, priority) AS (
    VALUES
        -- Programming
        ('Python','python','programming','word_boundary',100),
        ('Python','py','programming','contextual',120),
        ('Python','python3','programming','word_boundary',100),

        ('Java','java','programming','word_boundary',100),
        ('JavaScript','javascript','programming','word_boundary',100),
        ('JavaScript','js','programming','contextual',120),
        ('TypeScript','typescript','programming','word_boundary',100),
        ('TypeScript','ts','programming','contextual',120),
        ('Scala','scala','programming','word_boundary',100),
        ('Go','go','programming','contextual',120),
        ('Go','golang','programming','word_boundary',100),
        ('Rust','rust','programming','word_boundary',100),
        ('R','r','programming','contextual',130),
        ('C++','c++','programming','exact',100),
        ('C#','c#','programming','exact',100),
        ('Bash','bash','programming','word_boundary',100),
        ('Node.js','node.js','programming','exact',100),
        ('Node.js','nodejs','programming','word_boundary',100),

        -- Databases
        ('SQL','sql','database','word_boundary',100),
        ('PostgreSQL','postgresql','database','word_boundary',100),
        ('PostgreSQL','postgres','database','word_boundary',100),
        ('PostgreSQL','psql','database','word_boundary',110),
        ('MySQL','mysql','database','word_boundary',100),
        ('MongoDB','mongodb','database','word_boundary',100),
        ('Redis','redis','database','word_boundary',100),
        ('Elasticsearch','elasticsearch','database','word_boundary',100),
        ('Snowflake','snowflake','database','word_boundary',100),
        ('BigQuery','bigquery','database','word_boundary',100),
        ('BigQuery','google bigquery','database','phrase',100),

        -- Cloud
        ('AWS','aws','cloud','word_boundary',100),
        ('AWS','amazon web services','cloud','phrase',100),
        ('Azure','azure','cloud','word_boundary',100),
        ('GCP','gcp','cloud','word_boundary',100),
        ('GCP','google cloud','cloud','phrase',100),
        ('GCP','google cloud platform','cloud','phrase',100),

        -- DevOps / Infra
        ('Docker','docker','devops','word_boundary',100),
        ('Kubernetes','kubernetes','devops','word_boundary',100),
        ('Kubernetes','k8s','devops','word_boundary',100),
        ('Terraform','terraform','devops','word_boundary',100),
        ('Ansible','ansible','devops','word_boundary',100),
        ('Jenkins','jenkins','devops','word_boundary',100),
        ('GitHub Actions','github actions','devops','phrase',100),
        ('GitLab CI','gitlab ci','devops','phrase',100),

        -- Data / Big Data
        ('Apache Spark','spark','bigdata','word_boundary',100),
        ('Apache Spark','pyspark','bigdata','word_boundary',100),
        ('Apache Spark','spark sql','bigdata','phrase',100),
        ('Apache Kafka','kafka','bigdata','word_boundary',100),
        ('Apache Airflow','airflow','bigdata','word_boundary',100),
        ('dbt','dbt','bigdata','word_boundary',100),
        ('Databricks','databricks','bigdata','word_boundary',100),

        -- ML / Analytics
        ('PyTorch','pytorch','ml','word_boundary',100),
        ('TensorFlow','tensorflow','ml','word_boundary',100),
        ('Scikit-learn','scikit-learn','ml','word_boundary',100),
        ('Scikit-learn','sklearn','ml','word_boundary',100),
        ('Pandas','pandas','analytics','word_boundary',100),
        ('NumPy','numpy','analytics','word_boundary',100),
        ('XGBoost','xgboost','ml','word_boundary',100),
        ('LightGBM','lightgbm','ml','word_boundary',100),
        ('MLflow','mlflow','ml','word_boundary',100),

        -- Concepts
        ('Machine Learning','machine learning','ml_concept','phrase',100),
        ('Deep Learning','deep learning','ml_concept','phrase',100),
        ('Natural Language Processing','natural language processing','ml_concept','phrase',100),
        ('Natural Language Processing','nlp','ml_concept','word_boundary',100),
        ('Computer Vision','computer vision','ml_concept','phrase',100),
        ('Computer Vision','cv','ml_concept','contextual',130),
        ('Large Language Models','large language models','ml_concept','phrase',100),
        ('Large Language Models','llm','ml_concept','word_boundary',100),
        ('Retrieval Augmented Generation','retrieval augmented generation','ml_concept','phrase',100),
        ('Retrieval Augmented Generation','rag','ml_concept','word_boundary',100),
        ('Generative AI','generative ai','ml_concept','phrase',100),
        ('Prompt Engineering','prompt engineering','ml_concept','phrase',100),

        -- Web
        ('FastAPI','fastapi','web','word_boundary',100),
        ('Flask','flask','web','word_boundary',100),
        ('Django','django','web','word_boundary',100),
        ('React','react','web','word_boundary',100),
        ('Vue','vue','web','word_boundary',100),

        -- BI / Analytics
        ('Power BI','power bi','bi','phrase',100),
        ('Power BI','powerbi','bi','word_boundary',100),
        ('Tableau','tableau','bi','word_boundary',100),
        ('Looker','looker','bi','word_boundary',100),
        ('Metabase','metabase','bi','word_boundary',100),
        ('Excel','excel','bi','word_boundary',100),
        ('A/B Testing','a/b testing','analytics','phrase',100),
        ('A/B Testing','ab testing','analytics','phrase',100),
        ('Statistics','statistics','analytics','word_boundary',100),
        ('Data Visualization','data visualization','analytics','phrase',100),
        ('Product Analytics','product analytics','analytics','phrase',100),

        -- Tools / Methodologies
        ('Git','git','tool','word_boundary',100),
        ('Jira','jira','tool','word_boundary',100),
        ('Confluence','confluence','tool','word_boundary',100),
        ('Slack','slack','tool','word_boundary',100),
        ('Notion','notion','tool','word_boundary',100),
        ('Figma','figma','tool','word_boundary',100),
        ('Postman','postman','tool','word_boundary',100),

        ('Agile','agile','methodology','word_boundary',100),
        ('Scrum','scrum','methodology','word_boundary',100),
        ('Kanban','kanban','methodology','word_boundary',100),
        ('DevOps','devops','methodology','word_boundary',100),
        ('MLOps','mlops','methodology','word_boundary',100),
        ('CI/CD','ci/cd','methodology','exact',100),
        ('CI/CD','cicd','methodology','word_boundary',100),
        ('CI/CD','ci-cd','methodology','word_boundary',100),
        ('Microservices','microservices','methodology','word_boundary',100),
        ('ETL','etl','methodology','word_boundary',100),
        ('ELT','elt','methodology','word_boundary',100)
)
INSERT INTO skills_dictionary (canonical_name, category)
SELECT DISTINCT canonical_name, category
FROM seed
ON CONFLICT (canonical_name) DO UPDATE
SET category = EXCLUDED.category,
    updated_at = NOW();


-- Начальный seed правил матчинга
WITH seed(canonical_name, synonym, category, match_type, priority) AS (
    VALUES
        ('Python','python','programming','word_boundary',100),
        ('Python','py','programming','contextual',120),
        ('Python','python3','programming','word_boundary',100),

        ('Java','java','programming','word_boundary',100),
        ('JavaScript','javascript','programming','word_boundary',100),
        ('JavaScript','js','programming','contextual',120),
        ('TypeScript','typescript','programming','word_boundary',100),
        ('TypeScript','ts','programming','contextual',120),
        ('Scala','scala','programming','word_boundary',100),
        ('Go','go','programming','contextual',120),
        ('Go','golang','programming','word_boundary',100),
        ('Rust','rust','programming','word_boundary',100),
        ('R','r','programming','contextual',130),
        ('C++','c++','programming','exact',100),
        ('C#','c#','programming','exact',100),
        ('Bash','bash','programming','word_boundary',100),
        ('Node.js','node.js','programming','exact',100),
        ('Node.js','nodejs','programming','word_boundary',100),

        ('SQL','sql','database','word_boundary',100),
        ('PostgreSQL','postgresql','database','word_boundary',100),
        ('PostgreSQL','postgres','database','word_boundary',100),
        ('PostgreSQL','psql','database','word_boundary',110),
        ('MySQL','mysql','database','word_boundary',100),
        ('MongoDB','mongodb','database','word_boundary',100),
        ('Redis','redis','database','word_boundary',100),
        ('Elasticsearch','elasticsearch','database','word_boundary',100),
        ('Snowflake','snowflake','database','word_boundary',100),
        ('BigQuery','bigquery','database','word_boundary',100),
        ('BigQuery','google bigquery','database','phrase',100),

        ('AWS','aws','cloud','word_boundary',100),
        ('AWS','amazon web services','cloud','phrase',100),
        ('Azure','azure','cloud','word_boundary',100),
        ('GCP','gcp','cloud','word_boundary',100),
        ('GCP','google cloud','cloud','phrase',100),
        ('GCP','google cloud platform','cloud','phrase',100),

        ('Docker','docker','devops','word_boundary',100),
        ('Kubernetes','kubernetes','devops','word_boundary',100),
        ('Kubernetes','k8s','devops','word_boundary',100),
        ('Terraform','terraform','devops','word_boundary',100),
        ('Ansible','ansible','devops','word_boundary',100),
        ('Jenkins','jenkins','devops','word_boundary',100),
        ('GitHub Actions','github actions','devops','phrase',100),
        ('GitLab CI','gitlab ci','devops','phrase',100),

        ('Apache Spark','spark','bigdata','word_boundary',100),
        ('Apache Spark','pyspark','bigdata','word_boundary',100),
        ('Apache Spark','spark sql','bigdata','phrase',100),
        ('Apache Kafka','kafka','bigdata','word_boundary',100),
        ('Apache Airflow','airflow','bigdata','word_boundary',100),
        ('dbt','dbt','bigdata','word_boundary',100),
        ('Databricks','databricks','bigdata','word_boundary',100),

        ('PyTorch','pytorch','ml','word_boundary',100),
        ('TensorFlow','tensorflow','ml','word_boundary',100),
        ('Scikit-learn','scikit-learn','ml','word_boundary',100),
        ('Scikit-learn','sklearn','ml','word_boundary',100),
        ('Pandas','pandas','analytics','word_boundary',100),
        ('NumPy','numpy','analytics','word_boundary',100),
        ('XGBoost','xgboost','ml','word_boundary',100),
        ('LightGBM','lightgbm','ml','word_boundary',100),
        ('MLflow','mlflow','ml','word_boundary',100),

        ('Machine Learning','machine learning','ml_concept','phrase',100),
        ('Deep Learning','deep learning','ml_concept','phrase',100),
        ('Natural Language Processing','natural language processing','ml_concept','phrase',100),
        ('Natural Language Processing','nlp','ml_concept','word_boundary',100),
        ('Computer Vision','computer vision','ml_concept','phrase',100),
        ('Computer Vision','cv','ml_concept','contextual',130),
        ('Large Language Models','large language models','ml_concept','phrase',100),
        ('Large Language Models','llm','ml_concept','word_boundary',100),
        ('Retrieval Augmented Generation','retrieval augmented generation','ml_concept','phrase',100),
        ('Retrieval Augmented Generation','rag','ml_concept','word_boundary',100),
        ('Generative AI','generative ai','ml_concept','phrase',100),
        ('Prompt Engineering','prompt engineering','ml_concept','phrase',100),

        ('FastAPI','fastapi','web','word_boundary',100),
        ('Flask','flask','web','word_boundary',100),
        ('Django','django','web','word_boundary',100),
        ('React','react','web','word_boundary',100),
        ('Vue','vue','web','word_boundary',100),

        ('Power BI','power bi','bi','phrase',100),
        ('Power BI','powerbi','bi','word_boundary',100),
        ('Tableau','tableau','bi','word_boundary',100),
        ('Looker','looker','bi','word_boundary',100),
        ('Metabase','metabase','bi','word_boundary',100),
        ('Excel','excel','bi','word_boundary',100),
        ('A/B Testing','a/b testing','analytics','phrase',100),
        ('A/B Testing','ab testing','analytics','phrase',100),
        ('Statistics','statistics','analytics','word_boundary',100),
        ('Data Visualization','data visualization','analytics','phrase',100),
        ('Product Analytics','product analytics','analytics','phrase',100),

        ('Git','git','tool','word_boundary',100),
        ('Jira','jira','tool','word_boundary',100),
        ('Confluence','confluence','tool','word_boundary',100),
        ('Slack','slack','tool','word_boundary',100),
        ('Notion','notion','tool','word_boundary',100),
        ('Figma','figma','tool','word_boundary',100),
        ('Postman','postman','tool','word_boundary',100),

        ('Agile','agile','methodology','word_boundary',100),
        ('Scrum','scrum','methodology','word_boundary',100),
        ('Kanban','kanban','methodology','word_boundary',100),
        ('DevOps','devops','methodology','word_boundary',100),
        ('MLOps','mlops','methodology','word_boundary',100),
        ('CI/CD','ci/cd','methodology','exact',100),
        ('CI/CD','cicd','methodology','word_boundary',100),
        ('CI/CD','ci-cd','methodology','word_boundary',100),
        ('Microservices','microservices','methodology','word_boundary',100),
        ('ETL','etl','methodology','word_boundary',100),
        ('ELT','elt','methodology','word_boundary',100)
)
INSERT INTO skill_match_rules (synonym, skill_id, match_type, is_case_sensitive, priority)
SELECT
    s.synonym,
    sd.skill_id,
    s.match_type,
    FALSE,
    s.priority
FROM seed s
JOIN skills_dictionary sd
    ON sd.canonical_name = s.canonical_name
ON CONFLICT (synonym) DO UPDATE
SET skill_id = EXCLUDED.skill_id,
    match_type = EXCLUDED.match_type,
    is_case_sensitive = EXCLUDED.is_case_sensitive,
    priority = EXCLUDED.priority,
    updated_at = NOW();


-- Связи parent -> child для навыков
UPDATE skills_dictionary child
SET parent_skill_id = parent.skill_id,
    updated_at = NOW()
FROM skills_dictionary parent
WHERE child.parent_skill_id IS NULL
  AND (
      (child.canonical_name = 'Pandas' AND parent.canonical_name = 'Python')
   OR (child.canonical_name = 'NumPy' AND parent.canonical_name = 'Python')
   OR (child.canonical_name = 'PostgreSQL' AND parent.canonical_name = 'SQL')
   OR (child.canonical_name = 'MySQL' AND parent.canonical_name = 'SQL')
   OR (child.canonical_name = 'BigQuery' AND parent.canonical_name = 'SQL')
   OR (child.canonical_name = 'Power BI' AND parent.canonical_name = 'Data Visualization')
   OR (child.canonical_name = 'Tableau' AND parent.canonical_name = 'Data Visualization')
  );


-- Первичное заполнение job_skills из jobs_curated.skills_normalized
INSERT INTO job_skills (job_id, skill_id, source_type, confidence, is_required)
SELECT
    jc.job_id,
    sd.skill_id,
    'normalized_array' AS source_type,
    1.0000 AS confidence,
    NULL AS is_required
FROM jobs_curated jc
CROSS JOIN LATERAL unnest(COALESCE(jc.skills_normalized, '{}'::text[])) AS skill_name
JOIN skills_dictionary sd
    ON lower(sd.canonical_name) = lower(skill_name)
ON CONFLICT (job_id, skill_id, source_type) DO NOTHING;

COMMIT;