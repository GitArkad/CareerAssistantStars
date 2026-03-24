BEGIN;

INSERT INTO skill_synonyms (canonical_name, synonym, category) VALUES
    -- Programming languages
    ('Python','python','programming'),
    ('Python','py','programming'),
    ('Python','python3','programming'),
    ('Java','java','programming'),
    ('JavaScript','javascript','programming'),
    ('JavaScript','js','programming'),
    ('TypeScript','typescript','programming'),
    ('TypeScript','ts','programming'),
    ('Scala','scala','programming'),
    ('Go','go','programming'),
    ('Go','golang','programming'),
    ('Rust','rust','programming'),
    ('R','r','programming'),
    ('C++','c++','programming'),

    -- Databases
    ('SQL','sql','database'),
    ('PostgreSQL','postgresql','database'),
    ('PostgreSQL','postgres','database'),
    ('PostgreSQL','psql','database'),
    ('MySQL','mysql','database'),
    ('MongoDB','mongodb','database'),
    ('Redis','redis','database'),
    ('Elasticsearch','elasticsearch','database'),

    -- Cloud
    ('AWS','aws','cloud'),
    ('AWS','aws cloud','cloud'),
    ('Azure','azure','cloud'),
    ('GCP','gcp','cloud'),
    ('GCP','google cloud','cloud'),
    ('GCP','google cloud platform','cloud'),

    -- DevOps / Infra
    ('Docker','docker','devops'),
    ('Kubernetes','kubernetes','devops'),
    ('Kubernetes','k8s','devops'),
    ('Terraform','terraform','devops'),
    ('Ansible','ansible','devops'),
    ('Jenkins','jenkins','devops'),
    ('GitHub Actions','github actions','devops'),

    -- Big Data / Data Engineering
    ('Apache Spark','spark','bigdata'),
    ('Apache Spark','spark sql','bigdata'),
    ('Apache Spark','pyspark','bigdata'),
    ('Apache Kafka','kafka','bigdata'),
    ('Apache Airflow','airflow','bigdata'),
    ('Apache Airflow','air flow','bigdata'),
    ('dbt','dbt','bigdata'),
    ('dbt','dbt core','bigdata'),

    -- Machine Learning tools
    ('PyTorch','pytorch','ml'),
    ('TensorFlow','tensorflow','ml'),
    ('Scikit-learn','sklearn','ml'),
    ('Scikit-learn','scikit-learn','ml'),
    ('Scikit-learn','sci-kit learn','ml'),
    ('Pandas','pandas','ml'),
    ('NumPy','numpy','ml'),

    -- ML concepts
    ('Machine Learning','machine learning','ml_concept'),
    ('Deep Learning','deep learning','ml_concept'),
    ('Natural Language Processing','natural language processing','ml_concept'),
    ('Computer Vision','computer vision','ml_concept'),
    ('Large Language Models','large language models','ml_concept'),
    ('Retrieval Augmented Generation','retrieval augmented generation','ml_concept'),
    ('Retrieval Augmented Generation','rag','ml_concept'),
    ('Natural Language Processing','nlp','ml_concept'),
    ('Computer Vision','cv','ml_concept'),
    ('Large Language Models','llm','ml_concept'),

    -- Web / Backend
    ('FastAPI','fastapi','web'),
    ('Flask','flask','web'),
    ('Django','django','web'),
    ('React','react','web'),
    ('Vue','vue','web'),
    ('Node.js','node.js','web'),
    ('Node.js','nodejs','web'),
    ('Node.js','node','web'),

    -- Tools
    ('Git','git','tool'),
    ('Jira','jira','tool'),
    ('Confluence','confluence','tool'),
    ('Slack','slack','tool'),
    ('Notion','notion','tool'),
    ('Figma','figma','tool'),
    ('Postman','postman','tool'),

    -- BI / Analytics
    ('Power BI','power bi','bi'),
    ('Power BI','powerbi','bi'),
    ('Tableau','tableau','bi'),
    ('Looker','looker','bi'),
    ('Metabase','metabase','bi'),
    ('Excel','excel','bi'),
    ('Excel','ms excel','bi'),
    ('Excel','microsoft excel','bi'),

    -- Methodologies / Practices
    ('Agile','agile','methodology'),
    ('Scrum','scrum','methodology'),
    ('Kanban','kanban','methodology'),
    ('DevOps','devops','methodology'),
    ('MLOps','mlops','methodology'),
    ('CI/CD','ci/cd','methodology'),
    ('CI/CD','cicd','methodology'),
    ('CI/CD','ci-cd','methodology'),
    ('Microservices','microservices','methodology'),
    ('ETL','etl','methodology'),
    ('ELT','elt','methodology')

ON CONFLICT (synonym) DO UPDATE
SET canonical_name = EXCLUDED.canonical_name,
    category = EXCLUDED.category;

COMMIT;