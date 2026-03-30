from __future__ import annotations

import re
import ast
import json
import html
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

try:
    from src.parsers.search_queries import ALL_QUERIES_COMBINED, CATEGORY_MAP
except Exception:
    ALL_QUERIES_COMBINED = []  
    CATEGORY_MAP = {}


# Справочник навыков
EXTRACTABLE_SKILLS = {
    # Languages
    "python", "java", "javascript", "typescript", "scala", "go", "golang",
    "rust", "ruby", "php", "c++", "c#", "swift", "kotlin",
    "perl", "lua", "haskell", "elixir", "clojure", "groovy", "dart",
    "objective-c", "matlab", "julia", "solidity",
    # Databases
    "sql", "postgresql", "postgres", "mysql", "mongodb", "redis",
    "elasticsearch", "cassandra", "dynamodb", "neo4j", "clickhouse",
    "mariadb", "oracle", "mssql", "sqlite", "couchdb", "cockroachdb",
    "timescaledb", "influxdb", "supabase", "firebase",
    # Cloud
    "aws", "azure", "gcp", "google cloud", "heroku", "digitalocean",
    "cloudflare", "vercel", "netlify",
    # DevOps / Infra
    "docker", "kubernetes", "k8s", "terraform", "ansible", "jenkins",
    "gitlab ci", "github actions", "circleci", "travisci", "argocd",
    "helm", "istio", "consul", "vault", "nginx", "apache", "caddy",
    "linux", "bash", "shell", "powershell",
    # Big Data
    "spark", "pyspark", "kafka", "airflow", "dbt", "snowflake",
    "bigquery", "redshift", "databricks", "flink", "nifi", "hive",
    "presto", "trino", "delta lake", "iceberg", "dask", "ray",
    "prefect", "dagster", "luigi", "mage",
    # ML / AI
    "pytorch", "tensorflow", "keras", "jax", "scikit-learn", "sklearn",
    "xgboost", "lightgbm", "catboost", "huggingface", "transformers",
    "langchain", "llamaindex", "openai api", "anthropic api",
    "stable diffusion", "onnx", "tensorrt",
    "pandas", "numpy", "scipy", "matplotlib", "seaborn", "plotly",
    "opencv", "pillow", "spacy", "nltk", "gensim",
    # MLOps
    "mlflow", "wandb", "dvc", "kubeflow", "sagemaker", "vertex ai",
    "bentoml", "seldon", "triton", "feast", "great expectations",
    # Web / Backend
    "fastapi", "flask", "django", "express", "spring", "spring boot",
    "rails", "laravel", "nextjs", "nuxt", "svelte", "remix",
    "graphql", "rest api", "grpc", "websocket",
    # Frontend
    "react", "vue", "angular", "svelte", "tailwind", "bootstrap",
    "webpack", "vite", "storybook",
    # Mobile
    "react native", "flutter", "swiftui", "jetpack compose",
    # Tools
    "git", "jira", "confluence", "figma", "postman", "swagger",
    "notion", "slack", "datadog", "grafana", "prometheus",
    "splunk", "elastic", "kibana", "sentry", "pagerduty",
    # BI
    "power bi", "tableau", "looker", "metabase", "superset",
    "excel", "google sheets",
    # Methodologies
    "agile", "scrum", "kanban", "devops", "mlops", "ci/cd",
    "microservices", "tdd", "bdd", "soa",
}

EXACT_MATCH_SKILLS = {"go", "c#", "c++", "rust", "dart", "ray", "dask", "helm", "vault", "feast", "ruby"}

R_LANGUAGE_CONTEXT_PATTERNS = [
    r"\br\s*(?:language|lang)\b",
    r"\br\s+(?:programming|studio|shiny|markdown|package|cran|tidyverse|ggplot|dplyr)\b",
    r"\b(?:experience|proficiency|knowledge|expertise|programming|coding|development|analytics|analysis|statistics|statistical|visualization)\s+(?:with|in|using)\s+r\b",
    r"\b(?:with|using|in)\s+r\s+(?:for|and|/|,)",
    r"\br\s*(?:/|,|and)\s*(?:python|sql|sas|matlab|scala)\b",
    r"\b(?:python|sql|sas|matlab|scala)\s*(?:/|,|and)\s*r\b",
    r"\br\s+for\s+(?:statistics|statistical|analytics|analysis|data science|data analysis|visualization)\b",
    r"\b(?:programming\s+in|experience\s+with|knowledge\s+of|proficiency\s+in)\s+r\b",
]


#  Импликации навыков: если знаешь X, то Y тоже подразумевается
SKILL_IMPLIES = {
    "pandas": ["python"],
    "numpy": ["python"],
    "scipy": ["python"],
    "matplotlib": ["python"],
    "seaborn": ["python"],
    "plotly": ["python"],
    "scikit-learn": ["python"],
    "sklearn": ["python"],
    "pytorch": ["python"],
    "tensorflow": ["python"],
    "keras": ["python"],
    "jax": ["python"],
    "xgboost": ["python"],
    "lightgbm": ["python"],
    "catboost": ["python"],
    "fastapi": ["python"],
    "flask": ["python"],
    "django": ["python"],
    "pyspark": ["python", "spark"],
    "airflow": ["python"],
    "dbt": ["sql"],
    "postgresql": ["sql"],
    "postgres": ["sql"],
    "mysql": ["sql"],
    "mssql": ["sql"],
    "sqlite": ["sql"],
    "mariadb": ["sql"],
    "oracle": ["sql"],
    "cockroachdb": ["sql"],
    "clickhouse": ["sql"],
    "timescaledb": ["sql"],
    "bigquery": ["sql"],
    "redshift": ["sql"],
    "snowflake": ["sql"],
    "presto": ["sql"],
    "trino": ["sql"],
    "hive": ["sql"],
    "react native": ["react", "javascript"],
    "nextjs": ["react", "javascript"],
    "nuxt": ["vue", "javascript"],
    "express": ["javascript", "node.js"],
    "vue": ["javascript"],
    "angular": ["javascript", "typescript"],
    "react": ["javascript"],
    "typescript": ["javascript"],
    "spring boot": ["java", "spring"],
    "spring": ["java"],
    "kotlin": ["java"],
    "rails": ["ruby"],
    "laravel": ["php"],
    "swiftui": ["swift"],
    "jetpack compose": ["kotlin"],
    "flutter": ["dart"],
    "k8s": ["kubernetes"],
    "golang": ["go"],
    "helm": ["kubernetes"],
    "istio": ["kubernetes"],
    "kubeflow": ["kubernetes", "python"],
    "sagemaker": ["aws", "python"],
    "vertex ai": ["gcp", "python"],
    "databricks": ["spark", "python", "sql"],
    "mlflow": ["python"],
    "wandb": ["python"],
    "langchain": ["python"],
    "llamaindex": ["python"],
    "huggingface": ["python"],
    "opencv": ["python"],
    "spacy": ["python"],
    "nltk": ["python"],
    "gensim": ["python"],
    "google cloud": ["gcp"],
    "github actions": ["git"],
    "gitlab ci": ["git"],
}


# Карта синонимов навыков -> каноническое имя
SKILL_SYNONYMS = {
    # --- Programming Languages ---
    "python": "Python", "py": "Python", "python3": "Python",
    "питон": "Python", "пайтон": "Python", "пайтн": "Python",
    "java": "Java", "джава": "Java",
    "javascript": "JavaScript", "js": "JavaScript",
    "джаваскрипт": "JavaScript", "жс": "JavaScript",
    "typescript": "TypeScript", "ts": "TypeScript", "тайпскрипт": "TypeScript",
    "scala": "Scala", "скала": "Scala",
    "go": "Go", "golang": "Go", "голанг": "Go",
    "rust": "Rust", "раст": "Rust",
    "r_lang": "R", "r": "R",
    "c++": "C++", "cpp": "C++", "си плюс плюс": "C++", "плюсы": "C++",
    "c#": "C#", "csharp": "C#", "c sharp": "C#", "си шарп": "C#",
    "swift": "Swift", "свифт": "Swift",
    "kotlin": "Kotlin", "котлин": "Kotlin",
    "ruby": "Ruby", "руби": "Ruby",
    "php": "PHP", "пхп": "PHP",
    "perl": "Perl", "перл": "Perl",
    "dart": "Dart", "дарт": "Dart",
    "julia": "Julia", "джулия": "Julia",
    "matlab": "MATLAB", "матлаб": "MATLAB",
    "lua": "Lua", "луа": "Lua",
    ".net": ".NET", "dotnet": ".NET", "дотнет": ".NET",
    "solidity": "Solidity",
    "objective-c": "Objective-C", "objc": "Objective-C",
    "haskell": "Haskell", "хаскель": "Haskell", "хаскелл": "Haskell",
    "elixir": "Elixir", "эликсир": "Elixir",
    "clojure": "Clojure", "кложур": "Clojure",
    "groovy": "Groovy", "груви": "Groovy",

    # --- Databases ---
    "sql": "SQL", "скл": "SQL", "скуэль": "SQL", "эскуэль": "SQL",
    "postgresql": "PostgreSQL", "postgres": "PostgreSQL",
    "постгрес": "PostgreSQL", "постгря": "PostgreSQL", "пг": "PostgreSQL", "pg": "PostgreSQL",
    "mysql": "MySQL", "мускул": "MySQL", "мускл": "MySQL",
    "mongodb": "MongoDB", "монго": "MongoDB", "монга": "MongoDB",
    "redis": "Redis", "редис": "Redis",
    "elasticsearch": "Elasticsearch", "эластик": "Elasticsearch", "elastic": "Elastic",
    "cassandra": "Cassandra", "кассандра": "Cassandra",
    "dynamodb": "DynamoDB", "динамо": "DynamoDB",
    "neo4j": "Neo4j",
    "clickhouse": "ClickHouse", "клик": "ClickHouse", "кликхаус": "ClickHouse", "ch": "ClickHouse",
    "sqlite": "SQLite",
    "mariadb": "MariaDB",
    "oracle": "Oracle", "оракл": "Oracle",
    "cockroachdb": "CockroachDB",
    "supabase": "Supabase", "firebase": "Firebase",
    "mssql": "MS SQL", "ms sql": "MS SQL", "sql server": "MS SQL",
    "timescaledb": "TimescaleDB", "influxdb": "InfluxDB",
    "couchdb": "CouchDB",

    # --- Cloud ---
    "aws": "AWS", "amazon web services": "AWS", "амазон": "AWS",
    "azure": "Azure", "азур": "Azure", "ажур": "Azure",
    "gcp": "GCP", "google cloud": "GCP", "гугл клауд": "GCP",
    "heroku": "Heroku", "digitalocean": "DigitalOcean",
    "cloudflare": "Cloudflare", "vercel": "Vercel", "netlify": "Netlify",

    # --- DevOps / Infra ---
    "docker": "Docker", "докер": "Docker",
    "kubernetes": "Kubernetes", "k8s": "Kubernetes",
    "кубер": "Kubernetes", "кубернетис": "Kubernetes", "кубернетес": "Kubernetes", "к8с": "Kubernetes",
    "terraform": "Terraform", "терраформ": "Terraform", "tf": "Terraform",
    "ansible": "Ansible", "ансибл": "Ansible", "ансибль": "Ansible",
    "jenkins": "Jenkins", "дженкинс": "Jenkins",
    "github actions": "GitHub Actions", "гитхаб экшнс": "GitHub Actions",
    "gitlab ci": "GitLab CI", "гитлаб": "GitLab CI",
    "circleci": "CircleCI", "argocd": "ArgoCD",
    "helm": "Helm", "хелм": "Helm",
    "istio": "Istio",
    "consul": "Consul", "vault": "Vault",
    "nginx": "Nginx", "энджинкс": "Nginx",
    "apache": "Apache", "caddy": "Caddy",
    "linux": "Linux", "линукс": "Linux", "линакс": "Linux",
    "bash": "Bash", "баш": "Bash",
    "shell": "Shell", "powershell": "PowerShell",

    # --- Big Data ---
    "spark": "Apache Spark", "pyspark": "Apache Spark",
    "спарк": "Apache Spark", "пайспарк": "Apache Spark",
    "kafka": "Apache Kafka", "кафка": "Apache Kafka",
    "airflow": "Apache Airflow", "эирфлоу": "Apache Airflow", "аирфлоу": "Apache Airflow",
    "dbt": "dbt",
    "snowflake": "Snowflake", "сноуфлейк": "Snowflake",
    "bigquery": "BigQuery", "бигквери": "BigQuery", "bq": "BigQuery",
    "redshift": "Redshift", "редшифт": "Redshift",
    "databricks": "Databricks", "датабрикс": "Databricks",
    "flink": "Apache Flink", "флинк": "Apache Flink",
    "hive": "Hive", "хайв": "Hive",
    "presto": "Presto", "trino": "Trino",
    "delta lake": "Delta Lake", "iceberg": "Iceberg",
    "dask": "Dask", "ray": "Ray",
    "prefect": "Prefect", "dagster": "Dagster",
    "luigi": "Luigi", "mage": "Mage", "nifi": "NiFi",

    # --- ML / AI ---
    "pytorch": "PyTorch", "пайторч": "PyTorch", "торч": "PyTorch",
    "tensorflow": "TensorFlow", "тензорфлоу": "TensorFlow",
    "keras": "Keras", "керас": "Keras",
    "jax": "JAX",
    "scikit-learn": "Scikit-learn", "sklearn": "Scikit-learn",
    "скайлерн": "Scikit-learn", "сайкитлерн": "Scikit-learn",
    "xgboost": "XGBoost", "lightgbm": "LightGBM", "catboost": "CatBoost",
    "huggingface": "Hugging Face", "хаггингфейс": "Hugging Face", "hf": "Hugging Face",
    "langchain": "LangChain", "лангчейн": "LangChain",
    "llamaindex": "LlamaIndex",
    "opencv": "OpenCV",
    "spacy": "spaCy", "спейси": "spaCy",
    "nltk": "NLTK",
    "gensim": "Gensim",
    "pandas": "Pandas", "пандас": "Pandas", "pd": "Pandas",
    "numpy": "NumPy", "нампай": "NumPy", "np": "NumPy",
    "scipy": "SciPy", "сайпай": "SciPy",
    "matplotlib": "Matplotlib", "матплотлиб": "Matplotlib", "plt": "Matplotlib",
    "seaborn": "Seaborn", "сиборн": "Seaborn",
    "plotly": "Plotly",
    "transformers": "Transformers",
    "stable diffusion": "Stable Diffusion",
    "onnx": "ONNX", "tensorrt": "TensorRT",
    "pillow": "Pillow",

    # --- MLOps ---
    "mlflow": "MLflow", "wandb": "W&B", "dvc": "DVC",
    "kubeflow": "Kubeflow", "кубефлоу": "Kubeflow",
    "sagemaker": "SageMaker", "сейджмейкер": "SageMaker",
    "vertex ai": "Vertex AI",
    "bentoml": "BentoML", "seldon": "Seldon", "triton": "Triton",
    "feast": "Feast", "great expectations": "Great Expectations",

    # --- Web / Backend ---
    "fastapi": "FastAPI", "фастапи": "FastAPI",
    "flask": "Flask", "фласк": "Flask",
    "django": "Django", "джанго": "Django",
    "express": "Express", "экспресс": "Express",
    "spring": "Spring", "спринг": "Spring",
    "spring boot": "Spring Boot", "спринг бут": "Spring Boot",
    "rails": "Rails", "рельсы": "Rails",
    "laravel": "Laravel", "ларавел": "Laravel",
    "nextjs": "Next.js", "next.js": "Next.js", "некст": "Next.js",
    "nuxt": "Nuxt", "nuxtjs": "Nuxt",
    "svelte": "Svelte", "remix": "Remix",
    "graphql": "GraphQL", "графкл": "GraphQL",
    "rest api": "REST API", "рест апи": "REST API", "restful": "REST API",
    "grpc": "gRPC",
    "websocket": "WebSocket", "вебсокет": "WebSocket",

    # --- Frontend ---
    "react": "React", "реакт": "React", "reactjs": "React",
    "vue": "Vue", "вью": "Vue", "vuejs": "Vue",
    "angular": "Angular", "ангуляр": "Angular", "ангулар": "Angular",
    "tailwind": "Tailwind CSS", "тейлвинд": "Tailwind CSS",
    "bootstrap": "Bootstrap", "бутстрап": "Bootstrap",
    "webpack": "Webpack", "вебпак": "Webpack",
    "vite": "Vite", "storybook": "Storybook",

    # --- Mobile ---
    "react native": "React Native", "react-native": "React Native",
    "flutter": "Flutter", "флаттер": "Flutter",
    "swiftui": "SwiftUI", "jetpack compose": "Jetpack Compose",

    # --- Tools ---
    "git": "Git", "гит": "Git",
    "jira": "Jira", "джира": "Jira",
    "confluence": "Confluence", "конфлюенс": "Confluence",
    "figma": "Figma", "фигма": "Figma",
    "postman": "Postman", "постман": "Postman",
    "notion": "Notion", "ноушен": "Notion", "ноушн": "Notion",
    "slack": "Slack", "слак": "Slack",
    "swagger": "Swagger", "свагер": "Swagger",
    "datadog": "Datadog", "grafana": "Grafana", "графана": "Grafana",
    "prometheus": "Prometheus", "прометеус": "Prometheus",
    "splunk": "Splunk", "kibana": "Kibana",
    "sentry": "Sentry", "сентри": "Sentry",
    "pagerduty": "PagerDuty",
    "superset": "Superset",

    # --- BI ---
    "power bi": "Power BI", "пауэр би": "Power BI", "powerbi": "Power BI",
    "tableau": "Tableau", "табло": "Tableau",
    "looker": "Looker", "лукер": "Looker",
    "metabase": "Metabase", "метабейс": "Metabase",
    "excel": "Excel", "эксель": "Excel", "ms excel": "Excel",
    "google sheets": "Google Sheets", "гугл таблицы": "Google Sheets",

    # --- Methodologies ---
    "agile": "Agile", "эджайл": "Agile", "аджайл": "Agile",
    "scrum": "Scrum", "скрам": "Scrum",
    "kanban": "Kanban", "канбан": "Kanban",
    "devops": "DevOps", "девопс": "DevOps",
    "mlops": "MLOps", "млопс": "MLOps",
    "ci/cd": "CI/CD", "cicd": "CI/CD", "ci-cd": "CI/CD",
    "microservices": "Microservices", "микросервисы": "Microservices",
    "tdd": "TDD", "bdd": "BDD", "soa": "SOA",
    "etl": "ETL", "elt": "ELT",

    # --- Concepts (from resumes) ---
    "machine learning": "Machine Learning", "ml": "Machine Learning",
    "машинное обучение": "Machine Learning", "мл": "Machine Learning",
    "deep learning": "Deep Learning", "dl": "Deep Learning",
    "глубокое обучение": "Deep Learning",
    "natural language processing": "NLP", "nlp": "NLP",
    "обработка естественного языка": "NLP", "нлп": "NLP",
    "computer vision": "Computer Vision", "cv": "Computer Vision",
    "компьютерное зрение": "Computer Vision",
    "large language models": "LLM", "llm": "LLM", "llms": "LLM",
    "большие языковые модели": "LLM", "бям": "LLM",
    "generative ai": "Generative AI", "genai": "Generative AI",
    "генеративный ии": "Generative AI",
    "retrieval augmented generation": "RAG", "rag": "RAG",
    "prompt engineering": "Prompt Engineering",
    "промпт инжиниринг": "Prompt Engineering",
    "data science": "Data Science", "ds": "Data Science",
    "дата сайенс": "Data Science",
    "data engineering": "Data Engineering", "de": "Data Engineering",
    "дата инжиниринг": "Data Engineering",
    "data analysis": "Data Analysis", "аналитика данных": "Data Analysis",
    "a/b testing": "A/B Testing", "а/б тестирование": "A/B Testing",
    "feature engineering": "Feature Engineering",
    "time series": "Time Series", "временные ряды": "Time Series",
    "recommender systems": "Recommender Systems",
    "рекомендательные системы": "Recommender Systems",

    # --- Node.js (special) ---
    "node.js": "Node.js", "nodejs": "Node.js", "нода": "Node.js", "ноджс": "Node.js",
}

# Символы / слова валют
CURRENCY_SYMBOLS = {
    "c$": "CAD", "a$": "AUD", "s$": "SGD", "hk$": "HKD",
    "us$": "USD", "£": "GBP", "€": "EUR", "$": "USD",
    "₽": "RUB", "₹": "INR", "zł": "PLN", "₴": "UAH",
    "₸": "KZT", "¥": "JPY", "₩": "KRW", "₪": "ILS", "₺": "TRY",
}

CURRENCY_WORDS = {
    "usd": "USD", "eur": "EUR", "gbp": "GBP", "cad": "CAD",
    "aud": "AUD", "sgd": "SGD", "hkd": "HKD", "inr": "INR",
    "rub": "RUB", "rur": "RUB", "руб": "RUB", "рублей": "RUB",
    "pln": "PLN", "uah": "UAH", "kzt": "KZT", "jpy": "JPY",
    "yen": "JPY", "krw": "KRW", "won": "KRW", "cny": "CNY",
    "rmb": "CNY", "yuan": "CNY", "brl": "BRL", "mxn": "MXN",
    "ils": "ILS", "nis": "ILS", "shekel": "ILS", "shekels": "ILS",
    "try": "TRY", "aed": "AED", "chf": "CHF", "sek": "SEK",
    "nok": "NOK", "dkk": "DKK", "nzd": "NZD", "czk": "CZK",
    "ron": "RON", "huf": "HUF", "bgn": "BGN", "zar": "ZAR",
}

CURRENCY_NORMALIZATION = {
    "RUR": "RUB", "BYR": "BYN", "US$": "USD",
    "C$": "CAD", "A$": "AUD",  "S$": "SGD",
    "HK$": "HKD", "NIS": "ILS",
}

# Нормализует код валюты к единому формату
def _normalize_currency_code(cur: Optional[str]) -> Optional[str]:
    if _is_missing(cur):
        return None
    c = str(cur).strip().upper()
    return CURRENCY_NORMALIZATION.get(c, c)


# Карта стран
_COUNTRY_MAP = {
    "РОССИЯ": "RUSSIA",
    "RUSSIA": "RUSSIA",

    "UNITED KINGDOM": "UNITED KINGDOM",
    "UK": "UNITED KINGDOM",

    "GERMANY": "GERMANY",
    "FRANCE": "FRANCE",
    "NETHERLANDS": "NETHERLANDS",
    "POLAND": "POLAND",
    "CANADA": "CANADA",
    "AUSTRALIA": "AUSTRALIA",
    "INDIA": "INDIA",
    "SINGAPORE": "SINGAPORE",

    "UNITED STATES": "UNITED STATES",
    "USA": "UNITED STATES",
    "US": "UNITED STATES",

    "КАЗАХСТАН": "KAZAKHSTAN",
    "KAZAKHSTAN": "KAZAKHSTAN",

    "БЕЛАРУСЬ": "BELARUS",
    "BELARUS": "BELARUS",

    "УКРАИНА": "UKRAINE",
    "UKRAINE": "UKRAINE",

    "УЗБЕКИСТАН": "UZBEKISTAN",
    "UZBEKISTAN": "UZBEKISTAN",

    "BRAZIL": "BRAZIL",
    "IRELAND": "IRELAND",
    "ITALY": "ITALY",
    "SPAIN": "SPAIN",
    "JAPAN": "JAPAN",
    "ISRAEL": "ISRAEL",
    "SWEDEN": "SWEDEN",
    "SWITZERLAND": "SWITZERLAND",
    "BELGIUM": "BELGIUM",
    "GREECE": "GREECE",
    "MEXICO": "MEXICO",
    "CHINA": "CHINA",
    "SOUTH KOREA": "SOUTH KOREA",
    "DENMARK": "DENMARK",
    "NORWAY": "NORWAY",
    "FINLAND": "FINLAND",
    "AUSTRIA": "AUSTRIA",
    "PORTUGAL": "PORTUGAL",
    "CZECH REPUBLIC": "CZECH REPUBLIC",
    "CZECHIA": "CZECH REPUBLIC",
    "ROMANIA": "ROMANIA",
    "HUNGARY": "HUNGARY",
    "TURKEY": "TURKEY",
    "UAE": "UAE",
    "UNITED ARAB EMIRATES": "UAE",
    "NEW ZEALAND": "NEW ZEALAND",
    "BULGARIA": "BULGARIA",
    "SOUTH AFRICA": "SOUTH AFRICA",
    "TAIWAN": "TAIWAN",
    "HONG KONG": "HONG KONG",
}

_LOCATION_COUNTRY_HINTS = {
    "united states": "UNITED STATES",
    "usa": "UNITED STATES",
    "u.s.": "UNITED STATES",
    "u.s.a.": "UNITED STATES",
    "us": "UNITED STATES",
    "us -": "UNITED STATES",

    "canada": "CANADA",

    "united kingdom": "UNITED KINGDOM",
    "uk": "UNITED KINGDOM",
    "u.k.": "UNITED KINGDOM",
    "england": "UNITED KINGDOM",
    "scotland": "UNITED KINGDOM",
    "great britain": "UNITED KINGDOM",

    "germany": "GERMANY",
    "france": "FRANCE",
    "netherlands": "NETHERLANDS",
    "poland": "POLAND",
    "australia": "AUSTRALIA",
    "singapore": "SINGAPORE",
    "india": "INDIA",

    "россия": "RUSSIA",
    "russia": "RUSSIA",

    "казахстан": "KAZAKHSTAN",
    "kazakhstan": "KAZAKHSTAN",

    "беларусь": "BELARUS",
    "belarus": "BELARUS",

    "украина": "UKRAINE",
    "ukraine": "UKRAINE",

    "узбекистан": "UZBEKISTAN",
    "uzbekistan": "UZBEKISTAN",

    "brazil": "BRAZIL",
    "ireland": "IRELAND",
    "italy": "ITALY",
    "spain": "SPAIN",
    "japan": "JAPAN",
    "israel": "ISRAEL",
    "switzerland": "SWITZERLAND",
    "belgium": "BELGIUM",
    "greece": "GREECE",
    "mexico": "MEXICO",
    "denmark": "DENMARK",
    "norway": "NORWAY",
    "sweden": "SWEDEN",
    "finland": "FINLAND",
    "austria": "AUSTRIA",
    "new zealand": "NEW ZEALAND",
    "bulgaria": "BULGARIA",
    "south africa": "SOUTH AFRICA",
    "taiwan": "TAIWAN",
    "hong kong": "HONG KONG",

    "china": "CHINA",
    "portugal": "PORTUGAL",
    "romania": "ROMANIA",
    "hungary": "HUNGARY",
    "turkey": "TURKEY",
    "uae": "UAE",
    "united arab emirates": "UAE",
    "czech republic": "CZECH REPUBLIC",
    "czechia": "CZECH REPUBLIC",
    "south korea": "SOUTH KOREA",
}

US_STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}

CA_PROVINCES = {
    "british columbia", "alberta", "ontario", "quebec", "québec",
    "manitoba", "saskatchewan", "nova scotia", "new brunswick",
    "newfoundland and labrador", "prince edward island"
}
CA_PROVINCE_ABBR = {"BC", "AB", "ON", "QC", "MB", "SK", "NS", "NB", "NL", "PE"}

LOCATION_REGION_NOISE = {
    "emea", "apac", "latam", "global", "worldwide", "anywhere",
    "multiple locations", "various locations"
}

KNOWN_COUNTRIES_LOWER = set(_LOCATION_COUNTRY_HINTS.keys()) | {
    v.lower() for v in _COUNTRY_MAP.values()
} | {"usa", "uk", "us", "uae"}

LOCATION_NOISE_WORDS = {
    "remote", "hybrid", "onsite", "on-site", "office", "in-office",
    "distributed", "worldwide", "anywhere", "global", "work from home",
    "удалённо", "удаленно", "удаленка", "гибрид", "офис",
    "nan", "none", "null", "n/a",
}

# Определение страны по компании (fallback)
COMPANY_COUNTRY_MAP = {
    "airbnb": "UNITED STATES",
    "andurilindustries": "UNITED STATES",
    "axon": "UNITED STATES",
    "braze": "UNITED STATES",
    "calendly": "UNITED STATES",
    "cloudflare": "UNITED STATES",
    "coinbase": "UNITED STATES",
    "databricks": "UNITED STATES",
    "fanduel": "UNITED STATES",
    "hubspotjobs": "UNITED STATES",
    "lyft": "UNITED STATES",
    "mongodb": "UNITED STATES",
    "pinterest": "UNITED STATES",
    "samsara": "UNITED STATES",
    "sofi": "UNITED STATES",
    "waymo": "UNITED STATES",
    "openai": "UNITED STATES",
    "ramp": "UNITED STATES",
    "palantir": "UNITED STATES",
    "binance": "SINGAPORE",
    "wheely": "UNITED KINGDOM",
    "kayzen": "GERMANY",
}

# Русские города -> English
RU_CITY_TO_EN = {
    # --- Россия / СНГ ---
    "москва": "MOSCOW",
    "moscow": "MOSCOW",
    "msk": "MOSCOW",
    "мск": "MOSCOW",

    "санкт-петербург": "SAINT PETERSBURG",
    "st petersburg": "SAINT PETERSBURG",
    "петербург": "SAINT PETERSBURG",
    "спб": "SAINT PETERSBURG",
    "spb": "SAINT PETERSBURG",

    "новосибирск": "NOVOSIBIRSK",
    "nsk": "NOVOSIBIRSK",
    "нск": "NOVOSIBIRSK",

    "екатеринбург": "YEKATERINBURG",
    "yekaterinburg": "YEKATERINBURG",
    "екб": "YEKATERINBURG",

    "нижний новгород": "NIZHNY NOVGOROD",
    "казань": "KAZAN",
    "челябинск": "CHELYABINSK",
    "омск": "OMSK",
    "самара": "SAMARA",
    "ростов-на-дону": "ROSTOV-ON-DON",
    "уфа": "UFA",
    "красноярск": "KRASNOYARSK",
    "воронеж": "VORONEZH",
    "пермь": "PERM",
    "волгоград": "VOLGOGRAD",
    "краснодар": "KRASNODAR",
    "саратов": "SARATOV",
    "тюмень": "TYUMEN",
    "иркутск": "IRKUTSK",
    "хабаровск": "KHABAROVSK",
    "владивосток": "VLADIVOSTOK",
    "томск": "TOMSK",
    "калининград": "KALININGRAD",
    "сочи": "SOCHI",
    "ярославль": "YAROSLAVL",
    "тула": "TULA",
    "курск": "KURSK",
    "мурманск": "MURMANSK",
    "якутск": "YAKUTSK",

    "алматы": "ALMATY",
    "almaty": "ALMATY",
    "минск": "MINSK",
    "minsk": "MINSK",
    "витебск": "VITEBSK",
    "vitebsk": "VITEBSK",

    # --- США ---
    "new york": "NEW YORK",
    "new york city": "NEW YORK",
    "nyc": "NEW YORK",
    "ny": "NEW YORK",
    "нью-йорк": "NEW YORK",

    "san francisco": "SAN FRANCISCO",
    "sf": "SAN FRANCISCO",
    "сан-франциско": "SAN FRANCISCO",

    "austin": "AUSTIN",
    "остин": "AUSTIN",

    "seattle": "SEATTLE",
    "сиэтл": "SEATTLE",

    "palo alto": "PALO ALTO",
    "mountain view": "MOUNTAIN VIEW",

    "los angeles": "LOS ANGELES",
    "la": "LOS ANGELES",

    "boston": "BOSTON",
    "бостон": "BOSTON",

    "chicago": "CHICAGO",
    "чикаго": "CHICAGO",

    "atlanta": "ATLANTA",
    "dallas": "DALLAS",
    "denver": "DENVER",
    "miami": "MIAMI",
    "houston": "HOUSTON",
    "philadelphia": "PHILADELPHIA",
    "pittsburgh": "PITTSBURGH",
    "san diego": "SAN DIEGO",
    "san jose": "SAN JOSE",
    "salt lake city": "SALT LAKE CITY",
    "jersey city": "JERSEY CITY",
    "raleigh": "RALEIGH",
    "arlington": "ARLINGTON",
    "cambridge": "CAMBRIDGE",
    "honolulu": "HONOLULU",
    "provo": "PROVO",
    "frisco": "FRISCO",
    "plano": "PLANO",
    "sunnyvale": "SUNNYVALE",
    "bellevue": "BELLEVUE",
    "kirkland": "KIRKLAND",
    "ann arbor": "ANN ARBOR",
    "berkeley": "BERKELEY",
    "saint louis": "SAINT LOUIS",
    "st louis": "SAINT LOUIS",
    "tysons": "TYSONS",
    "cottonwood heights": "COTTONWOOD HEIGHTS",

    # --- Канада ---
    "toronto": "TORONTO",
    "торонто": "TORONTO",
    "vancouver": "VANCOUVER",
    "ванкувер": "VANCOUVER",
    "montreal": "MONTREAL",
    "монреаль": "MONTREAL",
    "ottawa": "OTTAWA",
    "оттава": "OTTAWA",
    "north vancouver": "NORTH VANCOUVER",
    "new westminster": "NEW WESTMINSTER",
    "mississauga": "MISSISSAUGA",

    # --- Европа ---
    "london": "LONDON",
    "лондон": "LONDON",
    "berlin": "BERLIN",
    "берлин": "BERLIN",
    "amsterdam": "AMSTERDAM",
    "амстердам": "AMSTERDAM",
    "paris": "PARIS",
    "париж": "PARIS",
    "warsaw": "WARSAW",
    "warszawa": "WARSAW",
    "варшава": "WARSAW",
    "belgrade": "BELGRADE",
    "beograd": "BELGRADE",
    "белград": "BELGRADE",
    "lisbon": "LISBON",
    "лиссабон": "LISBON",
    "limassol": "LIMASSOL",
    "лимасол": "LIMASSOL",
    "tbilisi": "TBILISI",
    "тбилиси": "TBILISI",
    "yerevan": "YEREVAN",
    "ереван": "YEREVAN",

    "dublin": "DUBLIN",
    "madrid": "MADRID",
    "barcelona": "BARCELONA",
    "munich": "MUNICH",
    "frankfurt": "FRANKFURT",
    "hamburg": "HAMBURG",
    "cologne": "COLOGNE",
    "zurich": "ZURICH",
    "geneva": "GENEVA",
    "stockholm": "STOCKHOLM",
    "prague": "PRAGUE",
    "porto": "PORTO",
    "sofia": "SOFIA",
    "bucharest": "BUCHAREST",
    "ghent": "GHENT",
    "edinburgh": "EDINBURGH",
    "manchester": "MANCHESTER",
    "leeds": "LEEDS",
    "marseille": "MARSEILLE",
    "toulouse": "TOULOUSE",
    "stuttgart": "STUTTGART",
    "thessaloniki": "THESSALONIKI",
    "utrecht": "UTRECHT",
    "aarhus": "AARHUS",
    "casablanca": "CASABLANCA",

    # --- Азия / Ближний Восток ---
    "dubai": "DUBAI",
    "дубай": "DUBAI",
    "singapore": "SINGAPORE",
    "сингапур": "SINGAPORE",
    "tel aviv": "TEL AVIV",
    "тел-авив": "TEL AVIV",
    "bangkok": "BANGKOK",
    "бангкок": "BANGKOK",
    "seoul": "SEOUL",
    "сеул": "SEOUL",
    "tokyo": "TOKYO",
    "токио": "TOKYO",

    "bangalore": "BANGALORE",
    "bengaluru": "BENGALURU",
    "mumbai": "MUMBAI",
    "delhi": "DELHI",
    "gurgaon": "GURGAON",
    "gurugram": "GURUGRAM",
    "noida": "NOIDA",
    "pune": "PUNE",
    "hyderabad": "HYDERABAD",
    "chennai": "CHENNAI",
    "shenzhen": "SHENZHEN",
    "shanghai": "SHANGHAI",
    "taipei": "TAIPEI",
    "taipei city": "TAIPEI",
    "kuala lumpur": "KUALA LUMPUR",
    "abu dhabi": "ABU DHABI",
    "doha": "DOHA",
    "ho chi minh city": "HO CHI MINH CITY",
    "herzliya": "HERZLIYA",

    # --- Австралия / Новая Зеландия / Африка / ЛатАм ---
    "sydney": "SYDNEY",
    "melbourne": "MELBOURNE",
    "brisbane": "BRISBANE",
    "canberra": "CANBERRA",
    "auckland": "AUCKLAND",
    "cape town": "CAPE TOWN",
    "mexico city": "MEXICO CITY",
    "monterrey": "MONTERREY",
    "sao paulo": "SAO PAULO",
    "são paulo": "SAO PAULO",
}

# US state -> UNITED STATES
US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia",
}


# Fallback URL для источников без прямых ссылок
SOURCE_FALLBACK_URLS = {
    "arbeitnow.com": "https://www.arbeitnow.com",
}

COUNTRY_DEFAULT_CURRENCY = {
    "UNITED STATES": "USD",
    "CANADA": "CAD",
    "UNITED KINGDOM": "GBP",
    "GERMANY": "EUR",
    "FRANCE": "EUR",
    "NETHERLANDS": "EUR",
    "IRELAND": "EUR",
    "ITALY": "EUR",
    "SPAIN": "EUR",
    "BELGIUM": "EUR",
    "AUSTRIA": "EUR",
    "FINLAND": "EUR",
    "PORTUGAL": "EUR",
    "GREECE": "EUR",
    "POLAND": "PLN",
    "CZECH REPUBLIC": "CZK",
    "CZECHIA": "CZK",
    "ROMANIA": "RON",
    "HUNGARY": "HUF",
    "AUSTRALIA": "AUD",
    "NEW ZEALAND": "NZD",
    "INDIA": "INR",
    "SINGAPORE": "SGD",
    "JAPAN": "JPY",
    "SOUTH KOREA": "KRW",
    "CHINA": "CNY",
    "BRAZIL": "BRL",
    "MEXICO": "MXN",
    "ISRAEL": "ILS",
    "TURKEY": "TRY",
    "UAE": "AED",
    "UNITED ARAB EMIRATES": "AED",
    "SWITZERLAND": "CHF",
    "SWEDEN": "SEK",
    "NORWAY": "NOK",
    "DENMARK": "DKK",
    "RUSSIA": "RUB",
    "KAZAKHSTAN": "KZT",
    "BELARUS": "BYN",
    "UKRAINE": "UAH",
    "UZBEKISTAN": "UZS",
    "HONG KONG": "HKD",
    "TAIWAN": "TWD",
    "SOUTH AFRICA": "ZAR",
    "BULGARIA": "BGN",
}

FALLBACK_TO_RUB_RATES = {
    "RUB": 1.0,
    "USD": 82.1314,
    "EUR": 95.0038,
    "GBP": 109.8261,
    "CAD": 59.5112,
    "AUD": 57.1224,
    "SGD": 64.0601,
    "INR": 0.874057,
    "JPY": 0.515156,
    "KRW": 0.0549154,
    "CNY": 11.9001,
    "BRL": 15.7123,
    "TRY": 1.85341,
    "AED": 22.3639,
    "CHF": 103.6882,
    "SEK": 8.83877,
    "NOK": 8.43619,
    "DKK": 12.7422,
    "PLN": 22.2289,
    "KZT": 0.170281,
    "BYN": 27.6984,
    "UAH": 1.87206,
    "UZS": 0.00674321,
    "NZD": 47.6609,
    "HKD": 10.5162,
}

MONTHLY_BOUNDS = {
    "USD": (1_000, 30_000),
    "EUR": (1_000, 28_000),
    "GBP": (1_000, 25_000),
    "CAD": (1_200, 35_000),
    "AUD": (1_200, 35_000),
    "SGD": (1_200, 30_000),
    "INR": (8_000, 800_000),
    "RUB": (18_000, 1_000_000),
    "PLN": (2_500, 50_000),
    "UAH": (7_000, 200_000),
    "KZT": (70_000, 2_500_000),
    "JPY": (120_000, 4_000_000),
    "KRW": (1_500_000, 20_000_000),
    "CNY": (4_000, 120_000),
    "BRL": (1_500, 80_000),
    "MXN": (4_000, 150_000),
    "ILS": (4_000, 120_000),
    "TRY": (8_000, 250_000),
    "AED": (3_000, 120_000),
    "CHF": (2_000, 40_000),
    "SEK": (10_000, 250_000),
    "NOK": (10_000, 250_000),
    "DKK": (10_000, 250_000),
    "NZD": (2_000, 35_000),
    "HKD": (8_000, 300_000),
}

# Позиция
SENIORITY_MAP = {
    "intern": "intern", "trainee": "intern", "стажер": "intern", "стажёр": "intern",
    "junior": "junior", "jr ": "junior", "jr.": "junior", "entry level": "junior",
    "entry-level": "junior", "начального": "junior", "младший": "junior",
    "без опыта": "junior", "graduate": "junior",
    "middle": "middle", "mid ": "middle", "mid-level": "middle",
    "мидл": "middle", "среднего": "middle", "regular": "middle",
    "senior": "senior", "sr ": "senior", "sr.": "senior", "старший": "senior",
    "lead": "lead", "ведущий": "lead", "главный": "lead", "team lead": "lead",
    "тимлид": "lead", "техлид": "lead",
    "principal": "principal", "staff": "principal",
    "manager": "manager", "руководитель": "manager", "head of": "manager",
    "director": "director", "директор": "director", "vp ": "director",
}

_ROLE_FAMILIES = {
    "data_scientist": ["data scientist", "дата сайентист", "ученый по данным"],
    "ml_engineer": ["machine learning", "ml engineer", "deep learning", "ai engineer"],
    "data_engineer": ["data engineer", "etl", "дата инженер", "инженер данных", "big data"],
    "data_analyst": ["data analyst", "аналитик данных", "bi analyst", "bi developer", "бизнес-аналитик"],
    "backend_developer": [
        "backend", "бэкенд", "python developer", "java developer", "go developer",
        "разработчик python", "node developer", "php developer", "c# developer",
    ],
    "frontend_developer": ["frontend", "фронтенд", "react developer", "vue developer", "angular"],
    "fullstack_developer": ["full stack", "fullstack", "фулстек"],
    "mobile_developer": ["mobile", "ios developer", "android developer", "flutter", "react native"],
    "devops_engineer": ["devops", "sre", "site reliability", "platform engineer", "infrastructure"],
    "mlops_engineer": ["mlops", "ml platform", "ml infrastructure"],
    "qa_engineer": ["qa", "test", "quality assurance", "тестировщик", "sdet"],
    "security_engineer": ["security", "cybersecurity", "infosec", "безопасност", "пентест"],
    "data_architect": ["data architect", "архитектор данных", "solutions architect"],
    "engineering_manager": [
        "engineering manager", "tech lead", "team lead", "тимлид", "техлид",
        "head of", "руководитель", "director of engineering", "vp engineering", "cto",
    ],
    "product_manager": ["product manager", "продакт", "проджект", "scrum master", "product owner"],
    "designer": ["ux designer", "ui designer", "дизайнер", "product designer"],
    "systems_engineer": ["systems engineer", "системный администратор", "network engineer", "сетевой"],
    "nlp_engineer": ["nlp", "natural language", "llm engineer", "conversational ai", "genai"],
    "cv_engineer": ["computer vision", "image processing", "perception engineer"],
    "researcher": ["research scientist", "research engineer", "applied scientist"],
}

DEPARTMENT_RULES = {
    "Data & Analytics": [
        "data", "analytics", "business intelligence", "bi", "reporting", "insights"
    ],
    "Machine Learning / AI": [
        "machine learning", "artificial intelligence", "ai", "ml", "nlp", "llm"
    ],
    "Engineering": [
        "engineering", "software", "platform", "infrastructure", "backend",
        "frontend", "fullstack", "mobile", "qa", "security", "devops", "sre"
    ],
    "Product": [
        "product", "product management", "product manager"
    ],
    "Design": [
        "design", "ux", "ui", "product design"
    ],
    "Finance": [
        "finance", "accounting", "fp&a", "financial"
    ],
    "Operations": [
        "operations", "business operations", "strategy & operations"
    ],
    "Sales": [
        "sales", "account executive", "business development"
    ],
    "Marketing": [
        "marketing", "growth", "seo", "content", "brand"
    ],
    "Customer Success / Support": [
        "customer success", "support", "customer service"
    ],
    "HR / Recruiting": [
        "people", "talent", "recruiting", "human resources", "hr"
    ],
    "Legal / Compliance": [
        "legal", "compliance", "privacy", "regulatory"
    ],
}

# удаление эмодзи
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002702-\U000027B0"  # dingbats
    "\U0001FA00-\U0001FA6F"  # chess symbols
    "\U0001FA70-\U0001FAFF"  # symbols extended
    "\U00002600-\U000026FF"  # misc symbols
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0000200D"             # zero width joiner
    "\U00002640"             # female sign
    "\U00002642"             # male sign
    "\U000023CF-\U000023F3"  # misc technical
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "]+",
    flags=re.UNICODE,
)

# Допустимые значения source для валидации строк
VALID_SOURCES = {
    "hh.ru", "greenhouse.com", "lever.co", "ashbyhq.com",
    "adzuna.com", "usajobs.gov", "arbeitnow.com", "himalayas.app",
    "unknown",
}

# Проверяет, что значение пустое или служебное
def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip().lower() in {
        "", "nan", "none", "null"
    }

# Векторно проверяет пустые значения в серии pandas
def _is_missing_series(series: pd.Series) -> pd.Series:
    """Vectorized version of _is_missing for DataFrame columns."""
    return (
        series.isna()
        | series.astype(str).str.strip().str.lower().isin({"", "nan", "none", "null"})
    )

# Безопасно приводит значение к строке для составного ключа
def _safe_key_part(value) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()

# Очищает текст от HTML, лишних пробелов и эмодзи
def _clean_text(value, strip_html_tags: bool = False, remove_emoji: bool = False) -> Optional[str]:
    if _is_missing(value):
        return None

    text = str(value)

    for _ in range(3):
        new_text = html.unescape(text)
        if new_text == text:
            break
        text = new_text

    text = text.replace("\xa0", " ")

    if strip_html_tags:
        text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</\s*(p|div|li|ul|ol|h\d)\s*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)

    for _ in range(2):
        new_text = html.unescape(text)
        if new_text == text:
            break
        text = new_text

    text = text.replace("\xa0", " ")

    # [Пункт 5] Удаление эмодзи
    if remove_emoji:
        text = EMOJI_PATTERN.sub("", text)

    text = re.sub(r"\s+", " ", text).strip()
    return text or None

# Преобразует строковое представление массива в список
def _parse_pg_array(val) -> List[str]:
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if not isinstance(val, str):
        return [str(val).strip()] if str(val).strip() else []
    val = val.strip()
    if not val or val.lower() in {"nan", "none", "null"}:
        return []
    if val.startswith("[") and val.endswith("]"):
        try:
            arr = json.loads(val)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
        try:
            arr = ast.literal_eval(val)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    if val.startswith("{") and val.endswith("}"):
        inner = val[1:-1].strip()
        if not inner:
            return []
        return [s.strip().strip('"').strip("'") for s in inner.split(",") if s.strip()]
    if val.startswith("(") and val.endswith(")"):
        inner = val[1:-1].strip()
        return [s.strip().strip('"').strip("'") for s in inner.split(",") if s.strip()]
    return [s.strip().strip('"').strip("'") for s in val.split(",") if s.strip()]

# Преобразует список в JSON-строку
def _to_pg_array(items: List[str]) -> str:
    clean = [str(x).strip() for x in items if str(x).strip()]
    return json.dumps(clean, ensure_ascii=False)

# Преобразует строку с числом в float
def _parse_number(s: str) -> Optional[float]:
    if _is_missing(s):
        return None

    s = str(s).strip().lower()
    s = s.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    s = s.replace("_", "")
    s = re.sub(r"[^\d.,km]", "", s)

    if not s:
        return None

    multiplier = 1.0
    if s.endswith("k"):
        multiplier = 1000.0
        s = s[:-1]
    elif s.endswith("m"):
        multiplier = 1_000_000.0
        s = s[:-1]
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        if "," in s:
            parts = s.split(",")
            if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
                s = "".join(parts)
            else:
                s = s.replace(",", ".")
    try:
        val = float(s)
    except ValueError:
        return None

    if multiplier == 1000.0 and val >= 1000:
        return val

    return val * multiplier

# Извлечение навыков
SKILL_ALIASES_REGEX = {
    "node.js": [
        re.compile(r"(?<!\w)node(?:\.?\s*js)(?!\w)", re.I),
    ],
    "vue": [
        re.compile(r"(?<!\w)vue(?:\.?\s*js)?(?!\w)", re.I),
    ],
    "react": [
        re.compile(r"(?<!\w)react(?:\.?\s*js)?(?!\w)", re.I),
    ],
    ".net": [
        re.compile(r"(?<!\w)\.net(?!\w)", re.I),
    ],
    "power bi": [
        re.compile(r"(?<!\w)power\s*bi(?!\w)", re.I),
    ],
    "github actions": [
        re.compile(r"(?<!\w)github\s*actions?(?!\w)", re.I),
    ],
    "gitlab ci": [
        re.compile(r"(?<!\w)gitlab[\s-]*ci(?!\w)", re.I),
    ],
    "ci/cd": [
        re.compile(r"(?<!\w)ci\s*/?\s*cd(?!\w)", re.I),
    ],
}

# Строит регулярное выражение для поиска навыка
def _build_skill_regex(skill: str) -> re.Pattern:
    escaped = re.escape(skill).replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![A-Za-z0-9_+#.-]){escaped}(?![A-Za-z0-9_+#.-])", re.I)

SKILL_REGEXES = {
    skill: _build_skill_regex(skill)
    for skill in EXTRACTABLE_SKILLS
    if skill != "r"  # R обрабатываем отдельно
}

# Извлекает навыки из текста вакансии
def extract_skills_from_text(text: str) -> List[str]:
    if not text:
        return []

    t = " " + re.sub(r"\s+", " ", str(text).lower()) + " "
    found = set()

    for skill, rx in SKILL_REGEXES.items():
        if rx.search(t):
            found.add(skill)

    for skill, patterns in SKILL_ALIASES_REGEX.items():
        if any(p.search(t) for p in patterns):
            found.add(skill)

    if any(re.search(pattern, t, flags=re.I) for pattern in R_LANGUAGE_CONTEXT_PATTERNS):
        found.add("r")

    implied = set()
    for skill in found:
        for imp in SKILL_IMPLIES.get(skill, []):
            implied.add(imp)
    found |= implied

    return sorted(found)

# Нормализует навыки к каноническим именам
def normalize_skills(raw_skills: List[str]) -> List[str]:
    normalized = set()
    for skill in raw_skills:
        key = skill.lower().strip()
        canonical = SKILL_SYNONYMS.get(key, skill)
        normalized.add(canonical)
    return sorted(normalized)

# Нормализует департамент по полю и тексту вакансии
def normalize_department(department: str, title: str = "", description: str = "") -> Optional[str]:
    dep = str(department or "").strip()
    if dep:
        haystack = dep.lower()
    else:
        haystack = f"{title or ''} {(description or '')[:300]}".lower()

    for canonical, keywords in DEPARTMENT_RULES.items():
        if any(k in haystack for k in keywords):
            return canonical

    return dep if dep else None

SALARY_NUMBER_RE = r"\d[\d\s,._]*(?:\.\d+)?(?:\s*[kKmM])?"
CURRENCY_TOKEN_RE = (
    r"(?:c\$|a\$|s\$|hk\$|us\$|£|€|\$|₽|₹|zł|₴|₸|¥|₩|₪|₺|"
    r"USD|EUR|GBP|CAD|AUD|SGD|HKD|INR|RUB|RUR|PLN|UAH|KZT|"
    r"JPY|KRW|CNY|RMB|BRL|MXN|ILS|NIS|TRY|AED|CHF|SEK|NOK|DKK|NZD|CZK|RON|HUF|BGN|ZAR)"
)

# Определяет период зарплаты по тексту
def _extract_period_hint(text: str) -> Optional[str]:
    if not text:
        return None
    txt = str(text).lower()

    period_map = {
        "hour": [
            r"/\s*hour\b", r"/\s*hr\b", r"\bper\s+hour\b", r"\bhourly\b",
            r"\ban?\s+hour\b", r"\bhr\b", r"\bв\s+час\b",
        ],
        "day": [r"/\s*day\b", r"\bper\s+day\b", r"\bdaily\b", r"\bв\s+день\b"],
        "week": [r"/\s*week\b", r"\bper\s+week\b", r"\bweekly\b", r"\bв\s+неделю\b"],
        "month": [r"/\s*month\b", r"/\s*mo\b", r"\bper\s+month\b", r"\bmonthly\b", r"\bв\s+месяц\b"],
        "year": [
            r"/\s*year\b", r"\bper\s+year\b", r"\byearly\b", r"\bannually\b",
            r"\bannual\b", r"\bper\s+annum\b", r"\bp\.?a\.?\b", r"\bв\s+год\b",
        ],
    }
    for period, patterns in period_map.items():
        if any(re.search(pattern, txt, flags=re.I) for pattern in patterns):
            return period
    return None

# Преобразует валютный токен в код валюты
def _currency_from_token(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    token = token.strip()
    low = token.lower()

    if low in CURRENCY_SYMBOLS:
        return CURRENCY_SYMBOLS[low]
    if token in CURRENCY_SYMBOLS:
        return CURRENCY_SYMBOLS[token]
    if low in CURRENCY_WORDS:
        return CURRENCY_WORDS[low]

    token_up = token.upper()
    return _normalize_currency_code(token_up)

# Извлекает зарплату, валюту и период из текста
def extract_salary_from_text(text: str) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    if not text:
        return None, None, None, None

    txt = str(text)
    period = _extract_period_hint(txt)

    patterns = [
        # $100k - $150k
        rf"(?P<cur1>{CURRENCY_TOKEN_RE})\s*(?P<n1>{SALARY_NUMBER_RE})\s*(?:-|–|—|to)\s*(?P<cur2>{CURRENCY_TOKEN_RE})?\s*(?P<n2>{SALARY_NUMBER_RE})",
        # 100k - 150k USD
        rf"(?P<n1>{SALARY_NUMBER_RE})\s*(?:-|–|—|to)\s*(?P<n2>{SALARY_NUMBER_RE})\s*(?P<cur3>{CURRENCY_TOKEN_RE})",
        # from 100k to 150k USD
        rf"(?:from|от)\s*(?P<n1>{SALARY_NUMBER_RE})\s*(?:to|до)\s*(?P<n2>{SALARY_NUMBER_RE})\s*(?P<cur4>{CURRENCY_TOKEN_RE})?",
        # single: $120k
        rf"(?P<cur5>{CURRENCY_TOKEN_RE})\s*(?P<n3>{SALARY_NUMBER_RE})",
        # single: 120k USD
        rf"(?P<n4>{SALARY_NUMBER_RE})\s*(?P<cur6>{CURRENCY_TOKEN_RE})",
    ]

    for pat in patterns:
        m = re.search(pat, txt, flags=re.I)
        if not m:
            continue

        gd = m.groupdict()

        if gd.get("n1") and gd.get("n2"):
            sal_from = _parse_number(gd["n1"])
            sal_to = _parse_number(gd["n2"])
            currency = (
                _currency_from_token(gd.get("cur1"))
                or _currency_from_token(gd.get("cur2"))
                or _currency_from_token(gd.get("cur3"))
                or _currency_from_token(gd.get("cur4"))
                or detect_currency_from_text(m.group(0))
                or detect_currency_from_text(txt)
            )
            if sal_from is not None and sal_to is not None:
                return sal_from, sal_to, currency, period

        if gd.get("n3"):
            sal = _parse_number(gd["n3"])
            currency = _currency_from_token(gd.get("cur5")) or detect_currency_from_text(m.group(0)) or detect_currency_from_text(txt)
            if sal is not None:
                return sal, sal, currency, period

        if gd.get("n4"):
            sal = _parse_number(gd["n4"])
            currency = _currency_from_token(gd.get("cur6")) or detect_currency_from_text(m.group(0)) or detect_currency_from_text(txt)
            if sal is not None:
                return sal, sal, currency, period

    return None, None, None, period

# Определяет валюту по символам и словам в тексте
def detect_currency_from_text(text: str) -> Optional[str]:
    if not text:
        return None

    txt = str(text)
    txt_lower = txt.lower()
    for sym in sorted(CURRENCY_SYMBOLS.keys(), key=len, reverse=True):
        if sym.lower() in txt_lower:
            code = CURRENCY_SYMBOLS[sym]
            if sym == "¥":
                if re.search(r"\b(cny|rmb|yuan)\b", txt_lower):
                    return "CNY"
                return "JPY"
            return code

    for word, code in CURRENCY_WORDS.items():
        if re.search(rf"(?<!\w){re.escape(word)}(?!\w)", txt_lower):
            return code

    return None

# Грубо оценивает реальный период зарплаты по сумме
def _guess_real_period(amount: float, currency: str) -> str:
    """Эвристика: определяем реальный период по величине суммы."""
    cur = (currency or "USD").upper()
    lo, hi = MONTHLY_BOUNDS.get(cur, (300, 120_000))
    if amount < lo:
        return "hour"
    elif amount > hi:
        return "year"
    else:
        return "month"


PERIOD_MULTIPLIERS = {
    "hour": 160.0,
    "day": 21.0,
    "week": 4.33,
    "month": 1.0,
    "year": 1.0 / 12.0,
}

# Переводит сумму в месячный эквивалент
def _to_monthly(amount: float, period: str) -> float:
    return amount * PERIOD_MULTIPLIERS.get(period, 1.0)

# Считает отклонение суммы от допустимого месячного диапазона
def _monthly_fit_score(monthly_amount: float, currency: str) -> float:
    cur = _normalize_currency_code(currency) or "USD"
    lo, hi = MONTHLY_BOUNDS.get(cur, (800, 50_000))

    if lo <= monthly_amount <= hi:
        return 0.0

    if monthly_amount < lo:
        return (lo - monthly_amount) / max(lo, 1)

    return (monthly_amount - hi) / max(hi, 1)

# Выбирает наиболее вероятный период зарплаты
def _choose_best_period(amount: float, currency: str, period_hint: Optional[str]) -> str:
    candidates = ["hour", "day", "week", "month", "year"]

    best_period = "month"
    best_score = float("inf")

    ordered = [period_hint] + [p for p in candidates if p != period_hint] if period_hint in candidates else candidates

    for p in ordered:
        monthly_val = _to_monthly(amount, p)
        score = _monthly_fit_score(monthly_val, currency)

        if p == period_hint:
            score -= 0.05

        if score < best_score:
            best_score = score
            best_period = p

    return best_period

# Приводит зарплату к месячному формату
def normalize_salary_to_monthly(sal_from, sal_to, currency, period) -> Tuple[Optional[float], Optional[float]]:
    if pd.isna(sal_from) and pd.isna(sal_to):
        return sal_from, sal_to

    cur = _normalize_currency_code(currency) or "USD"
    period_hint = str(period).strip().lower() if not _is_missing(period) else None
    if period_hint in {"annually", "annual", "yearly"}:
        period_hint = "year"
    if period_hint in {"monthly"}:
        period_hint = "month"
    if period_hint in {"hourly"}:
        period_hint = "hour"
    if period_hint in {"daily"}:
        period_hint = "day"
    if period_hint not in PERIOD_MULTIPLIERS:
        period_hint = None

    numeric_values = [float(v) for v in (sal_from, sal_to) if pd.notna(v)]
    if not numeric_values:
        return sal_from, sal_to

    test_val = max(numeric_values)
    real_period = _choose_best_period(float(test_val), cur, period_hint)
    mult = PERIOD_MULTIPLIERS[real_period]

    new_from = round(float(sal_from) * mult) if pd.notna(sal_from) else None
    new_to = round(float(sal_to) * mult) if pd.notna(sal_to) else None

    return new_from, new_to

# Проверяет и очищает границы месячной зарплаты
def _sanitize_monthly_salary_bounds(
    sal_from: Optional[float],
    sal_to: Optional[float],
    currency: Optional[str],
) -> Tuple[Optional[int], Optional[int]]:
    cur = _normalize_currency_code(currency) or "USD"
    lo, hi = MONTHLY_BOUNDS.get(cur, (800, 50_000))

    values: list[Optional[int]] = []
    for value in (sal_from, sal_to):
        if value is None or pd.isna(value):
            values.append(None)
        else:
            try:
                values.append(round(float(value)))
            except Exception:
                values.append(None)

    clean_from, clean_to = values

    if clean_from is not None and clean_from <= 0:
        clean_from = None
    if clean_to is not None and clean_to <= 0:
        clean_to = None

    if clean_from is not None and clean_to is not None and clean_from > clean_to:
        clean_from, clean_to = clean_to, clean_from

    present = [v for v in (clean_from, clean_to) if v is not None]
    if not present:
        return None, None

    absurd_limit = int(max(hi * 20, 1_000_000))
    if any(v > absurd_limit for v in present):
        return None, None

    if clean_from is not None and clean_to is not None:
        ratio = clean_to / max(clean_from, 1)
        if ratio > 20:
            return None, None

    return clean_from, clean_to


# Определяет seniority по заголовку и описанию
def detect_seniority(title: str = "", description: str = "", experience: str = "") -> str:
    for text in [experience, title, description[:300]]:
        if not text:
            continue
        t = text.lower()
        for keyword, level in SENIORITY_MAP.items():
            if keyword in t:
                return level
    return "unknown"

# Извлекает годы опыта из текста
def _extract_years_from_text(text: str) -> Tuple[Optional[float], Optional[float]]:
    if not text:
        return None, None

    txt = str(text).lower().replace(",", ".")

    patterns = [
        r"(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)\s*(?:years?|yrs?|лет|года|год)",
        r"(?:from|от)\s*(\d+(?:\.\d+)?)\s*(?:years?|yrs?|лет)",
        r"(\d+(?:\.\d+)?)\+\s*(?:years?|yrs?|лет)",
        r"(\d+(?:\.\d+)?)\s*(?:years?|yrs?|лет|года|год)",
    ]

    for pat in patterns:
        m = re.search(pat, txt)
        if m:
            g = m.groups()
            return (float(g[0]), float(g[1])) if len(g) == 2 else (float(g[0]), float(g[0]))

    return None, None

# Нормализует тип занятости
def normalize_employment_type(val: str, description: str = "") -> str:
    if not val and not description:
        return "unknown"
    text = f"{val or ''} {(description or '')[:500]}".lower()
    patterns = {
        "full_time": [
            "full_time", "full-time", "full time", "fulltime",
            "employee: full time", "permanent", "perm",
            "полная занятость", "полный день",
        ],
        "part_time": ["part_time", "part-time", "part time", "частичная занятость", "неполный"],
        "contract": ["contract", "contractor", "fixed-term", "temporary contract", "b2b", "контракт"],
        "freelance": ["freelance", "freelancer", "self-employed", "фриланс"],
        "internship": ["internship", "intern", "стажировка", "стажер", "стажёр"],
        "temporary": ["temporary", "temp", "seasonal", "временная"],
    }
    for canonical, keywords in patterns.items():
        if any(k in text for k in keywords):
            return canonical
    return "unknown"

# Определяет формат работы: remote, hybrid или office
def detect_remote_type(title: str = "", description: str = "", remote_flag=None, location: str = "") -> str:
    text = f"{title or ''} {(description or '')[:800]} {location or ''}".lower()

    if any(w in text for w in [
        "hybrid", "гибрид", "гибридн",
        "2-3 days in office", "2 days in office", "3 days in office",
        "split between home and office"
    ]):
        return "hybrid"

    if any(w in text for w in [
        "onsite", "on-site", "in-office", "in office", "office-based",
        "must be on site", "must be onsite", "must be in office",
        "работа в офисе", "в офисе", "fullDay"
    ]):
        return "office"

    if remote_flag in (True, "True", "true", 1):
        return "remote"

    if remote_flag in (False, "False", "false", 0):
        return "office"

    if any(w in text for w in [
        "remote", "remote-first", "fully remote", "100% remote",
        "distributed team", "work from home", "wfh",
        "удалённ", "удален", "дистанц"
    ]):
        return "remote"

    # если есть конкретная география и нет признаков remote/hybrid — считаем onsite
    if not _is_missing(location):
        return "office"

    return "unknown"


# Пытается определить страну по тексту
def _extract_country_from_haystack(text: str) -> Optional[str]:
    if _is_missing(text):
        return None

    t = str(text).strip().lower()

    # прямые страны
    for hint, canonical in _LOCATION_COUNTRY_HINTS.items():
        if hint in t:
            return canonical

    # штаты США
    for state in US_STATES:
        if re.search(rf"(?<!\w){re.escape(state)}(?!\w)", t):
            return "UNITED STATES"

    for abbr in US_STATE_ABBR:
        if re.search(rf"(?<!\w){abbr.lower()}(?!\w)", t):
            return "UNITED STATES"

    # провинции Канады
    for prov in CA_PROVINCES:
        if prov in t:
            return "CANADA"

    for abbr in CA_PROVINCE_ABBR:
        if re.search(rf"(?<!\w){abbr.lower()}(?!\w)", t):
            return "CANADA"

    return None

# Убирает служебные префиксы из location
def _strip_location_prefix(text: str) -> str:
    text = re.sub(r"(?i)^(remote|hybrid|onsite|on-site)\s*[-–—,:/|]\s*", "", text).strip()
    text = re.sub(r"(?i)\b(remote|hybrid|onsite|on-site)\b\s*$", "", text).strip(" -–—,;/|")
    return text.strip()

# Разбирает location на город и страну
def _parse_location_parts(location: str) -> Tuple[Optional[str], Optional[str]]:
    if _is_missing(location):
        return None, None

    text = str(location).strip()
    text = _strip_location_prefix(text)

    if not text:
        return None, None

    country = _extract_country_from_haystack(text)
    first_chunk = re.split(r"[;|/]", text, maxsplit=1)[0].strip()
    first_chunk = re.split(r"\s+\bor\b\s+", first_chunk, maxsplit=1, flags=re.I)[0].strip()

    parts = [p.strip(" -–—") for p in first_chunk.split(",") if p.strip()]
    city = None

    for part in parts:
        p_low = part.lower()

        if p_low in LOCATION_NOISE_WORDS:
            continue
        if p_low in LOCATION_REGION_NOISE:
            continue
        if p_low in KNOWN_COUNTRIES_LOWER:
            continue
        if p_low in US_STATES or part.upper() in US_STATE_ABBR:
            continue
        if p_low in CA_PROVINCES or part.upper() in CA_PROVINCE_ABBR:
            continue

        city = part.upper()
        break

    return city, country

# Определяет страну по полям вакансии и fallback-правилам
def infer_country(country, location, source: str = "", description: str = "", company_name: str = "") -> Optional[str]:
    # 1. Явная страна из source
    if not _is_missing(country):
        key = str(country).strip().upper()
        return _COUNTRY_MAP.get(key, key)

    # 2. Пробуем только location
    if not _is_missing(location):
        from_location = _extract_country_from_haystack(location)
        if from_location:
            return from_location
        return None

    # 3. Только если location вообще нет — можно смотреть description
    haystack = str(description or "")[:500]
    from_desc = _extract_country_from_haystack(haystack)
    if from_desc:
        return from_desc

    # 4. И только в самом конце fallback по компании
    if company_name:
        comp_key = str(company_name).strip().lower()
        if comp_key in COMPANY_COUNTRY_MAP:
            return COMPANY_COUNTRY_MAP[comp_key]

    return None

# Нормализует название города
def normalize_city_name(city: Optional[str]) -> Optional[str]:
    """Нормализует город: русский → английский, всё в UPPER CASE."""
    if not city:
        return None
    c = str(city).strip()
    if not c:
        return None
    key = c.lower()
    if key in {"nan", "none", "null", "n/a", "na", ""}:
        return None
    en_name = RU_CITY_TO_EN.get(key)
    if en_name:
        return en_name
    return c.upper()

# Определяет язык публикации вакансии
def detect_posting_language(text: str) -> str:
    """Определяет язык текста вакансии. Дефолт — English."""
    if not text:
        return "English"
    cyrillic = len(re.findall(r"[а-яёА-ЯЁ]", text[:2000]))
    latin = len(re.findall(r"[a-zA-Z]", text[:2000]))
    total = cyrillic + latin
    if total == 0:
        return "English"
    if cyrillic / total > 0.3:
        return "Russian"
    return "English"

# Нормализует title для поиска и агрегаций
def normalize_title(title: str, company_name: str = "") -> Optional[str]:
    """Нормализует title для downstream агрегаций и эмбеддингов."""
    if not title:
        return None
    t = title.strip()
    if company_name:
        cn = company_name.strip()
        if f" | {cn}" in t:
            t = t.replace(f" | {cn}", "").strip()
        elif f"{cn} | " in t:
            t = t.replace(f"{cn} | ", "").strip()
        if t.lower().endswith(f" at {cn.lower()}"):
            t = t[: -(len(cn) + 4)].strip()

    t = t.lower().strip()
    t = re.sub(r"[^a-zа-яё0-9\s+#.]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t or None

# Строит ключ для дедупликации вакансий
def _dedupe_key(row):
    source = _safe_key_part(row.get("source"))
    source_job_id = _safe_key_part(row.get("source_job_id"))
    url = _safe_key_part(row.get("url")).lower()
    job_id = _safe_key_part(row.get("job_id"))
    title = _safe_key_part(row.get("title")).lower()
    company = _safe_key_part(row.get("company_name")).lower()

    if source and source_job_id:
        return f"srcid::{source}::{source_job_id}"
    if source and url:
        return f"url::{source}::{url}"
    if job_id:
        return f"job::{job_id}"
    return f"title::{source}::{company}::{title}"



MAX_REASONABLE_YEARS = 25

_POSITIVE_EXPERIENCE_PATTERNS = [
    # 3-5 years of experience
    r"(\d{1,2})\s*[-–—]\s*(\d{1,2})\s+years?\s+of\s+experience",
    # from 3 years / at least 3 years
    r"(?:from|at\s+least|minimum\s+of|min\.?|over|more\s+than)\s*(\d{1,2})\+?\s+years?\s+(?:of\s+)?experience",
    # 3+ years of experience
    r"(\d{1,2})\+?\s+years?\s+of\s+experience",
    # 3 years experience
    r"(\d{1,2})\+?\s+years?\s+experience",
    # experience: 3 years
    r"experience.{0,25}?(\d{1,2})\+?\s+years?",
]

_NEGATIVE_EXPERIENCE_CONTEXT = [
    r"\bour company\b",
    r"\bcompany\b",
    r"\borganization\b",
    r"\bbusiness\b",
    r"\bheritage\b",
    r"\bhistory\b",
    r"\btradition\b",
    r"\bfounded\b",
    r"\bsince\s+\d{4}\b",
    r"\bfor\s+over\s+\d+\s+years\b",
    r"\bfor\s+more\s+than\s+\d+\s+years\b",
    r"\bserving\b",
    r"\bleading\b",
    r"\bglobal leader\b",
    r"\byears of innovation\b",
]

_STRONG_POSITIVE_HINTS = [
    r"\bexperience\b",
    r"\brequirements?\b",
    r"\brequired\b",
    r"\bmust\s+have\b",
    r"\bnice\s+to\s+have\b",
    r"\bqualification[s]?\b",
    r"\bminimum\b",
    r"\bat\s+least\b",
    r"\bcommercial\b",
    r"\bhands[- ]on\b",
]

# Безопасно извлекает опыт только из релевантного контекста
def _extract_years_from_text_safe(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None

    txt = str(text).lower()

    for pat in _POSITIVE_EXPERIENCE_PATTERNS:
        m = re.search(pat, txt, flags=re.I | re.S)
        if not m:
            continue
        nums = [int(g) for g in m.groups() if g is not None]
        if not nums:
            continue
        if len(nums) == 1:
            y1 = y2 = nums[0]
        else:
            y1, y2 = nums[0], nums[1]
        if y1 <= 0 or y2 <= 0:
            continue
        if y1 > MAX_REASONABLE_YEARS or y2 > MAX_REASONABLE_YEARS:
            continue

        start, end = m.span()
        window = txt[max(0, start - 120): min(len(txt), end + 120)]

        negative = any(re.search(p, window, flags=re.I) for p in _NEGATIVE_EXPERIENCE_CONTEXT)
        positive = any(re.search(p, window, flags=re.I) for p in _STRONG_POSITIVE_HINTS)
        if negative and not positive:
            continue

        return y1, y2

    return None, None

# Извлекает опыт из приоритетных текстовых полей строки
def _extract_years_from_row(row) -> pd.Series:
    texts = [
        str(row.get("requirements") or ""),
        str(row.get("nice_to_have") or ""),
        str(row.get("responsibilities") or ""),
        str(row.get("title") or ""),
        str(row.get("description") or ""),
    ]
    for text in texts:
        y1, y2 = _extract_years_from_text_safe(text)
        if y1 is not None:
            return pd.Series({
                "years_experience_min": y1,
                "years_experience_max": y2,
            })

    return pd.Series({
        "years_experience_min": None,
        "years_experience_max": None,
    })

# Собирает текстовое представление опыта
def _experience_to_text(row) -> str:

    y1 = row.get("years_experience_min")
    y2 = row.get("years_experience_max")

    if pd.notna(y1) and pd.notna(y2):
        if int(y1) == int(y2):
            return f"{int(y1)} years"
        return f"{int(y1)}-{int(y2)} years"

    return "нет данных"

# Нормализует текст специальности для сопоставления
def _normalize_specialty_text(text: str) -> str:
    text = str(text or "").lower().strip()
    text = re.sub(r"[^a-zа-яё0-9+#./\-\s]", " ", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


_SPECIALTY_QUERY_DISPLAY: dict[str, str] = {}
for q in ALL_QUERIES_COMBINED:
    key = _normalize_specialty_text(q)
    if key and key not in _SPECIALTY_QUERY_DISPLAY:
        _SPECIALTY_QUERY_DISPLAY[key] = q

_SPECIALTY_QUERY_KEYS = sorted(_SPECIALTY_QUERY_DISPLAY.keys(), key=len, reverse=True)

FALLBACK_SPECIALTY_QUERIES = [
    "Data Scientist", "Senior Data Scientist", "Junior Data Scientist", "Lead Data Scientist",
    "Machine Learning Engineer", "ML Engineer", "AI Engineer", "LLM Engineer", "NLP Engineer",
    "Data Engineer", "Analytics Engineer", "Data Analyst", "Business Analyst", "Product Analyst",
    "Backend Developer", "Backend Engineer", "Software Engineer",
    "Frontend Developer", "Frontend Engineer",
    "Full Stack Developer", "Fullstack Developer", "Full Stack Engineer", "Fullstack Engineer",
    "Android Developer", "Android Engineer", "iOS Developer", "iOS Engineer",
    "Mobile Developer", "Mobile Engineer",
    "DevOps Engineer", "Site Reliability Engineer", "SRE", "Platform Engineer",
    "QA Engineer", "Test Engineer", "SDET",
    "Security Engineer", "Product Manager", "Project Manager", "Engineering Manager",
    "Solutions Architect", "Solutions Engineer", "Sales Engineer", "Designer",
]

if not _SPECIALTY_QUERY_KEYS:
    for q in FALLBACK_SPECIALTY_QUERIES:
        key = _normalize_specialty_text(q)
        if key and key not in _SPECIALTY_QUERY_DISPLAY:
            _SPECIALTY_QUERY_DISPLAY[key] = q
    _SPECIALTY_QUERY_KEYS = sorted(_SPECIALTY_QUERY_DISPLAY.keys(), key=len, reverse=True)

# Определяет специальность по title
def extract_specialty_from_title(title: str) -> Optional[str]:
    if _is_missing(title):
        return None

    t = _normalize_specialty_text(title)
    for query_key in _SPECIALTY_QUERY_KEYS:
        escaped_query = re.escape(query_key)
        escaped_query = escaped_query.replace(r"\ ", r"\s+")
        escaped_query = escaped_query.replace(r"\-", r"(?:-|\s)")

        regex = rf"(?<![a-zа-яё0-9]){escaped_query}(?![a-zа-яё0-9])"
        if re.search(regex, t, flags=re.I):
            return _SPECIALTY_QUERY_DISPLAY[query_key]

    specific_patterns = [
        (r"\bfinancial analyst\b", "Financial Analyst"),
        (r"\bbusiness analyst\b", "Business Analyst"),
        (r"\bproduct analyst\b", "Product Analyst"),
        (r"\bdata analyst\b", "Data Analyst"),
        (r"\bfirmware engineer\b", "Firmware Engineer"),
        (r"\bembedded engineer\b", "Embedded Engineer"),
        (r"\bflight sciences engineer\b", "Flight Sciences Engineer"),
        (r"\bflight software engineer\b", "Flight Software Engineer"),
        (r"\bsoftware engineer\b", "Software Engineer"),
        (r"\bbackend engineer\b", "Backend Engineer"),
        (r"\bfrontend engineer\b", "Frontend Engineer"),
        (r"\bfull stack engineer\b", "Full Stack Engineer"),
        (r"\bfullstack engineer\b", "Fullstack Engineer"),
    ]
    for pat, label in specific_patterns:
        if re.search(pat, t, flags=re.I):
            return label

    if re.search(r"\banalyst\b", t, flags=re.I):
        return "Analyst"
    if re.search(r"\bengineer\b", t, flags=re.I):
        return "Engineer"
    if re.search(r"\bdeveloper\b", t, flags=re.I):
        return "Developer"
    if re.search(r"\bmanager\b", t, flags=re.I):
        return "Manager"
    if re.search(r"\barchitect\b", t, flags=re.I):
        return "Architect"
    if re.search(r"\bdesigner\b", t, flags=re.I):
        return "Designer"
    if re.search(r"\bspecialist\b", t, flags=re.I):
        return "Specialist"

    return None

# Определяет семейство роли по заголовку вакансии
def _detect_role_family(title: str) -> str:
    if _is_missing(title):
        return "other"

    t = str(title).lower()
    for family, keywords in _ROLE_FAMILIES.items():
        if any(kw in t for kw in keywords):
            return family
    return "other"

# Очищает, нормализует и обогащает датафрейм вакансий
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)
    logger.info("Cleaning: %s rows", initial)
    df = df.copy()

    # Гарантируем наличие базовых колонок, чтобы downstream не падал.
    default_none_cols = [
        "job_id", "source_job_id", "source", "url", "title", "description",
        "company_name", "department", "location", "country", "city", "remote",
        "employment_type", "salary_from", "salary_to", "currency",
        "spoken_languages", "visa_sponsorship", "relocation", "published_at",
        "parsed_at",
    ]
    default_array_cols = [
        "key_skills", "skills_extracted", "tech_stack_tags", "tools", "methodologies",
    ]

    for col in default_none_cols:
        if col not in df.columns:
            df[col] = None

    for col in default_array_cols:
        if col not in df.columns:
            df[col] = "[]"

    # Удаление мусорных колонок
    cols_to_drop = [c for c in df.columns if c.startswith("Unnamed")]
    for c in [
        "experience_level", "industry", "company_size", "education", "certifications",
        "benefits", "equity_bonus", "security_clearance", "search_query",
    ]:
        if c in df.columns:
            cols_to_drop.append(c)

    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # Валидация source
    if "source" in df.columns:
        valid_mask = df["source"].apply(
            lambda s: not _is_missing(s) and str(s).strip().lower() in VALID_SOURCES
        )
        bad_rows = int((~valid_mask).sum())
        if bad_rows > 0:
            logger.warning("Dropping %s rows with invalid 'source' (column shift detected)", bad_rows)
            df = df[valid_mask].copy()


    # Дедупликация
    df["_dedupe_key"] = df.apply(_dedupe_key, axis=1)
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="first")


    # Очистка текстовых полей
    if "title" in df.columns:
        df["title"] = df["title"].apply(_clean_text)
        df = df.dropna(subset=["title"])
        df = df[df["title"].astype(str).str.strip() != ""]

    if "description" in df.columns:
        df["description"] = df["description"].apply(
            lambda x: _clean_text(x, strip_html_tags=True, remove_emoji=True)
        )

    for col in ["company_name", "location", "department", "employment_type", "country", "city", "url"]:
        if col in df.columns:
            df[col] = df[col].apply(_clean_text)

    text_aux_cols = ["requirements", "responsibilities", "nice_to_have"]
    for col in text_aux_cols:
        if col in df.columns:
            df[col] = df[col].apply(_clean_text)


    df["title_normalized"] = df.apply(
        lambda r: normalize_title(str(r.get("title") or ""), str(r.get("company_name") or "")),
        axis=1,
    )
    df = df.dropna(subset=["title_normalized"])
    df = df[df["title_normalized"].astype(str).str.strip() != ""]

    df["specialty"] = df["title"].apply(extract_specialty_from_title)
    df["specialty_category"] = df["specialty"].apply(lambda x: CATEGORY_MAP.get(x) if not _is_missing(x) else None)
    df["role_family"] = df["title_normalized"].apply(_detect_role_family)

    if "source" in df.columns and "url" in df.columns:
        mask_bad_himalayas = (
            df["source"].astype(str).str.lower().eq("himalayas.app")
            & df["url"].fillna("").astype(str).str.strip().isin(
                ["", "https://himalayas.app", "http://himalayas.app"]
            )
        )

        if mask_bad_himalayas.any():
            df.loc[mask_bad_himalayas, "url"] = df.loc[mask_bad_himalayas].apply(
                lambda r: (
                    f"https://himalayas.app/jobs/{str(r['source_job_id']).strip()}"
                    if not _is_missing(r.get("source_job_id"))
                    else None
                ),
                axis=1,
            )

    years = df.apply(_extract_years_from_row, axis=1)
    df["years_experience_min"] = years["years_experience_min"]
    df["years_experience_max"] = years["years_experience_max"]


    mask_bad_exp = (
        (df["years_experience_min"].notna() & (df["years_experience_min"] > MAX_REASONABLE_YEARS))
        | (df["years_experience_max"].notna() & (df["years_experience_max"] > MAX_REASONABLE_YEARS))
        | (df["years_experience_min"].notna() & (df["years_experience_min"] <= 0))
        | (df["years_experience_max"].notna() & (df["years_experience_max"] <= 0))
    )

    if mask_bad_exp.any():
        logger.info("Resetting suspicious experience values for %s rows", int(mask_bad_exp.sum()))
        df.loc[mask_bad_exp, ["years_experience_min", "years_experience_max"]] = [None, None]


    logger.info("Extracting skills from selected source columns...")

    skill_text_cols = [
        c for c in ["description", "title", "requirements", "responsibilities", "nice_to_have"]
        if c in df.columns
    ]
    skill_array_cols = [c for c in ["key_skills", "skills_extracted"] if c in df.columns]

    if skill_text_cols:
        text_frame = df[skill_text_cols].fillna("").astype(str)
        df["_skill_text"] = (
            text_frame.agg(" ".join, axis=1)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    else:
        df["_skill_text"] = ""

    df["_text_skills"] = df["_skill_text"].apply(extract_skills_from_text)

    for col in skill_array_cols:
        df[f"_{col}_parsed"] = df[col].apply(_parse_pg_array)

    parsed_array_cols = [f"_{col}_parsed" for col in skill_array_cols]

    # Объединяет навыки из разных источников строки
    def _merge_skill_sources(row):
        merged = []
        merged.extend(row.get("_text_skills", []))
        for col in parsed_array_cols:
            parsed = row.get(col, [])
            if isinstance(parsed, list):
                merged.extend(parsed)
        return list(dict.fromkeys(x for x in merged if str(x).strip()))

    df["_merged_skills"] = df.apply(_merge_skill_sources, axis=1)

    df["skills_normalized"] = df["_merged_skills"].apply(
        lambda s: _to_pg_array(normalize_skills(s))
    )

    df["_skills_count"] = df["skills_normalized"].apply(
        lambda x: len(_parse_pg_array(x))
    )

    no_skills = int((df["_skills_count"] == 0).sum())
    if no_skills > 0:
        logger.info(
            "Keeping %s rows without extracted skills: they will still go to embeddings",
            no_skills
        )

    drop_cols = (
        ["_skill_text", "_text_skills", "_merged_skills", "_skills_count"]
        + parsed_array_cols
        + [c for c in [
            "requirements", "responsibilities", "nice_to_have",
            "key_skills", "skills_extracted", "tech_stack_tags",
            "tools", "methodologies",
        ] if c in df.columns]
    )
    df.drop(columns=drop_cols, inplace=True, errors="ignore")

    df["department"] = df.apply(
        lambda r: normalize_department(
            r.get("department"),
            str(r.get("title") or ""),
            str(r.get("description") or ""),
        ),
        axis=1,
    )

    df["seniority_normalized"] = df.apply(
        lambda r: detect_seniority(
            str(r.get("title") or ""),
            str(r.get("description") or ""),
            "",
        ),
        axis=1,
    )

    # Безопасно извлекает зарплату из текста описания
    def extract_salary_from_text_safe(text: str):
        """
        Возвращает:
        salary_from, salary_to, currency, period_hint, raw_salary_text
        """
        if not text:
            return None, None, None, None, None

        txt = _clean_text(text, strip_html_tags=True, remove_emoji=True) or ""
        if not txt:
            return None, None, None, None, None

        period = _extract_period_hint(txt)

        patterns = [
            rf"(?P<cur1>{CURRENCY_TOKEN_RE})\s*(?P<n1>{SALARY_NUMBER_RE})\s*(?:-|–|—|to)\s*(?P<cur2>{CURRENCY_TOKEN_RE})?\s*(?P<n2>{SALARY_NUMBER_RE})",
            rf"(?P<n1>{SALARY_NUMBER_RE})\s*(?:-|–|—|to)\s*(?P<n2>{SALARY_NUMBER_RE})\s*(?P<cur3>{CURRENCY_TOKEN_RE})",
            rf"(?P<cur4>{CURRENCY_TOKEN_RE})\s*(?P<n3>{SALARY_NUMBER_RE})",
            rf"(?P<n4>{SALARY_NUMBER_RE})\s*(?P<cur5>{CURRENCY_TOKEN_RE})",
        ]

        for pat in patterns:
            m = re.search(pat, txt, flags=re.I)
            if not m:
                continue

            gd = m.groupdict()
            raw_salary_text = m.group(0)

            currency = (
                _currency_from_token(gd.get("cur1"))
                or _currency_from_token(gd.get("cur2"))
                or _currency_from_token(gd.get("cur3"))
                or _currency_from_token(gd.get("cur4"))
                or _currency_from_token(gd.get("cur5"))
                or detect_currency_from_text(raw_salary_text)
                or detect_currency_from_text(txt)
            )
            currency = _normalize_currency_code(currency)

            if gd.get("n1") and gd.get("n2"):
                sal_from = _parse_number(gd["n1"])
                sal_to = _parse_number(gd["n2"])
                return sal_from, sal_to, currency, period, raw_salary_text

            if gd.get("n3"):
                sal = _parse_number(gd["n3"])
                return sal, sal, currency, period, raw_salary_text

            if gd.get("n4"):
                sal = _parse_number(gd["n4"])
                return sal, sal, currency, period, raw_salary_text

        return None, None, None, None, None

    # Заполняет и нормализует зарплатные поля строки
    def _fix_salary(row) -> pd.Series:
        sal_from = row.get("salary_from")
        sal_to = row.get("salary_to")
        currency = _normalize_currency_code(row.get("currency"))
        salary_period = row.get("salary_period")
        desc = str(row.get("description") or "")

        salary_text = None

        if pd.isna(sal_from) and pd.isna(sal_to):
            parsed_from, parsed_to, parsed_cur, parsed_period, parsed_text = extract_salary_from_text_safe(desc)
            salary_text = parsed_text

            if parsed_from is not None:
                sal_from = parsed_from
            if parsed_to is not None:
                sal_to = parsed_to
            if parsed_cur is not None:
                currency = parsed_cur
            if parsed_period is not None:
                salary_period = parsed_period

        monthly_from, monthly_to = normalize_salary_to_monthly(
            sal_from, sal_to, currency, salary_period
        )
        monthly_from, monthly_to = _sanitize_monthly_salary_bounds(
            monthly_from, monthly_to, currency
        )

        return pd.Series({
            "salary_from": monthly_from,
            "salary_to": monthly_to,
            "currency": currency if monthly_from is not None or monthly_to is not None else None,
            "salary_text_raw": salary_text,
        })

    salary_fixed = df.apply(_fix_salary, axis=1)
    df["salary_from"] = salary_fixed["salary_from"]
    df["salary_to"] = salary_fixed["salary_to"]
    df["currency"] = salary_fixed["currency"]
    df["salary_text_raw"] = salary_fixed["salary_text_raw"]

    # Собирает текстовое представление зарплаты
    def _salary_to_text(row) -> str:
        sf = row.get("salary_from")
        st = row.get("salary_to")
        cur = row.get("currency")
        raw = row.get("salary_text_raw")

        if pd.notna(sf) and pd.notna(st) and not _is_missing(cur):
            return f"{int(sf)} - {int(st)} {cur} per month"
        if pd.notna(sf) and not _is_missing(cur):
            return f"{int(sf)} {cur} per month"
        if pd.notna(st) and not _is_missing(cur):
            return f"{int(st)} {cur} per month"
        if raw and str(raw).strip():
            return str(raw).strip()
        return "нет данных"

    df["salary_text"] = df.apply(_salary_to_text, axis=1)
    df["experience_text"] = df.apply(_experience_to_text, axis=1)

    df["remote_type"] = df.apply(
        lambda r: detect_remote_type(
            str(r.get("title") or ""),
            str(r.get("description") or ""),
            r.get("remote"),
            str(r.get("location") or ""),
        ),
        axis=1,
    )
    df["remote"] = df["remote_type"].isin(["remote", "hybrid"])


    loc_parsed = df["location"].apply(_parse_location_parts)
    df["_parsed_city"] = loc_parsed.apply(lambda x: x[0])
    df["_parsed_country"] = loc_parsed.apply(lambda x: x[1])

    df["country"] = df.apply(
        lambda r: infer_country(
            r.get("_parsed_country") or r.get("country"),
            r.get("location"),
            str(r.get("source") or ""),
            str(r.get("description") or ""),
            str(r.get("company_name") or ""),
        ),
        axis=1,
    )

    df["city"] = df["_parsed_city"].apply(normalize_city_name)

    mask_remote = df["remote_type"] == "remote"
    df.loc[mask_remote, "city"] = None

    if "country_normalized" in df.columns:
        df.drop(columns=["country_normalized"], inplace=True)

    for col in ["_parsed_city", "_parsed_country"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if "city" in df.columns:
        df["city"] = df["city"].apply(lambda x: x.upper() if isinstance(x, str) else x)
    if "country" in df.columns:
        df["country"] = df["country"].apply(lambda x: x.upper() if isinstance(x, str) else x)

    if "currency" in df.columns:
        df["currency"] = df["currency"].apply(_normalize_currency_code)

    has_salary = df["salary_from"].notna() | df["salary_to"].notna()
    missing_currency = _is_missing_series(df["currency"])
    needs_currency = has_salary & missing_currency

    if needs_currency.any():
        df.loc[needs_currency, "currency"] = df.loc[needs_currency, "country"].apply(
            lambda c: COUNTRY_DEFAULT_CURRENCY.get(str(c).upper().strip(), "USD")
            if not _is_missing(c)
            else "USD"
        )
        logger.info("Inferred currency from country for %s rows", int(needs_currency.sum()))

    no_salary = ~has_salary
    if no_salary.any():
        df.loc[no_salary, "currency"] = None

    # Конвертирует сумму в рубли по fallback-курсу
    def _convert_to_rub(amount, currency):
        if pd.isna(amount) or _is_missing(currency):
            return None
        cur = _normalize_currency_code(currency)
        if not cur:
            return None
        if cur == "RUB":
            return round(amount)
        rate = FALLBACK_TO_RUB_RATES.get(cur)
        if rate is None:
            return None
        return round(amount * rate)

    # Проверяет и очищает числовое значение зарплаты
    def _sanitize_salary_number(value):
        if pd.isna(value):
            return None
        try:
            v = float(value)
        except (TypeError, ValueError):
            return None
        if v < 0:
            return None
        if v > 10_000_000_000:
            return None
        return round(v)

    # Проверяет и исправляет диапазон зарплаты
    def _sanitize_salary_range(row) -> pd.Series:
        sf = row.get("salary_from")
        st = row.get("salary_to")
        cur = _normalize_currency_code(row.get("currency")) or "USD"

        lo, hi = MONTHLY_BOUNDS.get(cur, (800, 50000))

        try:
            sf = float(sf) if pd.notna(sf) else None
        except Exception:
            sf = None

        try:
            st = float(st) if pd.notna(st) else None
        except Exception:
            st = None

        if sf is None and st is None:
            return pd.Series({"salary_from": None, "salary_to": None})

        # убираем явно нереалистичные monthly-значения
        if sf is not None and sf > hi * 5:
            sf = None
        if st is not None and st > hi * 5:
            st = None

        # если границы перепутаны местами
        if sf is not None and st is not None and sf > st:
            if sf <= st * 3:
                sf, st = st, sf
            else:
                sf, st = None, None

        if sf is not None and sf > hi * 5:
            sf = None
        if st is not None and st > hi * 5:
            st = None

        return pd.Series({
            "salary_from": round(sf) if sf is not None else None,
            "salary_to": round(st) if st is not None else None,
        })

    required_after_clean = [
        "salary_from",
        "salary_to",
        "salary_from_rub",
        "salary_to_rub",
    ]
    for col in required_after_clean:
        if col not in df.columns:
            df[col] = None

    for col in ["salary_from", "salary_to"]:
        df[col] = df[col].apply(_sanitize_salary_number)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    salary_sane = df.apply(_sanitize_salary_range, axis=1)
    df["salary_from"] = salary_sane["salary_from"]
    df["salary_to"] = salary_sane["salary_to"]

    df["salary_from_rub"] = df.apply(
        lambda r: _convert_to_rub(r.get("salary_from"), r.get("currency")), axis=1
    )
    df["salary_to_rub"] = df.apply(
        lambda r: _convert_to_rub(r.get("salary_to"), r.get("currency")), axis=1
    )

    for col in ["salary_from_rub", "salary_to_rub"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    rub_converted = int(
        (df["salary_from_rub"].notna() | df["salary_to_rub"].notna()).sum()
    )
    logger.info("Converted %s salary rows to RUB (fallback rates)", rub_converted)
    
    if "employment_type" in df.columns:
        df["employment_type"] = df.apply(
            lambda r: normalize_employment_type(
                str(r.get("employment_type") or ""),
                str(r.get("description") or ""),
            ),
            axis=1,
        )

    # Определяет наличие визовой поддержки
    def _detect_visa(row):
        raw = row.get("visa_sponsorship")
        if raw is True:
            return True
        if raw is False:
            return False

        t = str(row.get("description") or "").lower()

        positive = [
            "visa sponsor", "visa sponsorship", "sponsorship available",
            "work permit support", "visa support", "immigration support",
            "визовая поддержка", "спонсорство визы",
        ]
        negative = [
            "no visa", "no sponsorship", "without sponsorship",
            "unable to sponsor", "not eligible for sponsorship",
        ]

        if any(w in t for w in positive):
            return True
        if any(w in t for w in negative):
            return False

        return False

    # Определяет наличие релокации
    def _detect_reloc(row):
        if row.get("relocation") in (True, False):
            return row["relocation"]
        t = str(row.get("description") or "").lower()
        if any(w in t for w in ["relocation", "relocate", "релокац", "переезд", "relocation package"]):
            return True
        return None

    df["visa_sponsorship"] = df.apply(_detect_visa, axis=1)
    df["relocation"] = df.apply(_detect_reloc, axis=1)

    # Spoken languages
    COUNTRY_DEFAULT_LANGUAGES = {
        "UNITED STATES": ["English"],
        "UNITED KINGDOM": ["English"],
        "CANADA": ["English"],
        "AUSTRALIA": ["English"],
        "NEW ZEALAND": ["English"],
        "IRELAND": ["English"],
        "GERMANY": ["German"],
        "FRANCE": ["French"],
        "SPAIN": ["Spanish"],
        "ITALY": ["Italian"],
        "PORTUGAL": ["Portuguese"],
        "BRAZIL": ["Portuguese"],
        "POLAND": ["Polish"],
        "NETHERLANDS": ["Dutch"],
        "BELGIUM": ["Dutch", "French"],
        "SWITZERLAND": ["German", "French"],
        "AUSTRIA": ["German"],
        "FINLAND": ["Finnish"],
        "DENMARK": ["Danish"],
        "NORWAY": ["Norwegian"],
        "SWEDEN": ["Swedish"],
        "CZECH REPUBLIC": ["Czech"],
        "ROMANIA": ["Romanian"],
        "HUNGARY": ["Hungarian"],
        "INDIA": ["English"],
        "SINGAPORE": ["English"],
        "JAPAN": ["Japanese"],
        "CHINA": ["Chinese"],
        "HONG KONG": ["English", "Chinese"],
        "TAIWAN": ["Chinese"],
        "ISRAEL": ["Hebrew"],
        "TURKEY": ["Turkish"],
        "UAE": ["Arabic", "English"],
        "SOUTH KOREA": ["Korean"],
        "MEXICO": ["Spanish"],
        "RUSSIA": ["Russian"],
        "UKRAINE": ["Ukrainian"],
        "BELARUS": ["Russian"],
        "KAZAKHSTAN": ["Russian"],
        "UZBEKISTAN": ["Russian"],
        "BULGARIA": ["Bulgarian"],
        "SOUTH AFRICA": ["English"],
    }

    # Определяет языки вакансии по данным строки
    def _detect_langs(row):
        existing = row.get("spoken_languages")
        if not _is_missing(existing) and str(existing) not in ("{}", "[]"):
            return existing

        country = str(row.get("country") or "").strip().upper()
        langs = COUNTRY_DEFAULT_LANGUAGES.get(country)

        if langs:
            return _to_pg_array(langs)

        return _to_pg_array(["English"])
    df["posting_language"] = df["description"].apply(
    lambda d: detect_posting_language(str(d) if not _is_missing(d) else "")
)

    df["spoken_languages"] = df.apply(_detect_langs, axis=1)

    if "url" in df.columns and "source" in df.columns:
        mask_no_url = df["url"].isna() | (df["url"].astype(str).str.strip() == "")
        for src_name, fallback_url in SOURCE_FALLBACK_URLS.items():
            src_mask = mask_no_url & (df["source"].astype(str).str.lower() == src_name)
            df.loc[src_mask, "url"] = fallback_url

    # Проверяет совпадение роли с набором ключевых слов
    def _role_match(title: str, family_keywords: list[str]) -> bool:
        t = str(title or "").lower()
        return any(k in t for k in family_keywords)

    df["is_data_role"] = df["title_normalized"].apply(lambda t: _role_match(t, _ROLE_FAMILIES["data_scientist"]) or _role_match(t, _ROLE_FAMILIES["data_engineer"]) or _role_match(t, _ROLE_FAMILIES["data_analyst"]))
    df["is_ml_role"] = df["title_normalized"].apply(lambda t: _role_match(t, _ROLE_FAMILIES["ml_engineer"]) or _role_match(t, _ROLE_FAMILIES["nlp_engineer"]) or _role_match(t, _ROLE_FAMILIES["cv_engineer"]) or _role_match(t, _ROLE_FAMILIES["researcher"]))
    df["is_python_role"] = df["title_normalized"].apply(lambda t: "python" in str(t or "").lower())
    df["is_analyst_role"] = df["title_normalized"].apply(lambda t: _role_match(t, _ROLE_FAMILIES["data_analyst"]))


    # Cleanup / ordering
    if "_dedupe_key" in df.columns:
        df.drop(columns=["_dedupe_key"], inplace=True)

    for col in ["region"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)


    for col in ["salary_period", "salary_text_raw"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    existing = [c for c in FINAL_COLUMN_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    df = df[existing + rest]

    # Метрики качества
    try:
        missing_country = int(df["country"].isna().sum()) if "country" in df.columns else 0
        remote_count = int((df["remote_type"] == "remote").sum()) if "remote_type" in df.columns else 0
        non_remote_no_country = int(
            ((df["remote_type"] != "remote") & df["country"].isna()).sum()
        ) if {"remote_type", "country"}.issubset(df.columns) else 0

        logger.info(
            "Clean quality | rows=%s | dropped=%s | missing_country=%s (non-remote: %s) | remote=%s",
            len(df), initial - len(df), missing_country, non_remote_no_country, remote_count,
        )

        if "source" in df.columns:
            logger.info("Rows by source: %s", df["source"].value_counts(dropna=False).to_dict())

    except Exception as e:
        logger.warning("Failed to calculate clean quality metrics: %s", e)

    logger.info("Cleaning done: %s rows (dropped %s)", len(df), initial - len(df))
    return df


# Колонки, которые идут в Qdrant (payload + текст для эмбеддинга).
QDRANT_COLUMNS = [
    "job_id",
    "title",
    "title_normalized",
    "description",
    "company_name",
    "specialty",
    "specialty_category",
    "role_family",
    "country",
    "city",
    "remote_type",
    "seniority_normalized",
    "skills_normalized",
    "salary_from",
    "salary_to",
    "currency",
    "salary_from_rub",
    "salary_to_rub",
    "employment_type",
    "url",
    "source",
    "visa_sponsorship",
    "relocation"
]

# Все колонки чистого датасета (PostgreSQL + аналитика).
CLEAN_DATASET_COLUMNS = [
    "job_id",
    "source_job_id",
    "source",
    "url",
    "title",
    "title_normalized",
    "description",
    "company_name",
    "department",
    "country",
    "city",
    "location",
    "remote",
    "remote_type",
    "employment_type",
    "seniority_normalized",
    "years_experience_min",
    "years_experience_max",
    "salary_from",
    "salary_to",
    "currency",
    "salary_from_rub",
    "salary_to_rub",
    "skills_normalized",
    "spoken_languages",
    "is_data_role",
    "is_ml_role",
    "is_python_role",
    "is_analyst_role",
    "visa_sponsorship",
    "relocation",
    "published_at",
    "parsed_at",
]
FINAL_COLUMN_ORDER = [
    "job_id",
    "source_job_id",
    "source",
    "url",
    "title",
    "title_normalized",
    "description",
    "company_name",
    "department",
    "specialty",
    "specialty_category",
    "role_family",
    "location",
    "country",
    "city",
    "remote",
    "remote_type",
    "employment_type",
    "seniority_normalized",
    "years_experience_min",
    "years_experience_max",
    "salary_from",
    "salary_to",
    "currency",
    "salary_from_rub",
    "salary_to_rub",
    "salary_text",
    "experience_text",
    "skills_normalized",
    "spoken_languages",
    "posting_language",
    "visa_sponsorship",
    "relocation",
    "is_data_role",
    "is_ml_role",
    "is_python_role",
    "is_analyst_role",
    "published_at",
    "parsed_at",
]

# Строит ключ для объединения cleaned-слоёв
def _merge_dedupe_key(row) -> str:
    source = _safe_key_part(row.get("source"))
    source_job_id = _safe_key_part(row.get("source_job_id"))
    url = _safe_key_part(row.get("url")).lower()
    job_id = _safe_key_part(row.get("job_id"))
    title = _safe_key_part(row.get("title")).lower()
    company = _safe_key_part(row.get("company_name") or row.get("company")).lower()
    if source and source_job_id:
        return f"srcid::{source}::{source_job_id}"
    if source and url:
        return f"url::{source}::{url}"
    if job_id:
        return f"job::{job_id}"
    return f"title::{source}::{company}::{title}"

# Объединяет новый cleaned-снимок с latest-версиейс
def _merge_cleaned_with_latest(old_latest: pd.DataFrame, cleaned_new: pd.DataFrame) -> pd.DataFrame:
    if old_latest is None or old_latest.empty:
        return cleaned_new
    if cleaned_new is None or cleaned_new.empty:
        return old_latest

    latest = old_latest.copy()
    latest["_merge_key"] = latest.apply(_merge_dedupe_key, axis=1)
    latest["_merge_priority"] = 0

    new = cleaned_new.copy()
    new["_merge_key"] = new.apply(_merge_dedupe_key, axis=1)
    new["_merge_priority"] = 1

    combined = pd.concat([latest, new], ignore_index=True)
    combined = combined.sort_values(["_merge_key", "_merge_priority"])
    combined = combined.drop_duplicates(subset=["_merge_key"], keep="last")
    combined = combined.drop(columns=["_merge_key", "_merge_priority"], errors="ignore")
    combined = combined.reset_index(drop=True)
    logger.info(
        "Merged: old=%s, new=%s, final=%s", len(old_latest), len(cleaned_new), len(combined),
    )
    return combined

# Запускает clean-шаг и сохраняет snapshot и latest в S3
def run_clean_step(
    date_str: str = None,
    raw_s3_keys: list[str] | None = None,
    merge_with_latest: bool = True,
) -> str:
    from src.loaders.s3_storage import (
        clean_key,
        download_df,
        ensure_bucket,
        list_keys,
        make_run_id,
        upload_df,
        key_exists,
        latest_clean_key,
    )

    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    run_id = make_run_id(date_str)
    ensure_bucket()

    if raw_s3_keys:
        raw_keys = sorted({k for k in raw_s3_keys if k and str(k).endswith(".csv")})
        logger.info("Using raw_s3_keys: %s file(s)", len(raw_keys))
    else:
        prefix = f"raw/{date_str}/"
        raw_keys = [k for k in list_keys(prefix) if k.endswith(".csv")]
        logger.warning("raw_s3_keys not provided; using prefix %s (%s files)", prefix, len(raw_keys))

    if not raw_keys:
        logger.warning("No raw files to clean for %s", date_str)
        return ""

    dfs = []
    for key in raw_keys:
        try:
            d = download_df(key)
            if not d.empty:
                dfs.append(d)
                logger.info("Downloaded %s: %s rows", key, len(d))
        except Exception as e:
            logger.error("Failed to download %s: %s", key, e)

    if not dfs:
        logger.warning("All raw files failed or empty")
        return ""

    merged = pd.concat(dfs, ignore_index=True)
    logger.info("Merged raw: %s rows from %s files", len(merged), len(dfs))

    cleaned_new = clean_dataframe(merged)
    logger.info("Clean summary | raw=%s | clean=%s", len(merged), len(cleaned_new))

    # 1) Сохраняем snapshot ТОЛЬКО текущего запуска
    snapshot_key = clean_key(date_str, run_id=run_id)
    upload_df(cleaned_new, snapshot_key)

    # 2) Обновляем накопительный latest отдельно
    latest_key = latest_clean_key()
    latest_df = cleaned_new

    if merge_with_latest and key_exists(latest_key):
        try:
            old_latest = download_df(latest_key)
            latest_df = _merge_cleaned_with_latest(old_latest, cleaned_new)
        except Exception as e:
            logger.warning("Failed to load/merge previous latest: %s", e)
            latest_df = cleaned_new

    upload_df(latest_df, latest_key)

    logger.info(
        "Snapshot saved: %s (%s rows); latest updated: %s (%s rows)",
        snapshot_key,
        len(cleaned_new),
        latest_key,
        len(latest_df),
    )
    return snapshot_key


if __name__ == "__main__":
    key = run_clean_step()
    print(f"Clean: {key}")