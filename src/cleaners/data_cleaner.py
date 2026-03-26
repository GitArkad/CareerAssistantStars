"""
data_cleaner.py

Full cleaning, enrichment, and normalization pipeline.

What it does:
1.  Deduplication (url, job_id)
2.  Text cleaning (HTML, whitespace)
3.  SKILL EXTRACTION FROM DESCRIPTION — the main fix
4.  Salary parsing from description text (£45,000-£55,000 → salary_from/to)
5.  Currency detection from description (£ → GBP, not USD)
6.  Salary range splitting ("50000-70000" in one field → two fields)
7.  Seniority detection from title + description
8.  Remote type detection
9.  Country/city normalization
10. Skill normalization via synonym map
11. Employment type normalization
12. Build unified output for embeddings/models

Output columns (unified table):
    job_id, title, title_normalized, description, requirements,
    company_name, salary_from, salary_to, currency, salary_period,
    seniority_normalized, years_experience_min, years_experience_max,
    skills (list), tools (list), methodologies (list),
    location, country, city, remote, remote_type, employment_type,
    source, url, search_query, published_at, parsed_at
"""

from __future__ import annotations

import re
import ast
import json
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================================
# SKILL EXTRACTION — comprehensive keyword list
# ============================================================================

# Skills to search for in description text (lowercased for matching)
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
    # Methodologies (also extracted as skills)
    "agile", "scrum", "kanban", "devops", "mlops", "ci/cd",
    "microservices", "tdd", "bdd", "soa",
}

# Patterns that need word-boundary matching (avoid false positives)
# "r" alone matches too many things, "go" matches "going", etc.
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

# Skills that are tools (subset)
TOOLS_SET = {
    "git", "jira", "confluence", "docker", "kubernetes", "jenkins",
    "gitlab ci", "github actions", "slack", "notion", "figma", "postman",
    "terraform", "ansible", "datadog", "grafana", "prometheus",
    "swagger", "sentry", "pagerduty",
}

METHODOLOGIES_SET = {
    "agile", "scrum", "kanban", "devops", "mlops", "ci/cd",
    "microservices", "tdd", "bdd", "soa",
}


def extract_skills_from_text(text: str) -> List[str]:
    """Extract ALL skills from any text (description, requirements, etc.)."""
    if not text:
        return []
    t = text.lower()
    found = set()

    for skill in EXTRACTABLE_SKILLS:
        if skill in EXACT_MATCH_SKILLS:
            # Exact word boundary match
            if re.search(rf"\b{re.escape(skill)}\b", t):
                found.add(skill)
        else:
            # Substring is fine for multi-word skills and most tech terms
            if skill in t:
                found.add(skill)

    # Special cases
    if re.search(r"\bc\+\+\b", t):
        found.add("c++")
    if re.search(r"\bc#\b", t):
        found.add("c#")
    if re.search(r"\b(?:node\.?js|node js)\b", t):
        found.add("node.js")
    if re.search(r"\b(?:vue\.?js|vuejs)\b", t):
        found.add("vue")
    if re.search(r"\b(?:react\.?js|reactjs)\b", t):
        found.add("react")
    if re.search(r"\b\.net\b", t):
        found.add(".net")
    if re.search(r"\bpower\s*bi\b", t):
        found.add("power bi")
    if re.search(r"\bgithub\s*actions?\b", t):
        found.add("github actions")
    if re.search(r"\bgitlab[\s-]*ci\b", t):
        found.add("gitlab ci")
    if re.search(r"\bci\s*/?\s*cd\b", t):
        found.add("ci/cd")
    if any(re.search(pattern, t) for pattern in R_LANGUAGE_CONTEXT_PATTERNS):
        found.add("r")
    # Uppercase R detection — catches R in comma/slash-separated skill lists
    # Uses original case `text` because \bR\b must match uppercase only
    if re.search(
        r"(?:"
        r"\bR\b\s*(?:[,/;]|and\b|or\b|\+)"
        r"|(?:^|[,/;]\s*)\bR\b"
        r"|\bR\s+(?:programming|language|studio|shiny|markdown|package|cran|tidyverse|ggplot|dplyr)\b"
        r"|\b(?:programming\s+in|experience\s+with|knowledge\s+of|proficiency\s+in)\s+R\b"
        r"|\bR\s*$"
        r")", text
    ):
        found.add("r")

    return sorted(found)


# ============================================================================
# SKILL NORMALIZATION (synonym map)
# ============================================================================

SKILL_SYNONYMS = {
    "python": "Python", "py": "Python", "python3": "Python",
    "java": "Java", "javascript": "JavaScript", "js": "JavaScript",
    "typescript": "TypeScript", "ts": "TypeScript",
    "scala": "Scala", "go": "Go", "golang": "Go",
    "rust": "Rust", "r_lang": "R", "r": "R", "c++": "C++", "c#": "C#",
    "swift": "Swift", "kotlin": "Kotlin", "ruby": "Ruby",
    "php": "PHP", "perl": "Perl", "dart": "Dart",
    "julia": "Julia", "matlab": "MATLAB", "lua": "Lua",
    ".net": ".NET", "solidity": "Solidity",
    "sql": "SQL", "postgresql": "PostgreSQL", "postgres": "PostgreSQL",
    "mysql": "MySQL", "mongodb": "MongoDB", "redis": "Redis",
    "elasticsearch": "Elasticsearch", "cassandra": "Cassandra",
    "dynamodb": "DynamoDB", "neo4j": "Neo4j", "clickhouse": "ClickHouse",
    "sqlite": "SQLite", "mariadb": "MariaDB", "oracle": "Oracle",
    "cockroachdb": "CockroachDB", "supabase": "Supabase", "firebase": "Firebase",
    "aws": "AWS", "azure": "Azure", "gcp": "GCP", "google cloud": "GCP",
    "docker": "Docker", "kubernetes": "Kubernetes", "k8s": "Kubernetes",
    "terraform": "Terraform", "ansible": "Ansible", "jenkins": "Jenkins",
    "github actions": "GitHub Actions", "gitlab ci": "GitLab CI",
    "helm": "Helm", "istio": "Istio", "nginx": "Nginx",
    "linux": "Linux", "bash": "Bash", "shell": "Shell",
    "spark": "Apache Spark", "pyspark": "Apache Spark",
    "kafka": "Apache Kafka", "airflow": "Apache Airflow",
    "dbt": "dbt", "snowflake": "Snowflake", "bigquery": "BigQuery",
    "redshift": "Redshift", "databricks": "Databricks",
    "flink": "Apache Flink", "hive": "Hive", "presto": "Presto",
    "delta lake": "Delta Lake", "iceberg": "Iceberg",
    "dask": "Dask", "ray": "Ray", "prefect": "Prefect", "dagster": "Dagster",
    "pytorch": "PyTorch", "tensorflow": "TensorFlow", "keras": "Keras",
    "jax": "JAX", "scikit-learn": "Scikit-learn", "sklearn": "Scikit-learn",
    "xgboost": "XGBoost", "lightgbm": "LightGBM", "catboost": "CatBoost",
    "huggingface": "Hugging Face", "langchain": "LangChain",
    "llamaindex": "LlamaIndex", "opencv": "OpenCV",
    "spacy": "spaCy", "nltk": "NLTK",
    "pandas": "Pandas", "numpy": "NumPy", "scipy": "SciPy",
    "matplotlib": "Matplotlib", "plotly": "Plotly",
    "mlflow": "MLflow", "wandb": "W&B", "dvc": "DVC",
    "kubeflow": "Kubeflow", "sagemaker": "SageMaker", "vertex ai": "Vertex AI",
    "fastapi": "FastAPI", "flask": "Flask", "django": "Django",
    "express": "Express", "spring": "Spring", "spring boot": "Spring Boot",
    "rails": "Rails", "laravel": "Laravel",
    "nextjs": "Next.js", "nuxt": "Nuxt", "svelte": "Svelte",
    "graphql": "GraphQL", "rest api": "REST API", "grpc": "gRPC",
    "react": "React", "vue": "Vue", "angular": "Angular",
    "tailwind": "Tailwind CSS", "webpack": "Webpack", "vite": "Vite",
    "react native": "React Native", "flutter": "Flutter",
    "node.js": "Node.js",
    "git": "Git", "jira": "Jira", "confluence": "Confluence",
    "figma": "Figma", "postman": "Postman", "notion": "Notion",
    "datadog": "Datadog", "grafana": "Grafana", "prometheus": "Prometheus",
    "splunk": "Splunk", "elastic": "Elastic", "sentry": "Sentry",
    "power bi": "Power BI", "tableau": "Tableau", "looker": "Looker",
    "metabase": "Metabase", "excel": "Excel",
    "agile": "Agile", "scrum": "Scrum", "kanban": "Kanban",
    "devops": "DevOps", "mlops": "MLOps", "ci/cd": "CI/CD",
    "microservices": "Microservices", "tdd": "TDD",
}


def normalize_skills(raw_skills: List[str]) -> List[str]:
    """Normalize skill names to canonical form."""
    normalized = set()
    for skill in raw_skills:
        key = skill.lower().strip()
        canonical = SKILL_SYNONYMS.get(key, skill)
        normalized.add(canonical)
    return sorted(normalized)


# ============================================================================
# SALARY PARSING FROM DESCRIPTION
# ============================================================================

# Currency symbols → code
CURRENCY_SYMBOLS = {
    "£": "GBP", "€": "EUR", "$": "USD", "₽": "RUB",
    "₹": "INR", "zł": "PLN", "₴": "UAH", "₸": "KZT",
    "a$": "AUD", "c$": "CAD", "s$": "SGD",
}

# Currency words → code
CURRENCY_WORDS = {
    "gbp": "GBP", "eur": "EUR", "usd": "USD", "rub": "RUB",
    "rur": "RUB", "руб": "RUB", "рублей": "RUB",
    "pln": "PLN", "uah": "UAH", "kzt": "KZT",
    "cad": "CAD", "aud": "AUD", "inr": "INR", "sgd": "SGD",
    "chf": "CHF", "sek": "SEK", "nok": "NOK", "dkk": "DKK",
    "per annum": None, "p.a.": None, "pa": None,  # period markers, not currency
}


def _parse_number(s: str) -> Optional[float]:
    """Parse a number string like '45,000' or '45000' or '45k'."""
    s = s.strip().replace(",", "").replace(" ", "")
    if s.lower().endswith("k"):
        try:
            return float(s[:-1]) * 1000
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_salary_from_text(text: str) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    """
    Parse salary range and currency from description text.
    Returns (salary_from, salary_to, currency, period).

    Handles:
    - £45,000 - £55,000
    - $120k-$150k
    - 50000-70000 EUR
    - от 300000 до 500000 руб
    - Competitive salary between £45,000 and £50,000
    - €3,500/month
    """
    if not text:
        return None, None, None, None

    # Detect period
    period = None
    text_lower = text.lower()
    if any(w in text_lower for w in ["/month", "per month", "в месяц", "monthly", "/mo"]):
        period = "month"
    elif any(w in text_lower for w in ["/hour", "per hour", "hourly", "в час"]):
        period = "hour"
    elif any(w in text_lower for w in ["/day", "per day", "daily", "в день"]):
        period = "day"

    patterns = [
        # £45,000 - £55,000 or $120k-$150k
        r"([£€$₽₹])\s*([\d,]+\.?\d*k?)\s*[-–—to]+\s*[£€$₽₹]?\s*([\d,]+\.?\d*k?)",
        # Between £45,000 and £50,000
        r"between\s*([£€$₽₹])\s*([\d,]+\.?\d*k?)\s*and\s*[£€$₽₹]?\s*([\d,]+\.?\d*k?)",
        # от 300000 до 500000 (руб)
        r"от\s*([\d\s,]+)\s*(?:до)\s*([\d\s,]+)\s*(руб|rub|rur|₽)?",
        # 50000-70000 EUR/USD/GBP
        r"([\d,]+\.?\d*k?)\s*[-–—]\s*([\d,]+\.?\d*k?)\s*(EUR|USD|GBP|RUB|PLN|CAD|AUD|INR|SGD)",
        # £45,000 (single value with symbol)
        r"([£€$₽₹])\s*([\d,]+\.?\d*k?)\s*(?:per|p\.?a|/)",
        # Just a symbol + number in salary context
        r"salary[:\s]*([£€$₽₹])\s*([\d,]+\.?\d*k?)",
    ]

    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        groups = m.groups()

        if len(groups) == 3 and groups[0] in CURRENCY_SYMBOLS:
            # Pattern: symbol, min, max
            currency = CURRENCY_SYMBOLS.get(groups[0], "USD")
            sal_min = _parse_number(groups[1])
            sal_max = _parse_number(groups[2])
            if sal_min and sal_max:
                return sal_min, sal_max, currency, period

        elif len(groups) == 3 and groups[0] not in CURRENCY_SYMBOLS:
            # Pattern: min, max, currency_word
            sal_min = _parse_number(groups[0])
            sal_max = _parse_number(groups[1])
            cur_word = (groups[2] or "").lower().strip()
            currency = CURRENCY_WORDS.get(cur_word) or CURRENCY_SYMBOLS.get(cur_word)
            if sal_min and sal_max and currency:
                return sal_min, sal_max, currency, period

        elif len(groups) == 2:
            # Single value
            if groups[0] in CURRENCY_SYMBOLS:
                currency = CURRENCY_SYMBOLS[groups[0]]
                sal = _parse_number(groups[1])
                if sal:
                    return sal, sal, currency, period
            else:
                sal_min = _parse_number(groups[0])
                sal_max = _parse_number(groups[1])
                if sal_min and sal_max:
                    return sal_min, sal_max, None, period

    return None, None, None, None


def detect_currency_from_text(text: str) -> Optional[str]:
    """Detect currency from any text by looking for symbols and codes."""
    if not text:
        return None
    # Check symbols first (most reliable)
    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in text:
            return code
    # Check currency codes
    for word, code in CURRENCY_WORDS.items():
        if code and re.search(rf"\b{re.escape(word)}\b", text.lower()):
            return code
    return None


# ============================================================================
# SENIORITY DETECTION
# ============================================================================

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


def detect_seniority(title: str = "", description: str = "", experience: str = "") -> str:
    """Detect seniority from multiple fields."""
    # Priority: experience_level → title → description
    for text in [experience, title, description[:300]]:
        if not text:
            continue
        t = text.lower()
        for keyword, level in SENIORITY_MAP.items():
            if keyword in t:
                return level
    return "unknown"


# ============================================================================
# EMPLOYMENT TYPE NORMALIZATION
# ============================================================================

def normalize_employment_type(val: str, description: str = "") -> str:
    if not val and not description:
        return "unknown"
    text = f"{val or ''} {(description or '')[:300]}".lower()
    if any(w in text for w in ["full_time", "full-time", "fulltime", "полная занятость", "полный день"]):
        return "full_time"
    if any(w in text for w in ["part_time", "part-time", "parttime", "частичная", "неполный"]):
        return "part_time"
    if any(w in text for w in ["contract", "контракт", "b2b"]):
        return "contract"
    if any(w in text for w in ["freelance", "фриланс"]):
        return "freelance"
    if any(w in text for w in ["internship", "стажировка"]):
        return "internship"
    return val or "unknown"


# ============================================================================
# REMOTE TYPE DETECTION
# ============================================================================

def detect_remote_type(title: str = "", description: str = "", remote_flag=None) -> str:
    text = f"{title or ''} {(description or '')[:500]}".lower()
    if any(w in text for w in ["hybrid", "гибрид", "гибридн", "2-3 days in office", "2 days in office"]):
        return "hybrid"
    if remote_flag in (True, "True", "true", 1):
        return "remote"
    if any(w in text for w in ["remote", "удалённ", "удален", "дистанц", "work from home", "wfh"]):
        return "remote"
    if any(w in text for w in ["on-site", "onsite", "in-office", "in office", "офис"]):
        return "onsite"
    return "unknown"


# ============================================================================
# HELPERS
# ============================================================================

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


def _to_pg_array(items: List[str]) -> str:
    """Serialize lists as JSON text for CSV snapshots; loader converts them back to Python lists."""
    clean = [str(x).strip() for x in items if str(x).strip()]
    return json.dumps(clean, ensure_ascii=False)


def normalize_title(title: str) -> Optional[str]:
    if not title:
        return None
    t = title.lower().strip()
    t = re.sub(r"[^a-zа-яё0-9\s+#.]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def extract_city_from_location(location: str) -> Optional[str]:
    """Extract city name from location string."""
    if not location:
        return None
    # "London, UK" → "London"
    # "San Francisco, CA" → "San Francisco"
    parts = location.split(",")
    return parts[0].strip() if parts else None


def _extract_years_from_text(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    for pat in [
        r"(\d+)\s*[-–—]\s*(\d+)\s*(?:лет|years?|год)",
        r"(?:от|from)\s*(\d+)\s*(?:лет|years?)",
        r"(\d+)\+\s*(?:лет|years?)",
        r"(\d+)\s*(?:лет|years?|год)",
    ]:
        m = re.search(pat, text.lower())
        if m:
            g = m.groups()
            return (int(g[0]), int(g[1])) if len(g) == 2 else (int(g[0]), int(g[0]))
    return None, None


# ============================================================================
# MAIN CLEANING PIPELINE
# ============================================================================

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning + enrichment pipeline.
    Input: raw merged DataFrame from all parsers.
    Output: clean, enriched DataFrame ready for Postgres + Qdrant.
    """
    initial = len(df)
    logger.info(f"Cleaning: {initial} rows")
    df = df.copy()

    # ── 1. Dedup ──
    def _dedupe_key(row):
        source = str(row.get("source") or "")
        source_job_id = str(row.get("source_job_id") or "").strip()
        url = str(row.get("url") or "").strip().lower()
        job_id = str(row.get("job_id") or "").strip()
        title = str(row.get("title") or "").strip().lower()
        company = str(row.get("company_name") or "").strip().lower()
        if source and source_job_id:
            return f"srcid::{source}::{source_job_id}"
        if source and url:
            return f"url::{source}::{url}"
        if job_id:
            return f"job::{job_id}"
        return f"title::{source}::{company}::{title}"

    df["_dedupe_key"] = df.apply(_dedupe_key, axis=1)
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="first")

    # ── 2. Drop empty titles ──
    if "title" in df.columns:
        df = df.dropna(subset=["title"])
        df = df[df["title"].astype(str).str.strip() != ""]

    # ── 3. Text cleaning ──
    for col in ["title", "description", "company_name", "requirements",
                 "responsibilities", "nice_to_have"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.strip()
                .str.replace(r"\s+", " ", regex=True)
                .replace({"nan": None, "None": None, "": None})
            )

    # ── 4. EXTRACT SKILLS FROM DESCRIPTION + TITLE + REQUIREMENTS ──
    logger.info("Extracting skills from text fields...")

    def _extract_all_skills(row):
        """Combine text fields and extract all skills."""
        texts = []
        for col in ["description", "title", "requirements", "responsibilities", "nice_to_have"]:
            v = row.get(col)
            if v and str(v) not in ("None", "nan", ""):
                texts.append(str(v))
        full_text = " ".join(texts)
        return extract_skills_from_text(full_text)

    df["_all_skills"] = df.apply(_extract_all_skills, axis=1)

    # Merge extracted text skills with source-provided key_skills.
    # Cleaner is the single source of truth for skills_extracted/tools/methodologies.
    if "key_skills" in df.columns:
        df["_key_skills"] = df["key_skills"].apply(_parse_pg_array)
    else:
        df["_key_skills"] = [[] for _ in range(len(df))]

    if "skills_extracted" in df.columns:
        df["_legacy_skills"] = df["skills_extracted"].apply(_parse_pg_array)
    else:
        df["_legacy_skills"] = [[] for _ in range(len(df))]

    df["_merged_skills"] = df.apply(
        lambda r: list(dict.fromkeys(r["_all_skills"] + r["_key_skills"] + r["_legacy_skills"])),
        axis=1,
    )

    # Separate into skills, tools, methodologies
    df["skills_extracted"] = df["_merged_skills"].apply(
        lambda s: _to_pg_array(sorted(set(s))))
    df["tools"] = df["_merged_skills"].apply(
        lambda s: _to_pg_array(sorted(set(x for x in s if x in TOOLS_SET))))
    df["methodologies"] = df["_merged_skills"].apply(
        lambda s: _to_pg_array(sorted(set(x for x in s if x in METHODOLOGIES_SET))))
    df["tech_stack_tags"] = df["skills_extracted"]

    df.drop(columns=["_all_skills", "_key_skills", "_legacy_skills", "_merged_skills"], inplace=True)

    # ── 5. SKILL NORMALIZATION ──
    df["skills_normalized"] = df["skills_extracted"].apply(
        lambda s: _to_pg_array(normalize_skills(_parse_pg_array(s))))

    # ── 6. SALARY: parse from description if missing ──
    logger.info("Parsing salaries from descriptions...")

    def _fix_salary(row):
        sal_from = row.get("salary_from")
        sal_to = row.get("salary_to")
        currency = row.get("currency")
        desc = str(row.get("description") or "")

        # Try to detect real currency from description (override wrong USD)
        desc_currency = detect_currency_from_text(desc)
        if desc_currency and (not currency or currency == "USD"):
            # If API said USD but description has £ → it's GBP
            currency = desc_currency

        # If salary fields are empty, try parsing from description
        if pd.isna(sal_from) and pd.isna(sal_to):
            parsed_from, parsed_to, parsed_cur, parsed_period = extract_salary_from_text(desc)
            if parsed_from:
                sal_from = parsed_from
            if parsed_to:
                sal_to = parsed_to
            if parsed_cur:
                currency = parsed_cur

        # Handle single-column salary ("50000-70000" as string in salary_from)
        if isinstance(sal_from, str) and "-" in sal_from:
            parts = sal_from.split("-")
            if len(parts) == 2:
                v1 = _parse_number(parts[0])
                v2 = _parse_number(parts[1])
                if v1 and v2:
                    sal_from = v1
                    sal_to = v2

        return pd.Series({
            "salary_from": pd.to_numeric(sal_from, errors="coerce"),
            "salary_to": pd.to_numeric(sal_to, errors="coerce"),
            "currency": currency,
        })

    salary_fixed = df.apply(_fix_salary, axis=1)
    df["salary_from"] = salary_fixed["salary_from"]
    df["salary_to"] = salary_fixed["salary_to"]
    df["currency"] = salary_fixed["currency"]

    # Drop invalid salary ranges (from > to)
    mask_bad = (
        df["salary_from"].notna() & df["salary_to"].notna()
        & (df["salary_from"] > df["salary_to"])
    )
    if mask_bad.any():
        # Swap instead of drop — might just be reversed
        swap_mask = mask_bad
        df.loc[swap_mask, ["salary_from", "salary_to"]] = (
            df.loc[swap_mask, ["salary_to", "salary_from"]].values
        )
        logger.info(f"Swapped {swap_mask.sum()} reversed salary ranges")

    # ── 7. SENIORITY ──
    df["seniority_normalized"] = df.apply(
        lambda r: detect_seniority(
            str(r.get("title") or ""),
            str(r.get("description") or ""),
            str(r.get("experience_level") or ""),
        ), axis=1)

    # ── 8. YEARS EXPERIENCE ──
    def _get_years(row):
        # Try experience_level first, then description
        for col in ["experience_level", "description"]:
            v = str(row.get(col) or "")
            y1, y2 = _extract_years_from_text(v)
            if y1 is not None:
                return pd.Series({"years_experience_min": y1, "years_experience_max": y2})
        return pd.Series({"years_experience_min": None, "years_experience_max": None})

    years = df.apply(_get_years, axis=1)
    df["years_experience_min"] = years["years_experience_min"]
    df["years_experience_max"] = years["years_experience_max"]

    # ── 9. COUNTRY / CITY ──
    if "country" in df.columns:
        df["country"] = df["country"].astype(str).str.strip().str.upper().replace({"NAN": None, "NONE": None})
    if "location" in df.columns:
        df["city"] = df["location"].apply(extract_city_from_location)

    # ── 10. TITLE NORMALIZED ──
    if "title" in df.columns:
        df["title_normalized"] = df["title"].apply(normalize_title)

    # ── 11. REMOTE TYPE ──
    df["remote_type"] = df.apply(
        lambda r: detect_remote_type(
            str(r.get("title") or ""),
            str(r.get("description") or ""),
            r.get("remote"),
        ), axis=1)
    df["remote"] = df["remote_type"].isin(["remote", "hybrid"])

    # ── 12. EMPLOYMENT TYPE ──
    if "employment_type" in df.columns:
        df["employment_type"] = df.apply(
            lambda r: normalize_employment_type(
                str(r.get("employment_type") or ""),
                str(r.get("description") or ""),
            ), axis=1)

    # ── 13. KEY SKILLS as pg array ──
    if "key_skills" in df.columns:
        df["key_skills"] = df["key_skills"].apply(
            lambda x: _to_pg_array(_parse_pg_array(x)))

    # ── 14. VISA SPONSORSHIP (extract from description) ──
    def _detect_visa(row):
        if row.get("visa_sponsorship") in (True, False):
            return row["visa_sponsorship"]
        t = str(row.get("description") or "").lower()
        if any(w in t for w in ["visa sponsor", "visa sponsorship", "визовая поддержка", "work permit"]):
            return True
        if any(w in t for w in ["no visa", "no sponsorship"]):
            return False
        return None
    df["visa_sponsorship"] = df.apply(_detect_visa, axis=1)

    # ── 15. RELOCATION (extract from description) ──
    def _detect_reloc(row):
        if row.get("relocation") in (True, False):
            return row["relocation"]
        t = str(row.get("description") or "").lower()
        if any(w in t for w in ["relocation", "relocate", "релокац", "переезд", "relocation package"]):
            return True
        return None
    df["relocation"] = df.apply(_detect_reloc, axis=1)

    # ── 16. SPOKEN LANGUAGES (extract from description) ──
    _LANG_KW = {
        "English": ["english", "английск"],
        "German": ["german", "deutsch", "немецк"],
        "French": ["french", "français", "французск"],
        "Spanish": ["spanish", "español", "испанск"],
        "Russian": ["russian", "русск"],
        "Chinese": ["chinese", "mandarin", "китайск"],
        "Japanese": ["japanese", "японск"],
        "Portuguese": ["portuguese"],
        "Arabic": ["arabic", "арабск"],
    }
    def _detect_langs(row):
        existing = row.get("spoken_languages")
        if existing and str(existing) not in ("", "{}", "nan", "None"):
            return existing
        t = str(row.get("description") or "").lower()
        found = [lang for lang, kws in _LANG_KW.items() if any(kw in t for kw in kws)]
        return _to_pg_array(found) if found else "[]"
    df["spoken_languages"] = df.apply(_detect_langs, axis=1)

    # ── 17. SALARY PERIOD (detect from description) ──
    def _detect_period(row):
        if row.get("salary_period") and str(row["salary_period"]).lower() not in ("nan", "none", "", "null"):
            return row["salary_period"]
        t = str(row.get("description") or "").lower()
        if any(w in t for w in ["/month", "per month", "в месяц", "monthly"]):
            return "month"
        if any(w in t for w in ["/hour", "per hour", "hourly", "в час"]):
            return "hour"
        if any(w in t for w in ["/day", "per day", "daily"]):
            return "day"
        return None
    df["salary_period"] = df.apply(_detect_period, axis=1)

    # ══════════════════════════════════════════════════════════
    # 18-23. DERIVED / COMPUTED FIELDS
    # ══════════════════════════════════════════════════════════

    # ── 18. ROLE FAMILY ──
    _ROLE_FAMILIES = {
        "data_scientist": ["data scientist", "дата сайентист", "ученый по данным"],
        "ml_engineer": ["machine learning", "ml engineer", "deep learning", "ai engineer"],
        "data_engineer": ["data engineer", "etl", "дата инженер", "инженер данных", "big data"],
        "data_analyst": ["data analyst", "аналитик данных", "bi analyst", "bi developer", "бизнес-аналитик"],
        "backend_developer": ["backend", "бэкенд", "python developer", "java developer", "go developer",
                              "разработчик python", "node developer", "php developer", "c# developer"],
        "frontend_developer": ["frontend", "фронтенд", "react developer", "vue developer", "angular"],
        "fullstack_developer": ["full stack", "fullstack", "фулстек"],
        "mobile_developer": ["mobile", "ios developer", "android developer", "flutter", "react native"],
        "devops_engineer": ["devops", "sre", "site reliability", "platform engineer", "infrastructure"],
        "mlops_engineer": ["mlops", "ml platform", "ml infrastructure"],
        "qa_engineer": ["qa", "test", "quality assurance", "тестировщик", "sdet"],
        "security_engineer": ["security", "cybersecurity", "infosec", "безопасност", "пентест"],
        "data_architect": ["data architect", "архитектор данных", "solutions architect"],
        "engineering_manager": ["engineering manager", "tech lead", "team lead", "тимлид", "техлид",
                                "head of", "руководитель", "director of engineering", "vp engineering", "cto"],
        "product_manager": ["product manager", "продакт", "проджект", "scrum master", "product owner"],
        "designer": ["ux designer", "ui designer", "дизайнер", "product designer"],
        "systems_engineer": ["systems engineer", "системный администратор", "network engineer", "сетевой"],
        "nlp_engineer": ["nlp", "natural language", "llm engineer", "conversational ai", "genai"],
        "cv_engineer": ["computer vision", "image processing", "perception engineer"],
        "researcher": ["research scientist", "research engineer", "applied scientist"],
    }

    def _detect_role_family(title):
        if not title:
            return "other"
        t = title.lower()
        for family, keywords in _ROLE_FAMILIES.items():
            if any(kw in t for kw in keywords):
                return family
        return "other"

    df["role_family"] = df["title"].apply(_detect_role_family)

    # ── 19. BOOLEAN FLAGS (super useful for filtering) ──
    df["is_data_role"] = df["role_family"].isin([
        "data_scientist", "data_engineer", "data_analyst", "data_architect",
        "ml_engineer", "mlops_engineer", "nlp_engineer", "cv_engineer", "researcher"])
    df["is_ml_role"] = df["role_family"].isin([
        "data_scientist", "ml_engineer", "mlops_engineer", "nlp_engineer",
        "cv_engineer", "researcher"])
    df["is_python_role"] = df.apply(
        lambda r: "python" in str(r.get("skills_extracted") or "").lower()
                  or "python" in str(r.get("title") or "").lower(), axis=1)
    df["is_analyst_role"] = df["role_family"].isin(["data_analyst"])

    # ── 20. COUNTRY NORMALIZED (standardize country names) ──
    _COUNTRY_MAP = {
        "РОССИЯ": "Russia", "UNITED KINGDOM": "United Kingdom", "UK": "United Kingdom",
        "GERMANY": "Germany", "FRANCE": "France", "NETHERLANDS": "Netherlands",
        "POLAND": "Poland", "CANADA": "Canada", "AUSTRALIA": "Australia",
        "INDIA": "India", "SINGAPORE": "Singapore", "UNITED STATES": "United States",
        "USA": "United States", "US": "United States",
        "КАЗАХСТАН": "Kazakhstan", "БЕЛАРУСЬ": "Belarus", "УКРАИНА": "Ukraine",
        "УЗБЕКИСТАН": "Uzbekistan",
    }
    if "country" in df.columns:
        df["country_normalized"] = df["country"].map(
            lambda c: _COUNTRY_MAP.get(str(c).upper().strip(), str(c).strip()) if pd.notna(c) else None)

    if "_dedupe_key" in df.columns:
        df = df.drop(columns=["_dedupe_key"])

    logger.info(f"Cleaning done: {len(df)} rows (dropped {initial - len(df)})")
    return df


# ============================================================================
# S3 STEP
# ============================================================================

def _merge_dedupe_key(row) -> str:
    source = str(row.get("source") or "")
    source_job_id = str(row.get("source_job_id") or "").strip()
    url = str(row.get("url") or "").strip().lower()
    job_id = str(row.get("job_id") or "").strip()
    title = str(row.get("title") or "").strip().lower()
    company = str(row.get("company_name") or row.get("company") or "").strip().lower()
    if source and source_job_id:
        return f"srcid::{source}::{source_job_id}"
    if source and url:
        return f"url::{source}::{url}"
    if job_id:
        return f"job::{job_id}"
    return f"title::{source}::{company}::{title}"


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
        f"Merged cleaned snapshot with latest without re-cleaning: old={len(old_latest)}, new={len(cleaned_new)}, final={len(combined)}"
    )
    return combined


def run_clean_step(date_str: str = None, raw_s3_keys: list[str] | None = None) -> str:
    """
    AIRFLOW TASK 2:
    download only current run raw snapshots -> clean -> merge with latest history
    -> upload dated clean snapshot + latest pointer.
    """
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
        logger.info(
            "Using raw_s3_keys from parse task: %s file(s): %s",
            len(raw_keys),
            raw_keys,
        )
    else:
        prefix = f"raw/{date_str}/"
        raw_keys = [k for k in list_keys(prefix) if k.endswith(".csv")]
        logger.warning(
            "raw_s3_keys not provided; falling back to all raw keys under prefix %s (%s file(s))",
            prefix,
            len(raw_keys),
        )

    if not raw_keys:
        logger.warning("No raw files to clean for %s", date_str)
        return ""

    dfs = []
    for key in raw_keys:
        try:
            df = download_df(key)
            if not df.empty:
                dfs.append(df)
                logger.info("Downloaded raw file %s with %s rows", key, len(df))
            else:
                logger.warning("Raw file %s is empty", key)
        except Exception as e:
            logger.error("Failed to download raw file %s: %s", key, e)

    if not dfs:
        logger.warning("All selected raw files failed to download or were empty")
        return ""

    merged = pd.concat(dfs, ignore_index=True)
    logger.info("Merged current raw snapshots: %s rows from %s files", len(merged), len(dfs))

    cleaned_new = clean_dataframe(merged)

    latest_key = latest_clean_key()
    if key_exists(latest_key):
        try:
            old_latest = download_df(latest_key)
            final_df = _merge_cleaned_with_latest(old_latest, cleaned_new)
        except Exception as e:
            logger.warning("Failed to load previous latest clean dataset: %s", e)
            final_df = cleaned_new
    else:
        final_df = cleaned_new

    snapshot_key = clean_key(date_str, run_id=run_id)
    upload_df(final_df, snapshot_key)
    upload_df(final_df, latest_key)

    logger.info("Clean snapshot written: %s; latest updated: %s", snapshot_key, latest_key)
    return snapshot_key


if __name__ == "__main__":
    key = run_clean_step()
    print(f"Clean: {key}")