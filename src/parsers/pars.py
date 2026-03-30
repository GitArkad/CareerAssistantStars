"""
pars.py — IT Jobs Parser Pipeline v5.1.
Keeps append-only raw snapshots, preserves Adzuna country metadata,
and makes record normalization safer for incremental loads.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import warnings
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.parsers.ats_companies import get_active_companies, mark_inactive, reset_fail_count
from src.loaders.s3_storage import ensure_bucket, make_run_id, raw_key, upload_df

import pandas as pd
import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

from src.parsers.search_queries import get_queries_for_source

MAX_TOTAL_PER_SOURCE = int(os.getenv("MAX_TOTAL_PER_SOURCE", "20000"))
TARGET_PER_QUERY = int(os.getenv("TARGET_PER_QUERY", "120"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
KEEP_RAW_JSON = os.getenv("KEEP_RAW_JSON", "0").strip().lower() in {"1", "true", "yes", "y"}
HH_DETAIL_FETCH_LIMIT = int(os.getenv("HH_DETAIL_FETCH_LIMIT", "0"))
USAJOBS_TARGET_PER_QUERY = int(os.getenv("USAJOBS_TARGET_PER_QUERY", "600"))
GREENHOUSE_MAX_COMPANIES = int(os.getenv("GREENHOUSE_MAX_COMPANIES", "0"))
LEVER_MAX_COMPANIES = int(os.getenv("LEVER_MAX_COMPANIES", "0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.FileHandler("parser.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("pars")

TECH_KW = {
    "python", "java", "javascript", "typescript", "scala", "go", "golang", "rust", "ruby", "php", "c++", "c#",
    "swift", "kotlin", "r", "sql", "postgresql", "mysql", "mongodb", "redis", "elasticsearch", "cassandra", "dynamodb",
    "neo4j", "clickhouse", "docker", "kubernetes", "k8s", "terraform", "ansible", "jenkins", "gitlab", "github",
    "aws", "azure", "gcp", "cloud", "linux", "bash", "nginx", "spark", "kafka", "airflow", "dbt", "snowflake",
    "bigquery", "redshift", "databricks", "flink", "hive", "presto", "pytorch", "tensorflow", "sklearn", "pandas",
    "numpy", "scipy", "keras", "jax", "huggingface", "langchain", "llamaindex", "mlflow", "wandb", "kubeflow",
    "sagemaker", "vertex", "fastapi", "flask", "django", "react", "vue", "angular", "node.js", "express", "spring",
    "rails", "git", "jira", "confluence", "figma", "postman", "power bi", "tableau", "looker", "metabase",
    "grafana", "prometheus", "datadog", "splunk", "elastic", "excel", "notion", "slack",
}
TOOLS_KW = {
    "git", "jira", "confluence", "docker", "kubernetes", "jenkins", "gitlab", "github", "slack", "notion",
    "figma", "postman", "terraform", "ansible", "datadog", "grafana", "prometheus", "tableau", "power bi", "looker",
}
METH_KW = {"agile", "scrum", "kanban", "devops", "mlops", "dataops", "ci/cd", "cicd", "microservices", "tdd", "bdd"}
SEN_MAP = {
    "intern": "intern", "trainee": "intern", "стажер": "intern", "junior": "junior", "jr": "junior",
    "entry": "junior", "начального": "junior", "младший": "junior", "без опыта": "junior", "middle": "middle",
    "mid": "middle", "мидл": "middle", "среднего": "middle", "regular": "middle", "senior": "senior", "sr": "senior",
    "lead": "lead", "старший": "senior", "ведущий": "lead", "главный": "lead", "principal": "principal",
    "staff": "principal", "manager": "manager", "director": "director", "руководитель": "manager", "директор": "director",
}

SPECIAL_TOKEN_PATTERNS = {
    "c++": r"(?<!\w)c\+\+(?!\w)",
    "c#": r"(?<!\w)c#(?!\w)",
    "node.js": r"(?<!\w)node\.js(?!\w)",
    "ci/cd": r"(?<!\w)ci\s*/\s*cd(?!\w)",
    "power bi": r"(?<!\w)power\s*bi(?!\w)",
}

ADZUNA_C = [
    ("gb", "United Kingdom", "GBP"),
    ("de", "Germany", "EUR"),
    ("fr", "France", "EUR"),
    ("nl", "Netherlands", "EUR"),
    ("pl", "Poland", "PLN"),
    ("ca", "Canada", "CAD"),
    ("au", "Australia", "AUD"),
    ("in", "India", "INR"),
    ("sg", "Singapore", "SGD"),
]

RAW_COLUMNS = [
    "job_id",
    "source_job_id",
    "title",
    "description",
    "company_name",
    "department",
    "salary_from",
    "salary_to",
    "currency",
    "salary_period",
    "location",
    "country",
    "remote",
    "remote_type",
    "employment_type",
    "published_at",
    "source",
    "url",
    "search_query",
    "parsed_at",
    "requirements",
    "responsibilities",
    "nice_to_have",
    "experience_level",
    "seniority_normalized",
    "years_experience_min",
    "years_experience_max",
    "key_skills",
    "skills_extracted",
    "skills_normalized",
    "tech_stack_tags",
    "tools",
    "methodologies",
    "visa_sponsorship",
    "relocation",
    "benefits",
    "industry",
    "company_size",
    "education",
    "certifications",
    "spoken_languages",
    "equity_bonus",
    "security_clearance",
]

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jid(src: str, source_job_id: Optional[str], url: Optional[str], title: Optional[str], company_name: Optional[str] = None) -> str:
    stable = source_job_id or url or f"{title or ''}|{company_name or ''}"
    return hashlib.sha256(f"{src}:{stable}".encode("utf-8")).hexdigest()[:32]


def _html(text: Optional[str]) -> str:
    if not text:
        return ""

    s = str(text).strip()
    if not s:
        return ""

    # Если это обычный текст без HTML-тегов — не гоняем через BeautifulSoup
    if "<" not in s and ">" not in s:
        return re.sub(r"\s+", " ", s).strip()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", MarkupResemblesLocatorWarning)
        soup = BeautifulSoup(s, "html.parser")

    for x in soup(["script", "style"]):
        x.decompose()

    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def _match_token(text: str, token: str) -> bool:
    if token in SPECIAL_TOKEN_PATTERNS:
        return re.search(SPECIAL_TOKEN_PATTERNS[token], text) is not None
    return re.search(rf"(?<!\w){re.escape(token)}(?!\w)", text) is not None


def _yrs(text: Optional[str]):
    if not text:
        return None, None

    low = text.lower()

    positive_patterns = [
        r"(\d{1,2})\s*[-–—]\s*(\d{1,2})\s*(?:years?\s+of\s+experience|yrs?\s+of\s+experience|лет\s+опыта|года\s+опыта|год\s+опыта)",
        r"(?:от|from|at\s+least|minimum\s+of|min\.?|over|more\s+than)\s*(\d{1,2})\+?\s*(?:years?\s+of\s+experience|years?|yrs?|лет\s+опыта|лет)",
        r"(\d{1,2})\+\s*(?:years?\s+of\s+experience|years?|yrs?|лет\s+опыта|лет)",
        r"(\d{1,2})\s*(?:years?\s+of\s+experience|years?\s+experience|yrs?\s+experience|лет\s+опыта|года\s+опыта|год\s+опыта)",
    ]

    negative_context = [
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
    ]

    strong_positive_hints = [
        r"\bexperience\b",
        r"\brequirements?\b",
        r"\brequired\b",
        r"\bmust\s+have\b",
        r"\bminimum\b",
        r"\bat\s+least\b",
        r"\bcommercial\b",
        r"\bhands[- ]on\b",
    ]

    for pat in positive_patterns:
        m = re.search(pat, low, flags=re.I | re.S)
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
        if y1 > 25 or y2 > 25:
            continue

        start, end = m.span()
        window = low[max(0, start - 120): min(len(low), end + 120)]
        negative = any(re.search(p, window, flags=re.I) for p in negative_context)
        positive = any(re.search(p, window, flags=re.I) for p in strong_positive_hints)

        if negative and not positive:
            continue

        return (y1, y2)

    return None, None


def _sen(text: Optional[str]) -> str:
    if not text:
        return "unknown"
    low = text.lower()
    for k, v in SEN_MAP.items():
        if k in low:
            return v
    return "unknown"


def _extract_set(text: Optional[str], keywords: set[str]) -> List[str]:
    if not text:
        return []
    low = text.lower()
    return sorted({k for k in keywords if _match_token(low, k)})


def _jsonable(value: Any) -> Any:
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    return str(value)

def _normalize_text(value: Optional[str]) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9+#./\s-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _query_tokens(query: str) -> list[str]:
    q = _normalize_text(query)
    if not q:
        return []

    stop = {
        "and", "or", "of", "to", "for", "with", "in", "on", "at", "the", "a", "an",
        "remote", "hybrid", "office", "engineer", "developer", "specialist",
    }

    return [t for t in q.split() if len(t) >= 2 and t not in stop]


def _matches_query(text: str, query: str) -> bool:
    hay = _normalize_text(text)
    q = _normalize_text(query)

    if not hay or not q:
        return False

    if q in hay:
        return True

    tokens = _query_tokens(query)
    if tokens and all(tok in hay for tok in tokens):
        return True

    if len(tokens) >= 2:
        matched = sum(1 for tok in tokens if tok in hay)
        if matched / len(tokens) >= 0.66:
            return True

    return False


def _first_matching_query(title: Optional[str], description: Optional[str], queries: list[str]) -> Optional[str]:
    haystack = f"{title or ''} {description or ''}"
    for q in queries:
        if _matches_query(haystack, q):
            return q
    return None

class BaseParser(ABC):
    def __init__(self, src: str):
        self.source_name = src
        self.headers = {"User-Agent": "JobPipeline/5.1", "Accept": "application/json"}
        self.collected_ids: set[str] = set()
        self.vacancies: list[dict] = []

    def _sd(self, value):
        return value if isinstance(value, dict) else {}

    def _sl(self, value):
        return value if isinstance(value, list) else []

    def _validate_record_schema(self, rec: dict) -> Optional[dict]:
        if not isinstance(rec, dict):
            return None

        fixed = {col: rec.get(col) for col in RAW_COLUMNS}

        # обязательные поля
        if not fixed.get("title"):
            return None

        fixed["source"] = self.source_name

        # защита от мусора/сдвигов
        for col, val in fixed.items():
            if isinstance(val, (dict, list)) and col not in {"key_skills", "skills_extracted", "skills_normalized", "tech_stack_tags", "tools", "methodologies", "spoken_languages"}:
                fixed[col] = json.dumps(val, ensure_ascii=False, default=str)

        # примитивная sanity-check логика
        if fixed.get("source") not in {
            "hh.ru", "greenhouse.com", "lever.co", "ashbyhq.com",
            "adzuna.com", "usajobs.gov", "arbeitnow.com", "himalayas.app"
        }:
            return None

        return fixed


    def _rec(self, **kw):
        desc = kw.get("description") or ""
        req = kw.get("requirements") or ""
        full = f"{kw.get('title') or ''} {desc} {req}"
        remote = bool(kw.get("remote", False))
        remote_type = kw.get("remote_type") or ("remote" if remote else "office")
        source_job_id = kw.get("source_job_id")
        url = kw.get("url")
        title = kw.get("title")
        company_name = kw.get("company_name")
        salary_period = kw.get("salary_period")
        if salary_period in {"", "nan", "None", None}:
            salary_period = None
        key_skills = kw.get("key_skills") or []
        spoken_languages = kw.get("spoken_languages") or []
        raw_json = _jsonable(kw.get("raw_json"))

        return {
            "job_id": kw.get("job_id") or _jid(self.source_name, source_job_id, url, title, company_name),
            "source_job_id": str(source_job_id) if source_job_id not in {None, ""} else None,
            "title": title,
            "description": _html(desc),
            "company_name": company_name,
            "department": kw.get("department"),
            "salary_from": kw.get("salary_from"),
            "salary_to": kw.get("salary_to"),
            "currency": kw.get("currency"),
            "salary_period": salary_period,
            "location": kw.get("location"),
            "country": kw.get("country"),
            "remote": remote,
            "remote_type": remote_type,
            "employment_type": kw.get("employment_type"),
            "published_at": kw.get("published_at"),
            "source": self.source_name,
            "url": url,
            "search_query": kw.get("search_query"),
            "parsed_at": _utcnow_iso(),
            "requirements": _html(req),
            "responsibilities": _html(kw.get("responsibilities") or ""),
            "nice_to_have": _html(kw.get("nice_to_have") or ""),
            "experience_level": kw.get("experience_level"),
            "seniority_normalized": (lambda s: s if s != "unknown" else _sen(title))(_sen(kw.get("experience_level") or "")),
            "years_experience_min": kw.get("years_min"),
            "years_experience_max": kw.get("years_max"),
            "key_skills": key_skills,
            "skills_extracted": [],
            "skills_normalized": [],
            "tech_stack_tags": [],
            "tools": [],
            "methodologies": [],
            "visa_sponsorship": kw.get("visa_sponsorship"),
            "relocation": kw.get("relocation"),
            "benefits": kw.get("benefits"),
            "industry": kw.get("industry"),
            "company_size": kw.get("company_size"),
            "education": kw.get("education"),
            "certifications": kw.get("certifications"),
            "spoken_languages": spoken_languages,
            "equity_bonus": kw.get("equity_bonus"),
            "security_clearance": kw.get("security_clearance"),
            **({"raw_json": raw_json} if KEEP_RAW_JSON else {}),
        }

    def _add(self, record: dict):
        record = self._validate_record_schema(record)
        if not record:
            return

        job_id = record["job_id"]
        if job_id not in self.collected_ids:
            self.vacancies.append(record)
            self.collected_ids.add(job_id)

    def to_df(self) -> pd.DataFrame:
        if not self.vacancies:
            return pd.DataFrame(columns=RAW_COLUMNS)

        df = pd.DataFrame(self.vacancies)

        for col in RAW_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[RAW_COLUMNS]

        list_cols = [
            "key_skills", "skills_extracted", "skills_normalized",
            "tech_stack_tags", "tools", "methodologies", "spoken_languages"
        ]
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x
                )

        return df

class QueryParserBase(BaseParser, ABC):
    @abstractmethod
    def fetch(self, keyword: str, target: int, **kwargs) -> List[Dict]:
        raise NotImplementedError

    def run(self, keywords=None, target=TARGET_PER_QUERY):
        keywords = keywords or get_queries_for_source(self.source_name)
        logger.info("[%s] %s queries", self.source_name, len(keywords))
        for keyword in keywords:
            if len(self.vacancies) >= MAX_TOTAL_PER_SOURCE:
                break
            remaining = max(0, min(target, MAX_TOTAL_PER_SOURCE - len(self.vacancies)))
            if remaining == 0:
                break
            try:
                for record in self.fetch(keyword, remaining):
                    self._add(record)
            except Exception as e:
                logger.error("[%s] '%s': %s", self.source_name, keyword, e)
            time.sleep(0.25)
        logger.info("[%s] Done: %s", self.source_name, len(self.vacancies))


class CatalogParserBase(BaseParser, ABC):
    def __init__(self, src: str):
        super().__init__(src)
        self._catalog_cache = None
        self.catalog_load_retries = int(os.getenv("CATALOG_LOAD_RETRIES", "1"))
        self.catalog_retry_sleep = float(os.getenv("CATALOG_RETRY_SLEEP", "2"))

    @abstractmethod
    def load_catalog_once(self) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def raw_to_record(self, raw_job: dict, matched_query: Optional[str]) -> Optional[dict]:
        raise NotImplementedError

    def _load_catalog_with_retries(self) -> list[dict]:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.catalog_load_retries + 2):
            try:
                return self.load_catalog_once() or []
            except Exception as e:
                last_error = e
                logger.warning(
                    "[%s] load_catalog_once failed attempt=%s/%s: %s",
                    self.source_name,
                    attempt,
                    self.catalog_load_retries + 1,
                    e,
                )
                if attempt <= self.catalog_load_retries:
                    time.sleep(self.catalog_retry_sleep * attempt)

        if last_error is not None:
            raise last_error
        return []

    def run(self, keywords=None, target=MAX_TOTAL_PER_SOURCE):
        keywords = keywords or get_queries_for_source(self.source_name)
        logger.info("[%s] catalog-mode, %s queries", self.source_name, len(keywords))

        catalog = self._load_catalog_with_retries()
        if not catalog:
            logger.info("[%s] empty catalog", self.source_name)
            return

        for raw_job in catalog:
            if len(self.vacancies) >= target:
                break

            title = raw_job.get("title") or raw_job.get("text") or ""
            description = (
                raw_job.get("description")
                or raw_job.get("content")
                or raw_job.get("descriptionPlain")
                or ""
            )

            matched_query = _first_matching_query(title, description, keywords)
            if not matched_query:
                continue

            try:
                record = self.raw_to_record(raw_job, matched_query)
                if record:
                    self._add(record)
            except Exception as e:
                logger.warning("[%s] raw_to_record failed: %s", self.source_name, e)

        logger.info("[%s] Done: %s", self.source_name, len(self.vacancies))

class HHParser(QueryParserBase):
    def __init__(self):
        super().__init__("hh.ru")
        self._hh_ids = set()
        self.detail_fetch_limit = max(0, HH_DETAIL_FETCH_LIMIT)
        self._detail_fetch_count = 0

    def _det(self, vacancy_id):
        if not vacancy_id or vacancy_id in self._hh_ids:
            return None
        if self.detail_fetch_limit and self._detail_fetch_count >= self.detail_fetch_limit:
            return None
        for attempt in range(3):
            try:
                r = requests.get(
                    f"https://api.hh.ru/vacancies/{vacancy_id}",
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT,
                )
                if r.status_code == 429:
                    time.sleep(4 * (attempt + 1))
                    continue
                r.raise_for_status()
                self._hh_ids.add(vacancy_id)
                self._detail_fetch_count += 1
                return r.json()
            except Exception:
                time.sleep(2)
        return None

    def _p(self, v, keyword, country):
        sal = self._sd(v.get("salary"))
        area = self._sd(v.get("area"))
        emp = self._sd(v.get("employer"))
        exp = self._sd(v.get("experience"))
        sched = self._sd(v.get("schedule"))
        snippet = self._sd(v.get("snippet"))
        en = exp.get("name", "")
        y1, y2 = _yrs(en)
        rem = self._sd(v.get("work_mode")).get("id") == "REMOTE" or "удален" in (sched.get("name") or "").lower()
        skills = [s.get("name") for s in self._sl(v.get("key_skills")) if s.get("name")]
        dept = self._sd(v.get("department")).get("name")
        description = v.get("description") or " ".join(
            filter(None, [snippet.get("requirement"), snippet.get("responsibility")])
        )
        requirements_text = "; ".join(skills) if skills else (snippet.get("requirement") or "")
        responsibilities_text = snippet.get("responsibility") or ""
        desc_text = str(description or "").lower()
        visa = True if any(w in desc_text for w in ["visa", "виза", "визовая"]) else None
        relocation = True if any(w in desc_text for w in ["relocation", "релокац", "переезд"]) else False
        employment = self._sd(v.get("employment")).get("name") or sched.get("name")
        return self._rec(
            source_job_id=str(v.get("id") or ""),
            title=v.get("name"),
            description=description,
            requirements=requirements_text,
            responsibilities=responsibilities_text,
            company_name=emp.get("name"),
            department=dept,
            salary_from=sal.get("from"),
            salary_to=sal.get("to"),
            currency=sal.get("currency"),
            salary_period=None,
            experience_level=en,
            years_min=y1,
            years_max=y2,
            key_skills=skills,
            location=area.get("name"),
            country=country,
            remote=rem,
            remote_type="remote" if rem else "office",
            employment_type=employment,
            published_at=v.get("published_at"),
            url=v.get("alternate_url") or f"https://hh.ru/vacancy/{v.get('id')}",
            visa_sponsorship=visa,
            relocation=relocation,
            search_query=keyword,
            raw_json=v,
        )

    def fetch(self, keyword: str, target: int) -> list[dict]:
        areas = [113, 1, 2, 3, 4, 1001, 160]
        res: list[dict] = []
        page = 0

        while len(res) < target:
            params = {
                "text": keyword,
                "area": areas,
                "per_page": 100,
                "page": page,
                "search_field": "name",
                "only_with_salary": False,
            }

            r = requests.get(
                "https://api.hh.ru/vacancies",
                params=params,
                headers=self.headers,
                timeout=REQUEST_TIMEOUT,
            )

            if r.status_code == 403:
                logger.warning("[hh.ru] HTTP 403. Likely temporary blocking or anti-bot response.")
                break

            r.raise_for_status()
            data = r.json()
            items = data.get("items", [])
            if not items:
                break

            for j in items:
                if len(res) >= target:
                    break

                sal = j.get("salary") or {}
                req = self._sd(j.get("snippet")).get("requirement")
                resp = self._sd(j.get("snippet")).get("responsibility")
                desc = " ".join(x for x in [req, resp] if x)
                y1, y2 = _yrs(desc)
                addr = self._sd(j.get("address"))
                area = self._sd(j.get("area"))

                res.append(
                    self._rec(
                        source_job_id=str(j.get("id") or ""),
                        title=j.get("name"),
                        description=desc,
                        company_name=self._sd(j.get("employer")).get("name"),
                        salary_from=sal.get("from"),
                        salary_to=sal.get("to"),
                        currency=sal.get("currency"),
                        location=addr.get("city") or area.get("name"),
                        country="Russia" if area.get("id") == "113" else None,
                        remote=("remote" in (j.get("schedule", {}) or {}).get("id", "")),
                        remote_type=(j.get("schedule", {}) or {}).get("id"),
                        published_at=j.get("published_at"),
                        url=j.get("alternate_url"),
                        search_query=keyword,
                        years_min=y1,
                        years_max=y2,
                        raw_json=j,
                    )
                )

            pages = data.get("pages", 0)
            page += 1
            if page >= pages:
                break

            time.sleep(0.25)

        return res

class AdzunaParser(QueryParserBase):
    def __init__(self, app_id, app_key):
        super().__init__("adzuna.com")
        self.app_id = app_id
        self.app_key = app_key
        self.max_rate_limit_retries = int(os.getenv("ADZUNA_MAX_RATE_RETRIES", "6"))
        self.base_sleep_seconds = int(os.getenv("ADZUNA_RATE_SLEEP_SECONDS", "10"))
        self.auth_failed = False

    def _is_rate_limited(self, response: requests.Response) -> bool:
        if response.status_code == 429:
            return True
        try:
            payload = response.json()
            text = json.dumps(payload, ensure_ascii=False).lower()
        except Exception:
            text = (response.text or "").lower()
        return any(
            x in text
            for x in [
                "rate limit",
                "too many requests",
                "quota",
                "exceeded",
                "limit reached",
            ]
        )

    def fetch(self, keyword: str, target: int) -> list[dict]:
        res: list[dict] = []

        for country_code, country_name, default_currency in ADZUNA_C:
            if len(res) >= target or self.auth_failed:
                break

            page = 1
            rate_limit_hits = 0

            while len(res) < target:
                try:
                    r = requests.get(
                        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/{page}",
                        params={
                            "app_id": self.app_id,
                            "app_key": self.app_key,
                            "results_per_page": 50,
                            "what": keyword,
                            "content-type": "application/json",
                        },
                        headers=self.headers,
                        timeout=REQUEST_TIMEOUT,
                    )
                except requests.RequestException as e:
                    logger.warning(
                        "[adzuna.com] %s %s page=%s request failed: %s",
                        country_code,
                        keyword,
                        page,
                        e,
                    )
                    break

                if r.status_code == 401:
                    self.auth_failed = True
                    logger.error(
                        "[adzuna.com] HTTP 401 Unauthorized. Stop source immediately. "
                        "Check ADZUNA_APP_ID / ADZUNA_APP_KEY."
                    )
                    return res

                if self._is_rate_limited(r):
                    rate_limit_hits += 1
                    sleep_for = self.base_sleep_seconds * min(rate_limit_hits, 6)
                    logger.warning(
                        "[adzuna.com] rate limit: country=%s query=%r page=%s retry=%s/%s sleep=%ss",
                        country_code,
                        keyword,
                        page,
                        rate_limit_hits,
                        self.max_rate_limit_retries,
                        sleep_for,
                    )
                    if rate_limit_hits >= self.max_rate_limit_retries:
                        logger.warning(
                            "[adzuna.com] skip country due to repeated rate limits: country=%s query=%r",
                            country_code,
                            keyword,
                        )
                        break
                    time.sleep(sleep_for)
                    continue

                if r.status_code != 200:
                    logger.warning(
                        "[adzuna.com] HTTP %s: country=%s query=%r page=%s",
                        r.status_code,
                        country_code,
                        keyword,
                        page,
                    )
                    break

                rate_limit_hits = 0

                try:
                    jobs = r.json().get("results", [])
                except Exception as e:
                    logger.warning(
                        "[adzuna.com] bad json: country=%s query=%r page=%s err=%s",
                        country_code,
                        keyword,
                        page,
                        e,
                    )
                    break

                if not jobs:
                    break

                for j in jobs:
                    if len(res) >= target:
                        break

                    loc = self._sd(j.get("location"))
                    desc = j.get("description") or ""
                    y1, y2 = _yrs(desc)
                    remote = "remote" in desc.lower()

                    res.append(
                        self._rec(
                            source_job_id=str(j.get("id") or ""),
                            title=j.get("title"),
                            description=desc,
                            company_name=self._sd(j.get("company")).get("display_name"),
                            salary_from=j.get("salary_min"),
                            salary_to=j.get("salary_max"),
                            currency=j.get("salary_currency") or default_currency,
                            location=loc.get("display_name"),
                            country=country_name,
                            remote=remote,
                            remote_type="remote" if remote else "office",
                            published_at=j.get("created"),
                            url=j.get("redirect_url"),
                            search_query=keyword,
                            years_min=y1,
                            years_max=y2,
                            raw_json=j,
                        )
                    )

                page += 1
                time.sleep(0.5)

        return res

class USAJobsParser(QueryParserBase):
    def __init__(self, api_key, email):
        super().__init__("usajobs.gov")
        self.headers.update({
            "Host": "data.usajobs.gov",
            "User-Agent": email,
            "Authorization-Key": api_key,
        })

    def run(self, keywords=None, target=USAJOBS_TARGET_PER_QUERY):
        super().run(keywords=keywords, target=target)

    def fetch(self, keyword, target, **kwargs):
        res = []
        page = 1
        seen_ids = set()

        while len(res) < target:
            r = requests.get(
                "https://data.usajobs.gov/api/search",
                params={
                    "Keyword": keyword,
                    "ResultsPerPage": 250,
                    "Page": page,
                },
                headers=self.headers,
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code != 200:
                logger.warning("[usajobs.gov] HTTP %s for keyword=%r page=%s", r.status_code, keyword, page)
                break

            items = r.json().get("SearchResult", {}).get("SearchResultItems", [])
            if not items:
                break

            new_count = 0
            for item in items:
                if len(res) >= target:
                    break
                try:
                    j = self._sd(item.get("MatchedObjectDescriptor"))
                    source_job_id = str(j.get("PositionID") or "")
                    if source_job_id and source_job_id in seen_ids:
                        continue
                    if source_job_id:
                        seen_ids.add(source_job_id)

                    locs = j.get("PositionLocationDisplay", []) or []
                    loc = "; ".join(locs) if isinstance(locs, list) else locs
                    det = self._sd(self._sd(j.get("UserArea")).get("Details"))
                    desc = det.get("JobSummary") or det.get("MajorDuties") or ""
                    rems = self._sl(j.get("PositionRemuneration"))
                    first_pay = self._sd(rems[0]) if rems else {}

                    res.append(self._rec(
                        source_job_id=source_job_id,
                        title=j.get("PositionTitle"),
                        description=desc,
                        company_name=j.get("OrganizationName"),
                        salary_from=first_pay.get("MinimumRange"),
                        salary_to=first_pay.get("MaximumRange"),
                        currency="USD",
                        location=loc,
                        country="United States",
                        employment_type=self._sd(j.get("PositionSchedule")).get("Name"),
                        remote=any(w in desc.lower() for w in ["remote", "telework", "work from home"]),
                        published_at=j.get("PublicationStartDate"),
                        url=j.get("PositionURI"),
                        search_query=keyword,
                        raw_json=j,
                    ))
                    new_count += 1
                except Exception as e:
                    logger.warning("USAJobs item parse failed: %s", e)

            logger.info(
                "[usajobs.gov] keyword=%r page=%s fetched=%s new=%s total=%s",
                keyword, page, len(items), new_count, len(res)
            )

            if new_count == 0:
                break

            page += 1
            time.sleep(0.5)

        return res


class HimalayasParser(CatalogParserBase):
    def __init__(self):
        super().__init__("himalayas.app")
        self.page_size = int(os.getenv("HIMALAYAS_PAGE_SIZE", "100"))
        self.max_retries = int(os.getenv("HIMALAYAS_MAX_RETRIES", "1"))
        self.retry_sleep = float(os.getenv("HIMALAYAS_RETRY_SLEEP", "1"))
        self.page_sleep = float(os.getenv("HIMALAYAS_PAGE_SLEEP", "0.1"))
        self.max_pages = int(os.getenv("HIMALAYAS_MAX_PAGES", "300"))
        self.max_jobs = int(os.getenv("HIMALAYAS_MAX_JOBS", "15000"))
        self.request_timeout = int(os.getenv("HIMALAYAS_REQUEST_TIMEOUT", "8"))

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[himalayas.app] loading catalog once for this run")
        jobs_all = []
        offset = 0
        page_num = 0

        while page_num < self.max_pages and len(jobs_all) < self.max_jobs:
            success = False
            jobs = []

            for attempt in range(1, self.max_retries + 2):
                try:
                    r = requests.get(
                        "https://himalayas.app/jobs/api",
                        params={"offset": offset, "limit": self.page_size},
                        headers=self.headers,
                        timeout=self.request_timeout,
                    )

                    if r.status_code != 200:
                        logger.warning(
                            "[himalayas.app] HTTP %s at offset=%s page=%s",
                            r.status_code, offset, page_num + 1
                        )
                        self._catalog_cache = jobs_all
                        return self._catalog_cache

                    data = r.json()
                    jobs = data if isinstance(data, list) else data.get("jobs", [])
                    success = True
                    break

                except requests.exceptions.Timeout:
                    logger.warning(
                        "[himalayas.app] timeout at offset=%s page=%s retry=%s/%s",
                        offset, page_num + 1, attempt, self.max_retries + 1
                    )
                    if attempt <= self.max_retries:
                        time.sleep(self.retry_sleep)
                    else:
                        logger.warning(
                            "[himalayas.app] giving up at offset=%s, keep already fetched jobs=%s",
                            offset, len(jobs_all)
                        )
                        self._catalog_cache = jobs_all
                        return self._catalog_cache

                except requests.RequestException as e:
                    logger.warning(
                        "[himalayas.app] request failed at offset=%s page=%s err=%s",
                        offset, page_num + 1, e
                    )
                    self._catalog_cache = jobs_all
                    return self._catalog_cache

                except ValueError as e:
                    logger.warning(
                        "[himalayas.app] bad json at offset=%s page=%s err=%s",
                        offset, page_num + 1, e
                    )
                    self._catalog_cache = jobs_all
                    return self._catalog_cache

            if not success:
                self._catalog_cache = jobs_all
                return self._catalog_cache

            if not jobs:
                logger.info("[himalayas.app] no more jobs at page=%s offset=%s", page_num + 1, offset)
                break

            jobs_all.extend(jobs)

            logger.info(
                "[himalayas.app] page=%s fetched=%s total=%s",
                page_num + 1, len(jobs), len(jobs_all)
            )

            if len(jobs_all) >= self.max_jobs:
                jobs_all = jobs_all[: self.max_jobs]
                logger.info("[himalayas.app] reached max_jobs=%s", self.max_jobs)
                break

            if len(jobs) < self.page_size:
                logger.info(
                    "[himalayas.app] last partial page=%s fetched=%s < page_size=%s",
                    page_num + 1, len(jobs), self.page_size
                )
                break

            offset += self.page_size
            page_num += 1
            time.sleep(self.page_sleep)

        logger.info(
            "[himalayas.app] catalog loaded: %s jobs, pages=%s",
            len(jobs_all), page_num + 1
        )
        self._catalog_cache = jobs_all
        return self._catalog_cache

    def raw_to_record(self, j: dict, matched_query: Optional[str]) -> Optional[dict]:
        title = j.get("title", "")
        slug = (j.get("slug") or "").strip()
        source_job_id = str(j.get("id") or slug or "").strip()

        raw_url = (j.get("url") or "").strip()

        # Если API отдал нормальный URL вакансии — используем его.
        # Если отдал только домен сайта или пусто — строим стабильный surrogate URL.
        if raw_url and raw_url not in {"https://himalayas.app", "http://himalayas.app"}:
            final_url = raw_url
        elif slug:
            final_url = f"https://himalayas.app/jobs/{slug}"
        elif source_job_id:
            final_url = f"https://himalayas.app/jobs/id/{source_job_id}"
        else:
            # самый крайний случай: делаем стабильный URL из title+company
            safe_title = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")
            safe_company = re.sub(r"[^a-z0-9]+", "-", (j.get("companyName") or "").lower()).strip("-")
            final_url = f"https://himalayas.app/jobs/generated/{safe_company}-{safe_title}"[:500]

        return self._rec(
            source_job_id=source_job_id,
            title=title,
            description=j.get("description"),
            company_name=j.get("companyName"),
            salary_from=j.get("minSalary"),
            salary_to=j.get("maxSalary"),
            currency=j.get("salaryCurrency", "USD"),
            location=j.get("location"),
            country=j.get("country"),
            remote=True,
            remote_type="remote",
            employment_type=j.get("employmentType"),
            published_at=j.get("pubDate"),
            url=final_url,
            search_query=matched_query,
            raw_json=j,
        )


class ArbeitnowParser(CatalogParserBase):
    def __init__(self):
        super().__init__("arbeitnow.com")
        self.max_pages = int(os.getenv("ARBEITNOW_MAX_PAGES", "200"))
        self.page_sleep = float(os.getenv("ARBEITNOW_PAGE_SLEEP", "0.6"))
        self.max_retries = int(os.getenv("ARBEITNOW_MAX_RETRIES", "2"))
        self.retry_sleep = float(os.getenv("ARBEITNOW_RETRY_SLEEP", "3"))

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[arbeitnow.com] loading catalog once for this run")
        jobs_all = []
        seen_ids = set()
        page = 1

        while page <= self.max_pages:
            success = False
            jobs = []

            for attempt in range(1, self.max_retries + 2):
                try:
                    r = requests.get(
                        "https://www.arbeitnow.com/api/job-board-api",
                        params={"page": page},
                        headers={
                            **self.headers,
                            "Referer": "https://www.arbeitnow.com/",
                            "Accept": "application/json, text/plain, */*",
                        },
                        timeout=REQUEST_TIMEOUT,
                    )

                    if r.status_code == 403:
                        logger.warning(
                            "[arbeitnow.com] HTTP 403 at page=%s retry=%s/%s",
                            page, attempt, self.max_retries + 1
                        )
                        if attempt <= self.max_retries:
                            time.sleep(self.retry_sleep * attempt)
                            continue
                        self._catalog_cache = jobs_all
                        return self._catalog_cache

                    if r.status_code != 200:
                        logger.warning("[arbeitnow.com] HTTP %s at page=%s", r.status_code, page)
                        self._catalog_cache = jobs_all
                        return self._catalog_cache

                    payload = r.json() or {}
                    jobs = payload.get("data", [])
                    success = True
                    break

                except requests.RequestException as e:
                    logger.warning("[arbeitnow.com] request failed at page=%s err=%s", page, e)
                    if attempt <= self.max_retries:
                        time.sleep(self.retry_sleep * attempt)
                        continue
                    self._catalog_cache = jobs_all
                    return self._catalog_cache

                except ValueError as e:
                    logger.warning("[arbeitnow.com] bad json at page=%s err=%s", page, e)
                    self._catalog_cache = jobs_all
                    return self._catalog_cache

            if not success:
                self._catalog_cache = jobs_all
                return self._catalog_cache

            if not jobs:
                logger.info("[arbeitnow.com] no more jobs at page=%s", page)
                break

            new_count = 0
            for j in jobs:
                source_job_id = str(j.get("slug") or j.get("id") or "")
                dedupe_key = source_job_id or json.dumps(j, sort_keys=True, ensure_ascii=False)
                if dedupe_key in seen_ids:
                    continue
                seen_ids.add(dedupe_key)
                jobs_all.append(j)
                new_count += 1

            logger.info(
                "[arbeitnow.com] page=%s fetched=%s new=%s total=%s",
                page, len(jobs), new_count, len(jobs_all)
            )

            if new_count == 0:
                logger.info("[arbeitnow.com] stopping: repeated page at page=%s", page)
                break

            page += 1
            time.sleep(self.page_sleep)

        logger.info("[arbeitnow.com] catalog loaded: %s jobs", len(jobs_all))
        self._catalog_cache = jobs_all
        return self._catalog_cache

    def raw_to_record(self, j: dict, matched_query: Optional[str]) -> Optional[dict]:
        title = (j.get("title") or "").strip()
        desc = (j.get("description") or "").strip()
        tags = j.get("tags") or []
        remote = any("remote" in str(t).lower() for t in tags)

        return self._rec(
            source_job_id=str(j.get("slug") or j.get("id") or ""),
            title=title,
            description=desc,
            company_name=j.get("company_name"),
            salary_from=None,
            salary_to=None,
            currency=None,
            location=j.get("location"),
            country=None,
            remote=remote,
            remote_type="remote" if remote else "office",
            published_at=j.get("created_at"),
            url=j.get("url"),
            search_query=matched_query,
            years_min=None,
            years_max=None,
            raw_json=j,
        )


class GreenhouseParser(CatalogParserBase):
    def __init__(self):
        super().__init__("greenhouse.com")

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[greenhouse.com] loading catalog once for this run")
        jobs_all = []
        companies = list(get_active_companies("greenhouse"))
        if GREENHOUSE_MAX_COMPANIES > 0:
            companies = companies[:GREENHOUSE_MAX_COMPANIES]

        logger.info("[greenhouse.com] active companies: %s", len(companies))

        for idx, company in enumerate(companies, start=1):
            try:
                r = requests.get(
                    f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
                    params={"content": "true"},
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT,
                )

                if r.status_code == 404:
                    mark_inactive("greenhouse", company, "404")
                    continue

                if r.status_code != 200:
                    mark_inactive("greenhouse", company, f"HTTP {r.status_code}")
                    continue

                jobs = r.json().get("jobs", [])
                reset_fail_count("greenhouse", company)

                for j in jobs:
                    j["_company"] = company
                    jobs_all.append(j)

                if idx % 50 == 0:
                    logger.info("[greenhouse.com] companies=%s/%s jobs=%s", idx, len(companies), len(jobs_all))

                time.sleep(0.2)

            except requests.exceptions.Timeout:
                mark_inactive("greenhouse", company, "timeout")
            except Exception as e:
                logger.warning("GH %s: %s", company, e)
                mark_inactive("greenhouse", company, str(e)[:100])

        logger.info("[greenhouse.com] catalog loaded: %s jobs", len(jobs_all))
        self._catalog_cache = jobs_all
        return self._catalog_cache

    def raw_to_record(self, j: dict, matched_query: Optional[str]) -> Optional[dict]:
        loc = self._sd(j.get("location"))
        departments = self._sl(j.get("departments"))
        first_dept = self._sd(departments[0]) if departments else {}

        return self._rec(
            source_job_id=str(j.get("id") or ""),
            title=j.get("title"),
            description=j.get("content"),
            company_name=j.get("_company"),
            department=first_dept.get("name"),
            location=loc.get("name"),
            published_at=j.get("updated_at"),
            url=j.get("absolute_url"),
            search_query=matched_query,
            raw_json=j,
        )


class LeverParser(CatalogParserBase):
    def __init__(self):
        super().__init__("lever.co")

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[lever.co] loading catalog once for this run")
        jobs_all = []
        companies = list(get_active_companies("lever"))
        if LEVER_MAX_COMPANIES > 0:
            companies = companies[:LEVER_MAX_COMPANIES]

        logger.info("[lever.co] active companies: %s", len(companies))

        for idx, company in enumerate(companies, start=1):
            try:
                r = requests.get(
                    f"https://api.lever.co/v0/postings/{company}",
                    params={"mode": "json"},
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT,
                )

                if r.status_code == 404:
                    mark_inactive("lever", company, "404")
                    continue

                if r.status_code != 200:
                    mark_inactive("lever", company, f"HTTP {r.status_code}")
                    continue

                payload = r.json()
                jobs = payload if isinstance(payload, list) else []
                reset_fail_count("lever", company)

                for j in jobs:
                    j["_company"] = company
                    jobs_all.append(j)

                if idx % 50 == 0:
                    logger.info("[lever.co] companies=%s/%s jobs=%s", idx, len(companies), len(jobs_all))

                time.sleep(0.2)

            except requests.exceptions.Timeout:
                mark_inactive("lever", company, "timeout")
            except Exception as e:
                logger.warning("Lever %s: %s", company, e)
                mark_inactive("lever", company, str(e)[:100])

        logger.info("[lever.co] catalog loaded: %s jobs", len(jobs_all))
        self._catalog_cache = jobs_all
        return self._catalog_cache

    def raw_to_record(self, j: dict, matched_query: Optional[str]) -> Optional[dict]:
        cats = self._sd(j.get("categories"))
        desc = j.get("descriptionPlain") or j.get("description") or ""

        dept = (
            cats.get("department")
            or cats.get("team")
            or cats.get("group")
            or None
        )

        return self._rec(
            source_job_id=str(j.get("id") or ""),
            title=j.get("text"),
            description=desc,
            company_name=j.get("_company"),
            department=dept,
            location=cats.get("location"),
            employment_type=cats.get("commitment"),
            published_at=None,
            url=j.get("hostedUrl"),
            search_query=matched_query,
            raw_json=j,
        )

class AshbyParser(QueryParserBase):
    def __init__(self):
        super().__init__("ashbyhq.com")

    def fetch_company_jobs(self, slug: str) -> list[dict]:
        url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"

        payload = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {
                "organizationHostedJobsPageName": slug
            },
            "query": """
            query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
              jobBoard: jobBoardWithTeams(
                organizationHostedJobsPageName: $organizationHostedJobsPageName
              ) {
                teams {
                  id
                  name
                  parentTeamId
                }
                jobs {
                  id
                  title
                  locationName
                  employmentType
                  secondaryLocations {
                    locationName
                  }
                  applyUrl
                  descriptionHtml
                  publishedAt
                  isListed
                }
              }
            }
            """,
        }

        r = requests.post(
            url,
            json=payload,
            headers={**self.headers, "content-type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()

        data = r.json() or {}
        board = ((data.get("data") or {}).get("jobBoard")) or {}
        jobs = board.get("jobs") or []

        result: list[dict] = []

        for j in jobs:
            if not j or not j.get("isListed", True):
                continue

            desc_html = j.get("descriptionHtml") or ""
            try:
                desc = BeautifulSoup(desc_html, "html.parser").get_text("\n", strip=True)
            except Exception:
                desc = desc_html

            result.append(
                {
                    "id": j.get("id"),
                    "title": j.get("title"),
                    "description": desc,
                    "location": j.get("locationName"),
                    "locationName": j.get("locationName"),
                    "employmentType": j.get("employmentType"),
                    "publishedAt": j.get("publishedAt"),
                    "jobUrl": j.get("applyUrl"),
                    "raw_json": j,
                }
            )

        return result

    
    def fetch(self, keyword: str, target: int) -> list[dict]:
        companies_raw = get_active_companies("ashby")
        res: list[dict] = []
        kl = (keyword or "").strip().lower()

        for company in companies_raw:
            if len(res) >= target:
                break

            # Поддерживаем и dict, и str
            if isinstance(company, dict):
                slug = (
                    company.get("company_key")
                    or company.get("slug")
                    or company.get("company_slug")
                    or company.get("name")
                )
                company_name = (
                    company.get("company_name")
                    or company.get("name")
                    or slug
                )
            else:
                slug = str(company).strip()
                company_name = slug

            if not slug:
                logger.warning("[ashby] skip company with empty slug: %r", company)
                continue

            try:
                jobs = self.fetch_company_jobs(slug)
            except Exception as e:
                logger.warning("[ashby] failed company=%s err=%s", slug, e)
                continue

            for j in jobs:
                if len(res) >= target:
                    break

                title = (j.get("title") or "").strip()
                desc = (j.get("description") or "").strip()
                text = f"{title}\n{desc}".lower()

                if kl and kl not in text:
                    continue

                location = j.get("location") or j.get("locationName")
                remote = "remote" in text or "remote" in str(location or "").lower()

                res.append(
                    self._rec(
                        source_job_id=str(j.get("id") or ""),
                        title=title,
                        description=desc,
                        company_name=company_name,
                        salary_from=None,
                        salary_to=None,
                        currency=None,
                        location=location,
                        country=None,
                        remote=remote,
                        remote_type="remote" if remote else "office",
                        employment_type=j.get("employmentType"),
                        published_at=j.get("publishedAt"),
                        url=j.get("jobUrl"),
                        search_query=keyword,
                        years_min=None,
                        years_max=None,
                        raw_json=j.get("raw_json") or j,
                    )
                )

            time.sleep(0.3)

        return res


def build_parsers():
    parsers: list[BaseParser] = [HHParser()]

    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if app_id and app_key:
        parsers.append(AdzunaParser(app_id, app_key))

    usajobs_api_key = os.getenv("USAJOBS_API_KEY")
    usajobs_email = os.getenv("USAJOBS_EMAIL")
    if usajobs_api_key and usajobs_email:
        parsers.append(USAJobsParser(usajobs_api_key, usajobs_email))

    parsers += [
        ArbeitnowParser(),
        AshbyParser(),
        GreenhouseParser(),
        LeverParser(),
        HimalayasParser(),
    ]
    return parsers


def run_parse_step(date_str=None):
    from src.loaders.s3_storage import ensure_bucket, make_run_id, raw_key, upload_df
    date_str = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = make_run_id(date_str)
    ensure_bucket()
    keys: list[str] = []
    for parser in build_parsers():
        try:
            parser.run()
            df = parser.to_df()
            if not df.empty:
                key = raw_key(parser.source_name, date_str=date_str, run_id=run_id)
                upload_df(df, key)
                keys.append(key)
                logger.info("[%s] %s rows -> %s", parser.source_name, len(df), key)
        except Exception as e:
            logger.error("[%s] FAILED: %s", parser.source_name, e)
    return keys


if __name__ == "__main__":
    print(run_parse_step())