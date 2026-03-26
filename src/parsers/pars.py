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
    for pat in [
        r"(\d+)\s*[-–—]\s*(\d+)\s*(?:лет|years?|год)",
        r"(?:от|from)\s*(\d+)\s*(?:лет|years?)",
        r"(\d+)\+\s*(?:лет|years?)",
        r"(\d+)\s*(?:лет|years?|год)",
    ]:
        m = re.search(pat, low)
        if m:
            g = m.groups()
            return (int(g[0]), int(g[1])) if len(g) == 2 else (int(g[0]), int(g[0]))
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
    return (value or "").strip().lower()


def _matches_query(text: str, query: str) -> bool:
    return _normalize_text(query) in _normalize_text(text)


def _first_matching_query(title: Optional[str], description: Optional[str], queries: list[str]) -> Optional[str]:
    haystack = f"{title or ''} {description or ''}".lower()
    for q in queries:
        if q.lower() in haystack:
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

    def _rec(self, **kw):
        desc = kw.get("description") or ""
        req = kw.get("requirements") or ""
        full = f"{kw.get('title') or ''} {desc} {req}"
        remote = bool(kw.get("remote", False))
        remote_type = kw.get("remote_type") or ("remote" if remote else "onsite")
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
        job_id = record["job_id"]
        if job_id not in self.collected_ids:
            self.vacancies.append(record)
            self.collected_ids.add(job_id)

    def to_df(self) -> pd.DataFrame:
        if not self.vacancies:
            return pd.DataFrame()
        df = pd.DataFrame(self.vacancies)
        list_cols = ["key_skills", "skills_extracted", "skills_normalized", "tech_stack_tags", "tools", "methodologies", "spoken_languages"]
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x)
        if "raw_json" in df.columns:
            df["raw_json"] = df["raw_json"].apply(lambda x: json.dumps(x, ensure_ascii=False, default=str) if isinstance(x, (dict, list)) else x)
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

    @abstractmethod
    def load_catalog_once(self) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def raw_to_record(self, raw_job: dict, matched_query: Optional[str]) -> Optional[dict]:
        raise NotImplementedError

    def run(self, keywords=None, target=MAX_TOTAL_PER_SOURCE):
        keywords = keywords or get_queries_for_source(self.source_name)
        logger.info("[%s] catalog-mode, %s queries", self.source_name, len(keywords))

        catalog = self.load_catalog_once()
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
                r = requests.get(f"https://api.hh.ru/vacancies/{vacancy_id}", headers=self.headers, timeout=REQUEST_TIMEOUT)
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
        description = v.get("description") or " ".join(filter(None, [snippet.get("requirement"), snippet.get("responsibility")]))
        requirements_text = "; ".join(skills) if skills else (snippet.get("requirement") or "")
        responsibilities_text = snippet.get("responsibility") or ""
        desc_text = str(description or "").lower()
        visa = True if any(w in desc_text for w in ["visa", "виза", "визовая"]) else None
        relocation = True if any(w in desc_text for w in ["relocation", "релокац", "переезд"]) else None
        employment = self._sd(v.get("employment")).get("name") or sched.get("name")
        return self._rec(
            source_job_id=str(v.get("id") or ""),
            title=v.get("name"), description=description, requirements=requirements_text,
            responsibilities=responsibilities_text,
            company_name=emp.get("name"), department=dept,
            salary_from=sal.get("from"), salary_to=sal.get("to"), currency=sal.get("currency"),
            salary_period=None, experience_level=en, years_min=y1, years_max=y2,
            key_skills=skills, location=area.get("name"), country=country,
            remote=rem, remote_type="remote" if rem else "onsite",
            employment_type=employment, published_at=v.get("published_at"),
            url=v.get("alternate_url") or f"https://hh.ru/vacancy/{v.get('id')}", visa_sponsorship=visa, relocation=relocation,
            search_query=keyword, raw_json=v,
        )

    def fetch(self, keyword, target, **kwargs):
        res = []
        areas = [(113, "Russia"), (1, "Russia"), (2, "Russia"), (88, "Belarus"), (160, "Kazakhstan"), (1005, "Ukraine"), (1002, "Uzbekistan")]
        per_area_target = max(1, target // len(areas))
        for area_id, country in areas:
            if len(res) >= target:
                break
            area_count = 0
            page = 0
            while area_count < per_area_target and len(res) < target:
                r = requests.get(
                    "https://api.hh.ru/vacancies",
                    params={"text": keyword, "area": area_id, "per_page": 100, "page": page},
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT,
                )
                if r.status_code != 200:
                    break
                data = r.json()
                items = data.get("items", [])
                if not items:
                    break
                for item in items:
                    if area_count >= per_area_target or len(res) >= target:
                        break
                    vacancy = item
                    if self.detail_fetch_limit > 0:
                        detail = self._det(item.get("id"))
                        if detail:
                            vacancy = detail
                    rec = self._p(vacancy, keyword, country)
                    res.append(rec)
                    area_count += 1
                page += 1
                if page >= data.get("pages", 0):
                    break
                time.sleep(0.35)
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
        return any(x in text for x in [
            "rate limit",
            "too many requests",
            "quota",
            "exceeded",
            "limit reached",
        ])

    def fetch(self, keyword, target, **kwargs):
        if self.auth_failed:
            return []

        res = []
        per_country_target = max(1, target // len(ADZUNA_C))

        for country_code, country_name, default_currency in ADZUNA_C:
            if len(res) >= target or self.auth_failed:
                break

            page = 1
            country_count = 0
            rate_limit_hits = 0

            while country_count < per_country_target and len(res) < target:
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
                    logger.warning("[adzuna.com] %s %s page=%s request failed: %s", country_code, keyword, page, e)
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
                        country_code, keyword, page, rate_limit_hits, self.max_rate_limit_retries, sleep_for
                    )
                    if rate_limit_hits >= self.max_rate_limit_retries:
                        logger.warning(
                            "[adzuna.com] skip country due to repeated rate limits: country=%s query=%r",
                            country_code, keyword
                        )
                        break
                    time.sleep(sleep_for)
                    continue

                if r.status_code != 200:
                    logger.warning(
                        "[adzuna.com] HTTP %s: country=%s query=%r page=%s",
                        r.status_code, country_code, keyword, page
                    )
                    break

                rate_limit_hits = 0

                try:
                    jobs = r.json().get("results", [])
                except Exception as e:
                    logger.warning("[adzuna.com] bad json: country=%s query=%r page=%s err=%s", country_code, keyword, page, e)
                    break

                if not jobs:
                    break

                for j in jobs:
                    if country_count >= per_country_target or len(res) >= target:
                        break

                    loc = self._sd(j.get("location"))
                    y1, y2 = _yrs(j.get("description") or "")
                    desc = j.get("description") or ""
                    remote = "remote" in desc.lower()

                    res.append(self._rec(
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
                        remote_type="remote" if remote else "onsite",
                        published_at=j.get("created"),
                        url=j.get("redirect_url"),
                        search_query=keyword,
                        years_min=y1,
                        years_max=y2,
                        raw_json=j,
                    ))
                    country_count += 1

                page += 1
                time.sleep(0.5)

        return res


class USAJobsParser(QueryParserBase):
    def __init__(self, api_key, email):
        super().__init__("usajobs.gov")
        self.headers.update({"Host": "data.usajobs.gov", "User-Agent": email, "Authorization-Key": api_key})

    def fetch(self, keyword, target, **kwargs):
        res = []
        page = 1
        while len(res) < target:
            r = requests.get("https://data.usajobs.gov/api/search", params={"Keyword": keyword, "ResultsPerPage": 250, "Page": page}, headers=self.headers, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                break
            items = r.json().get("SearchResult", {}).get("SearchResultItems", [])
            if not items:
                break
            for item in items:
                if len(res) >= target:
                    break
                try:
                    j = self._sd(item.get("MatchedObjectDescriptor"))
                    locs = j.get("PositionLocationDisplay", []) or []
                    loc = "; ".join(locs) if isinstance(locs, list) else locs
                    det = self._sd(self._sd(j.get("UserArea")).get("Details"))
                    desc = det.get("JobSummary") or det.get("MajorDuties") or ""
                    rems = self._sl(j.get("PositionRemuneration"))
                    first_pay = self._sd(rems[0]) if rems else {}
                    res.append(self._rec(
                        source_job_id=str(j.get("PositionID") or ""),
                        title=j.get("PositionTitle"), description=desc, company_name=j.get("OrganizationName"),
                        salary_from=first_pay.get("MinimumRange"), salary_to=first_pay.get("MaximumRange"), currency="USD",
                        location=loc, country="United States", employment_type=self._sd(j.get("PositionSchedule")).get("Name"),
                        remote=any(w in desc.lower() for w in ["remote", "telework", "work from home"]), published_at=j.get("PublicationStartDate"),
                        url=j.get("PositionURI"), search_query=keyword, raw_json=j,
                    ))
                except Exception as e:
                    logger.warning("USAJobs item parse failed: %s", e)
            page += 1
            time.sleep(0.5)
        return res


class HimalayasParser(CatalogParserBase):
    def __init__(self):
        super().__init__("himalayas.app")
        self.page_size = int(os.getenv("HIMALAYAS_PAGE_SIZE", "20"))
        self.max_retries = int(os.getenv("HIMALAYAS_MAX_RETRIES", "2"))
        self.retry_sleep = float(os.getenv("HIMALAYAS_RETRY_SLEEP", "2"))
        self.page_sleep = float(os.getenv("HIMALAYAS_PAGE_SLEEP", "0.4"))

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[himalayas.app] loading catalog once for this run")
        jobs_all = []
        offset = 0

        while True:
            success = False

            for attempt in range(1, self.max_retries + 2):
                try:
                    r = requests.get(
                        "https://himalayas.app/jobs/api",
                        params={"offset": offset, "limit": self.page_size},
                        headers=self.headers,
                        timeout=REQUEST_TIMEOUT,
                    )

                    if r.status_code != 200:
                        logger.warning("[himalayas.app] HTTP %s at offset=%s", r.status_code, offset)
                        self._catalog_cache = jobs_all
                        return self._catalog_cache

                    data = r.json()
                    jobs = data if isinstance(data, list) else data.get("jobs", [])
                    success = True
                    break

                except requests.exceptions.Timeout:
                    logger.warning(
                        "[himalayas.app] timeout at offset=%s retry=%s/%s",
                        offset, attempt, self.max_retries + 1
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
                    logger.warning("[himalayas.app] request failed at offset=%s err=%s", offset, e)
                    self._catalog_cache = jobs_all
                    return self._catalog_cache

            if not success:
                self._catalog_cache = jobs_all
                return self._catalog_cache

            if not jobs:
                break

            jobs_all.extend(jobs)

            if len(jobs) < self.page_size:
                break

            offset += self.page_size
            time.sleep(self.page_sleep)

        logger.info("[himalayas.app] catalog loaded: %s jobs", len(jobs_all))
        self._catalog_cache = jobs_all
        return self._catalog_cache

    def raw_to_record(self, j: dict, matched_query: Optional[str]) -> Optional[dict]:
        title = j.get("title", "")
        return self._rec(
            source_job_id=str(j.get("id") or j.get("slug") or ""),
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
            url=j.get("url"),
            search_query=matched_query,
            raw_json=j,
        )


class ArbeitnowParser(QueryParserBase):
    def __init__(self):
        super().__init__("arbeitnow.com")

    def fetch(self, keyword, target, **kwargs):
        res = []
        page = 1
        while len(res) < target:
            r = requests.get("https://www.arbeitnow.com/api/job-board-api", params={"page": page}, headers=self.headers, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                break
            data = r.json()
            jobs = data.get("data", [])
            if not jobs:
                break
            for j in jobs:
                title = j.get("title", "")
                if keyword.lower() not in title.lower():
                    continue
                if len(res) >= target:
                    break
                remote = bool(j.get("remote", False))
                res.append(self._rec(
                    source_job_id=str(j.get("slug") or j.get("id") or ""),
                    title=title, description=j.get("description"), company_name=j.get("company_name"),
                    location=j.get("location"), country=j.get("country"), remote=remote, remote_type="remote" if remote else "onsite",
                    published_at=j.get("created_at"), url=j.get("url"), key_skills=j.get("tags", []), search_query=keyword, currency="EUR", raw_json=j,
                ))
            page += 1
            if not data.get("links", {}).get("next"):
                break
            time.sleep(0.8)
        return res


class GreenhouseParser(CatalogParserBase):
    def __init__(self):
        super().__init__("greenhouse.com")

    def load_catalog_once(self):
        if self._catalog_cache is not None:
            return self._catalog_cache

        logger.info("[greenhouse.com] loading catalog once for this run")
        jobs_all = []

        for company in get_active_companies("greenhouse"):
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
                if not jobs:
                    reset_fail_count("greenhouse", company)
                    continue

                reset_fail_count("greenhouse", company)

                for j in jobs:
                    j["_company"] = company
                    jobs_all.append(j)

                time.sleep(0.3)

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

        for company in get_active_companies("lever"):
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

                time.sleep(0.3)

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

        return self._rec(
            source_job_id=str(j.get("id") or ""),
            title=j.get("text"),
            description=desc,
            company_name=j.get("_company"),
            location=cats.get("location"),
            employment_type=cats.get("commitment"),
            published_at=j.get("createdAt"),
            url=j.get("hostedUrl"),
            search_query=matched_query,
            raw_json=j,
        )

class AshbyParser(QueryParserBase):
    def __init__(self):
        super().__init__("ashbyhq.com")

    def fetch(self, keyword, target, **kwargs):
        res = []
        kl = keyword.lower()
        for company in get_active_companies("ashby"):
            if len(res) >= target:
                break
            try:
                r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{company}", params={"includeCompensation": "true"}, headers=self.headers, timeout=REQUEST_TIMEOUT)
                if r.status_code == 404:
                    mark_inactive("ashby", company, "404")
                    continue
                if r.status_code != 200:
                    mark_inactive("ashby", company, f"HTTP {r.status_code}")
                    continue
                jobs = r.json().get("jobs", [])
                reset_fail_count("ashby", company)
                for j in jobs:
                    title = j.get("title", "")
                    if kl not in title.lower():
                        continue
                    if len(res) >= target:
                        break
                    comp = self._sd(j.get("compensation"))
                    sr = self._sd(comp.get("range"))
                    res.append(self._rec(
                        source_job_id=str(j.get("id") or j.get("jobId") or ""),
                        title=title, description=j.get("descriptionHtml"), company_name=company,
                        location=j.get("location"), salary_from=sr.get("minimum"), salary_to=sr.get("maximum"), currency=comp.get("currency"),
                        remote=j.get("isRemote", False), published_at=j.get("publishedAt"), url=j.get("jobUrl"), search_query=keyword, raw_json=j,
                    ))
                time.sleep(0.3)
            except requests.exceptions.Timeout:
                mark_inactive("ashby", company, "timeout")
            except Exception as e:
                logger.warning("Ashby %s: %s", company, e)
                mark_inactive("ashby", company, str(e)[:100])
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