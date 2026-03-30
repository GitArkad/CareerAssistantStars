"""
qdrant_service.py

Qdrant integration using FastEmbed (built into qdrant-client).
Model: intfloat/multilingual-e5-large (1024d, RU/EN).

v2 — Payload выровнен с форматом кандидата из LangGraph:
  Кандидат:                     Вакансия (payload):
  ─────────                     ───────────────────
  grade                    ↔    grade              (Junior/Middle/Senior/Lead/...)
  specialization           ↔    title              (название вакансии)
  skills: [str]            ↔    skills: [str]      (канонические имена из SKILL_SYNONYMS)
  country                  ↔    country            (UPPER CASE)
  city                     ↔    city               (UPPER CASE)
  work_format: [str]       ↔    work_format: str   (Remote/Hybrid/Office/Unknown)
  experience_years         ↔    years_experience_min / years_experience_max
  desired_salary           ↔    salary_from / salary_to (МЕСЯЧНЫЕ, оригинальная валюта)
                                + salary_from_rub / salary_to_rub (МЕСЯЧНЫЕ, в рублях для матчинга)
  foreign_languages: [str] ↔    spoken_languages: [str]
  relocation               ↔    relocation

Ключевые изменения:
- Убраны ссылки на удалённые колонки (key_skills, skills_extracted, tech_stack_tags)
- skills берутся ТОЛЬКО из skills_normalized (канонические имена)
- grade маппит seniority_normalized в формат, совместимый с кандидатом
- country/city в UPPER CASE
- salary_from/salary_to — всегда месячные
- normalize_candidate_skills() — нормализует скиллы кандидата тем же словарём
"""

from __future__ import annotations

import os
import uuid
import time
import logging
from typing import Optional, List, Dict, Any

from qdrant_client import QdrantClient
from qdrant_client.http import models

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG
# ============================================================================

COLLECTION_NAME = "vacancies"
MODEL_NAME = "intfloat/multilingual-e5-large"  # 1024d, multilingual RU/EN
BATCH_SIZE = 64
EMBEDDING_LIMIT = int(os.getenv("EMBEDDING_LIMIT", "3000"))

# ============================================================================
# CONNECTION
# ============================================================================

_client: Optional[QdrantClient] = None


def _detect_cuda_available() -> bool:
    """
    Пытаемся понять, доступен ли GPU для ONNX/FastEmbed.
    Если нет GPU runtime или нет нужных пакетов — спокойно уходим в CPU.
    """
    forced = os.getenv("QDRANT_CUDA", "auto").strip().lower()

    if forced in {"0", "false", "no", "off", "cpu"}:
        return False
    if forced in {"1", "true", "yes", "on", "gpu"}:
        return True

    # auto mode
    try:
        import onnxruntime as ort
        providers = ort.get_available_providers()
        return "CUDAExecutionProvider" in providers
    except Exception:
        return False


def _parse_device_ids() -> Optional[list[int]]:
    raw = os.getenv("QDRANT_CUDA_DEVICE_IDS", "").strip()
    if not raw:
        return None
    try:
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    except Exception:
        return None


def get_client() -> QdrantClient:
    global _client
    if _client is not None:
        return _client

    url = os.getenv("QDRANT_URL", "http://qdrant:6333")
    retries = int(os.getenv("QDRANT_RETRIES", "20"))
    delay = int(os.getenv("QDRANT_DELAY", "3"))

    use_cuda = _detect_cuda_available()
    device_ids = _parse_device_ids()

    last_error = None
    for attempt in range(retries):
        try:
            client = QdrantClient(url=url)
            client.get_collections()
            client.set_model(
                MODEL_NAME,
                cuda=use_cuda,
                device_ids=device_ids if use_cuda else None,
            )
            logger.info(
                "Qdrant connected, model=%s, cuda=%s, device_ids=%s (attempt %s)",
                MODEL_NAME,
                use_cuda,
                device_ids,
                attempt + 1,
            )
            _client = client
            return _client
        except Exception as e:
            last_error = e
            logger.warning("Qdrant not ready %s/%s", attempt + 1, retries)
            time.sleep(delay)

    raise RuntimeError(f"Qdrant unavailable: {last_error}")

# ============================================================================
# DETERMINISTIC UUID
# ============================================================================

def _det_uuid(job: dict) -> str:
    stable_key = (
        job.get("job_id")
        or job.get("source_job_id")
        or job.get("url")
        or f"{job.get('company_name', '')}_{job.get('title', '')}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, str(stable_key)))


# ============================================================================
# COLLECTION INIT
# ============================================================================

def init_qdrant() -> QdrantClient:
    client = get_client()
    if client.collection_exists(COLLECTION_NAME):
        logger.info(f"Collection '{COLLECTION_NAME}' exists")
        return client

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=client.get_fastembed_vector_params(),
    )
    logger.info(f"Created collection '{COLLECTION_NAME}' with {MODEL_NAME}")
    return client


# ============================================================================
# GRADE MAPPING: seniority_normalized → grade (совместимый с кандидатом)
# ============================================================================

# Маппинг seniority_normalized в формат, который кандидат тоже использует.
# Кандидат присылает "Junior|Middle|Senior", но мы поддерживаем расширенный набор.
SENIORITY_TO_GRADE = {
    "intern": "Intern",
    "junior": "Junior",
    "middle": "Middle",
    "senior": "Senior",
    "lead": "Lead",
    "principal": "Principal",
    "manager": "Manager",
    "director": "Director",
    "unknown": "Specialist",
}

# Обратный маппинг: какой grade кандидата покрывает какие grade вакансий.
GRADE_MATCH_MAP = {
    "intern": ["Intern"],
    "junior": ["Intern", "Junior"],
    "middle": ["Middle", "Junior"],
    "senior": ["Senior", "Lead"],
    "lead": ["Lead", "Senior", "Principal"],
    "principal": ["Principal", "Lead", "Senior"],
    "manager": ["Manager", "Lead", "Director"],
    "director": ["Director", "Manager"],
}


def _map_grade(seniority_normalized: str) -> str:
    """Маппит seniority_normalized в grade для payload."""
    return SENIORITY_TO_GRADE.get(
        str(seniority_normalized).strip().lower(), "Specialist"
    )


# ============================================================================
# WORK FORMAT MAPPING: remote_type → work_format
# ============================================================================

REMOTE_TO_WORK_FORMAT = {
    "remote": "Remote",
    "hybrid": "Hybrid",
    "onsite": "Office",
    "office": "Office",
    "unknown": "Unknown",
}

WORK_FORMAT_MATCH_MAP = {
    "remote": ["Remote"],
    "hybrid": ["Hybrid", "Remote"],
    "office": ["Office"],
    "onsite": ["Office"],
}


def _map_work_format(remote_type: str) -> str:
    return REMOTE_TO_WORK_FORMAT.get(str(remote_type).lower(), "Unknown")


# ============================================================================
# SKILLS: parse + normalize (shared with candidate)
# ============================================================================

def _parse_pg_array(val) -> List[str]:
    """Parse postgres array / JSON list → list of strings."""
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if not isinstance(val, str) or not val.strip():
        return []
    val = val.strip()
    if val.startswith("{") and val.endswith("}"):
        inner = val[1:-1]
        return [s.strip().strip('"') for s in inner.split(",") if s.strip()] if inner else []
    if val.startswith("["):
        import json
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    return [s.strip() for s in val.split(",") if s.strip()]


def normalize_candidate_skills(raw_skills: List[str]) -> List[str]:
    """
    Нормализует скиллы кандидата ТЕМ ЖЕ словарём, что и вакансии.
    Используется в LangGraph перед поиском.
    """
    try:
        from src.cleaners.data_cleaner import SKILL_SYNONYMS
    except ImportError:
        return raw_skills

    normalized = set()
    for skill in raw_skills:
        key = skill.lower().strip()
        canonical = SKILL_SYNONYMS.get(key, skill)
        normalized.add(canonical)
    return sorted(normalized)


def convert_candidate_salary_to_rub(
    desired_salary: float,
    candidate_currency: str = "RUB",
) -> Optional[float]:
    """
    Конвертирует desired_salary кандидата в RUB для сравнения с salary_from_rub в Qdrant.
    """
    if desired_salary is None:
        return None

    try:
        from src.cleaners.data_cleaner import FALLBACK_TO_RUB_RATES
    except ImportError:
        FALLBACK_TO_RUB_RATES = {"RUB": 1.0, "USD": 92.0, "EUR": 99.0}

    cur = candidate_currency.upper().strip()
    rate = FALLBACK_TO_RUB_RATES.get(cur)
    if rate is None:
        return None
    return round(desired_salary * rate)


# ============================================================================
# BUILD DOCUMENT + METADATA
# ============================================================================

def _build_document(job: Dict) -> str:
    """
    Build E5-formatted document text for embedding.
    "passage:" prefix as required by intfloat/multilingual-e5-large.
    """
    grade = _map_grade(job.get("seniority_normalized") or "unknown")
    title = job.get("title") or "Position"
    company = job.get("company_name") or job.get("company") or "Company"
    country = job.get("country") or ""
    city = job.get("city") or ""
    work_format = _map_work_format(job.get("remote_type") or "")

    # Skills — только из skills_normalized
    skills = _parse_pg_array(job.get("skills_normalized"))
    skills_text = ", ".join(skills) if skills else ""

    description = str(job.get("description") or "")[:800]

    parts = [f"passage: {grade} {title} at {company}"]
    if country:
        parts[0] += f" in {country}"
    if city:
        parts[0] += f" ({city})"
    parts[0] += "."
    if work_format and work_format != "Unknown":
        parts.append(f"Format: {work_format}.")
    if skills_text:
        parts.append(f"Skills: {skills_text}.")
    if description:
        parts.append(description)

    return " ".join(parts)


def _build_metadata(job: Dict) -> Dict[str, Any]:
    """
    Build metadata payload aligned with LangGraph candidate format.

    Кандидат:
    {
        "name": "",
        "country": "",
        "city": "",
        "relocation": false,
        "grade": "",
        "specialization": "",
        "experience_years": 0,
        "desired_salary": 0,
        "work_format": [],
        "foreign_languages": [],
        "skills": [],
    }
    """
    skills = _parse_pg_array(job.get("skills_normalized"))
    foreign_languages = _parse_pg_array(job.get("spoken_languages"))

    grade = _map_grade(job.get("seniority_normalized") or "unknown")
    work_format = _map_work_format(job.get("remote_type") or "")

    years_min = job.get("years_experience_min")
    years_max = job.get("years_experience_max")
    experience_years = years_min if years_min is not None else years_max

    specialization = job.get("specialty") or job.get("title")

    return {
        # Поля, сопоставимые с кандидатом из LangGraph
        "specialization": specialization,
        "title": job.get("title"),   # display field
        "company": job.get("company_name") or job.get("company"),
        "grade": grade,
        "skills": skills,
        "country": job.get("country"),
        "city": job.get("city"),
        "work_format": work_format,
        "experience_years": experience_years,
        "foreign_languages": foreign_languages,
        "relocation": bool(job.get("relocation", False)),

        # Зарплата (месячная после нормализации в cleaner)
        "salary_from": job.get("salary_from"),
        "salary_to": job.get("salary_to"),
        "currency": job.get("currency"),
        "salary_from_rub": job.get("salary_from_rub"),
        "salary_to_rub": job.get("salary_to_rub"),

        # Доп. метаданные
        "description": str(job.get("description") or "")[:1000],
        "job_id": job.get("job_id"),
        "source": job.get("source"),
        "url": job.get("url"),
        "role_family": job.get("role_family"),
        "remote_type": job.get("remote_type"),
        "employment_type": job.get("employment_type"),
        "posting_language": job.get("posting_language"),
        "visa_sponsorship": bool(job.get("visa_sponsorship", False)),
        "specialty_category": job.get("specialty_category"),
        "title_normalized": job.get("title_normalized"),
    }


# ============================================================================
# LOAD VACANCIES (Airflow Task 5)
# ============================================================================

def load_vacancies_to_qdrant(date_str: str = None, batch_size: int = BATCH_SIZE) -> Dict:
    """
    Fetch pending jobs from Postgres, embed via FastEmbed, load to Qdrant.

    Important:
    - all vacancies still go to PostgreSQL at the load step;
    - only vacancies with non-empty skills_normalized are sent to Qdrant,
      because LangGraph matching relies on resume skills vs vacancy skills.
    """
    from src.loaders.db_loader import get_connection

    client = get_client()
    loaded = 0
    failed = 0
    done_ids: list[str] = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_id, source_job_id, title, title_normalized, description, company_name,
                    specialty, specialty_category, role_family,
                    country, city, location,
                    seniority_normalized, years_experience_min, years_experience_max,
                    skills_normalized, spoken_languages,
                    remote, remote_type, employment_type, relocation,
                    salary_from, salary_to, currency,
                    salary_from_rub, salary_to_rub,
                    source, url, posting_language,
                    visa_sponsorship
                FROM jobs_curated
                WHERE embedding_status = 'pending'
                  AND title IS NOT NULL
                  AND COALESCE(array_length(skills_normalized, 1), 0) > 0
                  AND (%s IS NULL OR DATE(COALESCE(parsed_at, created_at)) = %s::date)
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (date_str, date_str, EMBEDDING_LIMIT),
            )
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()

            if not rows:
                logger.info("No pending jobs to embed")
                return {"loaded": 0, "failed": 0}

            logger.info("Loading %s jobs to Qdrant", len(rows))

            for i in range(0, len(rows), batch_size):
                batch = [dict(zip(columns, r)) for r in rows[i:i + batch_size]]

                try:
                    documents = []
                    metadata_list = []
                    ids = []

                    for job in batch:
                        documents.append(_build_document(job))
                        metadata_list.append(_build_metadata(job))
                        ids.append(_det_uuid(job))

                    client.add(
                        collection_name=COLLECTION_NAME,
                        documents=documents,
                        metadata=metadata_list,
                        ids=ids,
                    )

                    done_ids.extend(job["job_id"] for job in batch)
                    loaded += len(batch)
                    logger.info("Batch %s: %s loaded", i // batch_size + 1, len(batch))

                except Exception as e:
                    logger.exception("Batch %s failed: %s", i // batch_size + 1, e)
                    failed += len(batch)

            if done_ids:
                cur.execute(
                    """
                    UPDATE jobs_curated
                    SET embedding_status = 'created', updated_at = NOW()
                    WHERE job_id = ANY(%s)
                    """,
                    (done_ids,),
                )

        conn.commit()

    result = {"loaded": loaded, "failed": failed}
    logger.info("Qdrant load done: %s", result)
    return result


# ============================================================================
# SEARCH: by candidate profile (for LangGraph)
# ============================================================================

def search_for_candidate(
    candidate: Dict[str, Any],
    limit: int = 20,
    score_threshold: float = 0.3,
) -> List[Dict]:
    """
    Match vacancies to a candidate from LangGraph state.

    Кандидат:
    {
        "name": "Иван",
        "grade": "Senior",
        "specialization": "Data Engineer",
        "skills": ["Python", "Spark", "Airflow", "SQL"],
        "country": "Россия",
        "city": "Москва",
        "work_format": ["Remote", "Hybrid"],
        "experience_years": 5,
        "desired_salary": 300000,
        "foreign_languages": ["English", "Russian"],
    }
    """
    client = get_client()

    # Нормализуем скиллы кандидата тем же словарём, что и вакансии.
    raw_skills = candidate.get("skills") or []
    normalized_skills = normalize_candidate_skills(raw_skills)

    # Build query text
    parts = []
    if candidate.get("specialization"):
        parts.append(candidate["specialization"])
    if candidate.get("grade"):
        parts.append(candidate["grade"])
    if normalized_skills:
        parts.append(f"Skills: {', '.join(normalized_skills)}")
    if candidate.get("country"):
        parts.append(f"in {candidate['country']}")
    if candidate.get("experience_years"):
        parts.append(f"{candidate['experience_years']} years experience")

    query_text = f"query: {' '.join(parts)}"

    # Build Qdrant filter
    conditions = []

    # Grade filter — маппим grade кандидата к допустимым grade вакансий.
    grade = candidate.get("grade")
    if grade:
        allowed = GRADE_MATCH_MAP.get(grade.lower(), [grade])
        conditions.append(
            models.FieldCondition(
                key="grade",
                match=models.MatchAny(any=allowed),
            )
        )

    # Work format filter — кандидат может указать несколько форматов.
    wf = candidate.get("work_format")
    if wf:
        if isinstance(wf, str):
            wf = [wf]
        allowed_wf = set()
        for fmt in wf:
            mapped = WORK_FORMAT_MATCH_MAP.get(fmt.lower(), [fmt])
            allowed_wf.update(mapped)
        if allowed_wf:
            conditions.append(
                models.FieldCondition(
                    key="work_format",
                    match=models.MatchAny(any=list(allowed_wf)),
                )
            )

    # Country filter — приводим к UPPER CASE для совместимости.
    country = candidate.get("country")
    if country:
        conditions.append(
            models.FieldCondition(
                key="country",
                match=models.MatchValue(value=country.upper()),
            )
        )

    query_filter = models.Filter(must=conditions) if conditions else None

    results = client.query(
        collection_name=COLLECTION_NAME,
        query_text=query_text,
        query_filter=query_filter,
        limit=limit,
    )

    # Enrich results with skill matching (canonical names).
    candidate_skills_set = set(s.lower() for s in normalized_skills)

    output = []
    for hit in results:
        meta = hit.metadata
        score = hit.score if hasattr(hit, "score") else 0

        job_skills = set(s.lower() for s in (meta.get("skills") or []))
        overlap = candidate_skills_set & job_skills
        missing = candidate_skills_set - job_skills

        result = {
            **meta,
            "score": round(score, 4) if score else None,
            "skill_match_count": len(overlap),
            "skill_match_pct": round(
                len(overlap) / max(len(candidate_skills_set), 1) * 100, 1
            ),
            "matched_skills": sorted(overlap),
            "missing_skills": sorted(missing),
        }
        output.append(result)

    output.sort(
        key=lambda x: (x.get("score") or 0, x["skill_match_count"]),
        reverse=True,
    )
    return output


# ============================================================================
# SEARCH: free text
# ============================================================================

def search_similar(
    query: str,
    limit: int = 20,
    country: str = None,
    grade: str = None,
    work_format: str = None,
) -> List[Dict]:
    """Semantic search by free text query."""
    client = get_client()

    conditions = []
    if country:
        conditions.append(
            models.FieldCondition(
                key="country",
                match=models.MatchValue(value=country.upper()),
            )
        )
    if grade:
        allowed = GRADE_MATCH_MAP.get(grade.lower(), [grade])
        conditions.append(
            models.FieldCondition(
                key="grade",
                match=models.MatchAny(any=allowed),
            )
        )
    if work_format:
        conditions.append(
            models.FieldCondition(
                key="work_format",
                match=models.MatchValue(value=work_format),
            )
        )

    query_filter = models.Filter(must=conditions) if conditions else None

    results = client.query(
        collection_name=COLLECTION_NAME,
        query_text=f"query: {query}",
        query_filter=query_filter,
        limit=limit,
    )

    return [
        {**hit.metadata, "score": round(hit.score, 4) if hasattr(hit, "score") else None}
        for hit in results
    ]


# ============================================================================
# SEARCH: similar to existing job
# ============================================================================

def search_similar_to_job(
    company: str,
    title: str,
    limit: int = 20,
    exclude_same_company: bool = True,
) -> List[Dict]:
    """Find jobs similar to an existing vacancy."""
    client = get_client()
    point_id = _det_uuid({"company_name": company, "title": title})

    try:
        points = client.retrieve(
            collection_name=COLLECTION_NAME,
            ids=[point_id],
            with_vectors=True,
        )
        if not points:
            return []

        vector = points[0].vector
        comp = points[0].payload.get("company")

        qf = None
        if exclude_same_company and comp:
            qf = models.Filter(must_not=[
                models.FieldCondition(
                    key="company",
                    match=models.MatchValue(value=comp),
                )
            ])

        hits = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=vector,
            query_filter=qf,
            limit=limit,
        )
        return [
            {**h.payload, "score": round(h.score, 4)}
            for h in hits
        ]

    except Exception as e:
        logger.error(f"search_similar_to_job: {e}")
        return []


# ============================================================================
# AIRFLOW TASK
# ============================================================================

def run_embedding_step(date_str: str = None) -> Dict:
    """AIRFLOW TASK 5: Init collection + load new vacancies."""
    init_qdrant()
    return load_vacancies_to_qdrant(date_str)


if __name__ == "__main__":
    init_qdrant()
    print("Qdrant ready")