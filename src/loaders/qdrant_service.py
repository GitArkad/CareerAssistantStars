"""
qdrant_service.py

Qdrant integration using FastEmbed (built into qdrant-client).
Model: intfloat/multilingual-e5-large (1024d, RU/EN).

Matches the team's format:
- client.set_model() + client.add(documents, metadata, ids)
- E5 prefix: "passage:" for documents, "query:" for search
- Deterministic UUID from company+title
- Payload: skills as list, grade, work_format, country, city, relocation
- Compatible with LangGraph candidate state

Usage:
    from qdrant_service import init_qdrant, load_vacancies_to_qdrant
    from qdrant_service import search_for_candidate, search_similar

    init_qdrant()
    load_vacancies_to_qdrant()

    results = search_for_candidate({
        "grade": "senior",
        "specialization": "ML Engineer",
        "skills": ["Python", "PyTorch"],
        "country": "Russia",
        "work_format": "remote",
    })
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

# ============================================================================
# CONNECTION
# ============================================================================

_client: Optional[QdrantClient] = None


def get_client() -> QdrantClient:
    """Get or create Qdrant client with FastEmbed model."""
    global _client
    if _client is not None:
        return _client

    url = os.getenv("QDRANT_URL", "http://qdrant:6333")
    retries = int(os.getenv("QDRANT_RETRIES", "20"))
    delay = int(os.getenv("QDRANT_DELAY", "3"))

    last_error = None
    for attempt in range(retries):
        try:
            client = QdrantClient(url=url)
            client.get_collections()  # health check
            client.set_model(MODEL_NAME)
            logger.info(f"Qdrant connected, model={MODEL_NAME} (attempt {attempt + 1})")
            _client = client
            return _client
        except Exception as e:
            last_error = e
            logger.warning(f"Qdrant not ready {attempt + 1}/{retries}")
            time.sleep(delay)

    raise RuntimeError(f"Qdrant unavailable: {last_error}")


# ============================================================================
# DETERMINISTIC UUID (same logic as colleague's code)
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
    """Create collection with FastEmbed vector config if not exists."""
    client = get_client()

    if client.collection_exists(COLLECTION_NAME):
        logger.info(f"Collection '{COLLECTION_NAME}' exists")
        return client

    # get_fastembed_vector_params() auto-detects dimension from set_model()
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=client.get_fastembed_vector_params(),
    )
    logger.info(f"Created collection '{COLLECTION_NAME}' with {MODEL_NAME}")

    return client


# ============================================================================
# HELPERS: parse pg arrays, build document text
# ============================================================================

def _parse_pg_array(val) -> List[str]:
    """Parse postgres array {python,sql} → ['python', 'sql']."""
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    if not isinstance(val, str) or not val.strip():
        return []
    val = val.strip()
    if val.startswith("{") and val.endswith("}"):
        inner = val[1:-1]
        return [s.strip() for s in inner.split(",") if s.strip()] if inner else []
    return [s.strip() for s in val.split(",") if s.strip()]


def _map_remote_to_work_format(remote_type: str) -> str:
    """Map our remote_type to colleague's work_format field."""
    mapping = {
        "remote": "Remote",
        "hybrid": "Hybrid",
        "onsite": "Office",
        "office": "Office",
        "unknown": "Unknown",
    }
    return mapping.get(str(remote_type).lower(), "Unknown")


def _build_document(job: Dict) -> str:
    """
    Build E5-formatted document text for embedding.
    Uses "passage:" prefix as required by intfloat/multilingual-e5-large.

    Enriches text with structured fields for better semantic matching.
    """
    grade = job.get("seniority_normalized") or job.get("grade") or "Specialist"
    title = job.get("title") or "Position"
    company = job.get("company_name") or job.get("company") or "Company"
    country = job.get("country") or ""
    city = job.get("city") or ""
    work_format = _map_remote_to_work_format(
        job.get("remote_type") or job.get("work_format") or "")

    # Skills as text
    skills = _parse_pg_array(
        job.get("skills") or job.get("skills_normalized") or job.get("skills_extracted"))
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
    Build metadata payload matching the team's format.
    Fields align with LangGraph candidate state.
    """
    # Parse skills to LIST of strings (as required by data engineer)
    skills_raw = (
        job.get("skills")
        or job.get("skills_normalized")
        or job.get("skills_extracted")
    )
    skills = _parse_pg_array(skills_raw)

    # Also merge key_skills
    key_skills = _parse_pg_array(job.get("key_skills"))
    all_skills = list(dict.fromkeys(skills + key_skills))  # dedupe

    # Map remote_type → work_format
    work_format = _map_remote_to_work_format(
        job.get("remote_type") or job.get("work_format") or "")

    # City from location if not set
    city = job.get("city") or ""
    if not city and job.get("location"):
        city = str(job["location"]).split(",")[0].strip()

    return {
        # Core fields (match colleague's format)
        "title": job.get("title"),
        "company": job.get("company_name") or job.get("company"),
        "city": city,
        "country": job.get("country"),
        "work_format": work_format,
        "relocation": bool(job.get("relocation", False)),
        "grade": job.get("seniority_normalized") or job.get("grade") or "unknown",
        "salary_from": job.get("salary_from"),
        "salary_to": job.get("salary_to"),
        "currency": job.get("currency"),
        "skills": all_skills,  # LIST of strings!
        "description": str(job.get("description") or "")[:1000],

        # Extra fields for our pipeline
        "job_id": job.get("job_id"),
        "source": job.get("source"),
        "url": job.get("url"),
        "remote_type": job.get("remote_type"),
        "employment_type": job.get("employment_type"),
    }


# ============================================================================
# LOAD VACANCIES (Airflow Task 5)
# ============================================================================

def load_vacancies_to_qdrant(date_str: str = None, batch_size: int = BATCH_SIZE) -> Dict:
    """
    Fetch pending jobs from Postgres, embed via FastEmbed, load to Qdrant.
    Uses client.add() — same approach as colleague's init_db_uuid().
    """
    from src.loaders.db_loader import get_connection

    client = get_client()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT job_id, source_job_id, title, description, company_name, location, country, city,
            seniority_normalized, skills_extracted, skills_normalized, key_skills,
            remote, remote_type, employment_type, relocation,
            salary_from, salary_to, currency, source, url
        FROM jobs_curated
        WHERE embedding_status = 'pending'
        AND title IS NOT NULL
        AND (%s IS NULL OR DATE(COALESCE(parsed_at, created_at)) = %s::date)
        ORDER BY created_at DESC
        LIMIT 10000
    """, (date_str, date_str))
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()

    if not rows:
        logger.info("No pending jobs to embed")
        cur.close()
        conn.close()
        return {"loaded": 0, "failed": 0}

    logger.info(f"Loading {len(rows)} jobs to Qdrant")
    loaded = 0
    failed = 0
    done_ids = []

    for i in range(0, len(rows), batch_size):
        batch = [dict(zip(columns, r)) for r in rows[i:i + batch_size]]

        try:
            documents = []
            metadata_list = []
            ids = []

            for job in batch:
                # Build document text with "passage:" prefix
                doc_text = _build_document(job)
                documents.append(doc_text)

                # Build metadata payload
                meta = _build_metadata(job)
                metadata_list.append(meta)

                # Deterministic UUID
                uid = _det_uuid(job)
                ids.append(uid)

            # Load via client.add() — FastEmbed creates vectors automatically
            client.add(
                collection_name=COLLECTION_NAME,
                documents=documents,
                metadata=metadata_list,
                ids=ids,
            )

            done_ids.extend(job["job_id"] for job in batch)
            loaded += len(batch)
            logger.info(f"Batch {i // batch_size + 1}: {len(batch)} loaded")

        except Exception as e:
            logger.error(f"Batch {i // batch_size + 1} failed: {e}")
            failed += len(batch)

    # Update status in Postgres
    if done_ids:
        cur.execute("""
            UPDATE jobs_curated SET embedding_status = 'created', updated_at = NOW()
            WHERE job_id = ANY(%s)
        """, (done_ids,))
        conn.commit()

    cur.close()
    conn.close()

    result = {"loaded": loaded, "failed": failed}
    logger.info(f"Qdrant load done: {result}")
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

    Uses "query:" prefix as required by E5 model.

    candidate: {
        "name": "Ivan",
        "grade": "senior",
        "specialization": "Data Engineer",
        "skills": ["Python", "Spark", "Airflow", "SQL"],
        "country": "Россия",
        "city": "Москва",
        "work_format": "remote",
        "experience_years": 5,
        "desired_salary": 300000,
        "foreign_languages": ["English", "Russian"],
    }
    """
    client = get_client()

    # Build query text with E5 "query:" prefix
    parts = []
    if candidate.get("specialization"):
        parts.append(candidate["specialization"])
    if candidate.get("grade"):
        parts.append(candidate["grade"])
    if candidate.get("skills"):
        parts.append(f"Skills: {', '.join(candidate['skills'])}")
    if candidate.get("country"):
        parts.append(f"in {candidate['country']}")
    if candidate.get("experience_years"):
        parts.append(f"{candidate['experience_years']} years experience")

    query_text = f"query: {' '.join(parts)}"

    # Build Qdrant filter
    conditions = []

    grade = candidate.get("grade")
    if grade:
        grade_map = {
            "junior": ["Junior", "junior", "intern"],
            "middle": ["Middle", "middle"],
            "senior": ["Senior", "senior", "lead", "Lead"],
            "lead": ["Lead", "lead", "senior", "Senior", "principal"],
        }
        allowed = grade_map.get(grade.lower(), [grade])
        conditions.append(
            models.FieldCondition(
                key="grade",
                match=models.MatchAny(any=allowed),
            )
        )

    wf = candidate.get("work_format")
    if wf:
        wf_map = {
            "remote": ["Remote"],
            "hybrid": ["Hybrid", "Remote"],
            "office": ["Office"],
            "onsite": ["Office"],
        }
        allowed_wf = wf_map.get(wf.lower(), [wf])
        conditions.append(
            models.FieldCondition(
                key="work_format",
                match=models.MatchAny(any=allowed_wf),
            )
        )

    country = candidate.get("country")
    if country:
        conditions.append(
            models.FieldCondition(
                key="country",
                match=models.MatchValue(value=country),
            )
        )

    query_filter = models.Filter(must=conditions) if conditions else None

    # Search using client.query() — FastEmbed encodes the query automatically
    results = client.query(
        collection_name=COLLECTION_NAME,
        query_text=query_text,
        query_filter=query_filter,
        limit=limit,
    )

    # Enrich results with skill matching
    candidate_skills = set(
        s.lower() for s in (candidate.get("skills") or []))

    output = []
    for hit in results:
        meta = hit.metadata
        score = hit.score if hasattr(hit, "score") else 0

        # Skill overlap
        job_skills = set(
            s.lower() for s in (meta.get("skills") or []))
        overlap = candidate_skills & job_skills

        result = {
            **meta,
            "score": round(score, 4) if score else None,
            "skill_match_count": len(overlap),
            "skill_match_pct": round(
                len(overlap) / max(len(candidate_skills), 1) * 100, 1),
            "matched_skills": sorted(overlap),
            "missing_skills": sorted(candidate_skills - job_skills),
        }
        output.append(result)

    # Sort by score desc, then skill match
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
    """
    Semantic search by free text query.
    Adds "query:" prefix for E5 model.
    """
    client = get_client()

    conditions = []
    if country:
        conditions.append(
            models.FieldCondition(
                key="country",
                match=models.MatchValue(value=country)))
    if grade:
        conditions.append(
            models.FieldCondition(
                key="grade",
                match=models.MatchValue(value=grade)))
    if work_format:
        conditions.append(
            models.FieldCondition(
                key="work_format",
                match=models.MatchValue(value=work_format)))

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