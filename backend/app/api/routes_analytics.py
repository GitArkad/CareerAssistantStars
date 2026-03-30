from fastapi import APIRouter
import psycopg2
import os

router = APIRouter()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


@router.get("/salary")
def get_salary_analytics():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT role, country, seniority, p50, avg_salary
        FROM salary_aggregates
        LIMIT 10
    """)

    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    result = [
        dict(zip(columns, row))
        for row in rows
    ]

    cur.close()
    conn.close()

    return {
        "count": len(result),
        "data": result
    }
    
@router.get("/skills")
def get_skills_analytics(
    role: str = None,
    country: str = None,
    seniority: str = None,
    limit: int = 20
):
    """
    📊 Аналитика по навыкам
    """

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        SELECT skill_name, share_pct, avg_salary, job_count
        FROM market_skill_stats
        WHERE 1=1
    """

    params = []

    if role:
        query += " AND role = %s"
        params.append(role)

    if country:
        query += " AND country = %s"
        params.append(country)

    if seniority:
        query += " AND seniority = %s"
        params.append(seniority)

    query += " ORDER BY share_pct DESC"
    query += " LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    result = [
        dict(zip(columns, row))
        for row in rows
    ]

    cur.close()
    conn.close()

    return {
        "count": len(result),
        "data": result
    }