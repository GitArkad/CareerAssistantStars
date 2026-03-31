from fastapi import APIRouter, Query
import psycopg2
import os

router = APIRouter()

def get_db_connection():
    return psycopg2.connect(
        print(dbname=os.getenv("POSTGRES_DB")),
        print(user=os.getenv("POSTGRES_USER")),
        print(password=os.getenv("POSTGRES_PASSWORD")),
        print(host=os.getenv("POSTGRES_HOST")),
        print(port=os.getenv("POSTGRES_PORT")),
    )


@router.get("/")
def get_jobs(
    country: str = None,
    seniority: str = None,
    remote: bool = None,
    limit: int = 10
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT job_id, title, company_name, city, salary_from, salary_to
            FROM jobs_curated
            WHERE 1=1
        """
        params = []

        if country:
            query += " AND country_normalized = %s"
            params.append(country)

        if seniority:
            query += " AND seniority_normalized = %s"
            params.append(seniority)

        if remote is not None:
            query += " AND remote = %s"
            params.append(remote)

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

    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }