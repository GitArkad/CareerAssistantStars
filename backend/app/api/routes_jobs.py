from fastapi import APIRouter
import psycopg2
import os

router = APIRouter()

# ================================
# ПОДКЛЮЧЕНИЕ К POSTGRES
# ================================
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


# ================================
# ПОЛУЧИТЬ ВАКАНСИИ
# ================================
@router.get("/")
def get_jobs(limit: int = 10):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM jobs LIMIT {limit}")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return {"count": len(rows), "data": rows}