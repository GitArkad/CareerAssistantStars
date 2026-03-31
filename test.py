from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://postgres:post123gres@16.54.110.212:5433/postgres"

try:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print("✅ OK:", conn.execute(text("SELECT 1")).scalar())
except Exception as e:
    print("❌ ERROR:", e)