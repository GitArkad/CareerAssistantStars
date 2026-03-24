import pandas as pd
from sqlalchemy import create_engine
import os


def load_to_postgres(csv_path, table_name="jobs"):
    if not os.path.exists(csv_path):
        print("❌ CSV не найден")
        return

    df = pd.read_csv(csv_path)

    print(f"📥 Загружено {len(df)} строк из CSV")

    # Подключение к Postgres
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )

    # Загрузка
    df.to_sql(
        table_name,
        engine,
        if_exists='append',  # 🔥 добавляем, не перезаписываем
        index=False
    )

    print(f"✅ Данные загружены в таблицу {table_name}")