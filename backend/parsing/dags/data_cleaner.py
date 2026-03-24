import pandas as pd


def clean_data(input_path: str, output_path: str):
    """
    Очистка, нормализация и дедупликация вакансий
    """

    print(f"📥 Читаем файл: {input_path}")
    df = pd.read_csv(input_path)

    # =========================
    # 🧹 1. Удаление дублей
    # =========================
    if 'url' in df.columns:
        df = df.drop_duplicates(subset=['url'])
    else:
        print("⚠️ Нет колонки url — пропускаем дедупликацию")

    # =========================
    # 🧼 2. Очистка текста
    # =========================
    text_cols = ['title', 'description', 'company']

    for col in text_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", " ", regex=True)
            )

    # =========================
    # 💰 3. Зарплата → числа
    # =========================
    for col in ['salary_min', 'salary_max']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # =========================
    # 🌍 4. Нормализация страны
    # =========================
    if 'country' in df.columns:
        df['country'] = df['country'].str.upper()

    # =========================
    # 🧹 5. Удаление пустых строк
    # =========================
    df = df.dropna(subset=['title'])

    print(f"✅ После очистки: {len(df)} строк")

    # =========================
    # 💾 Сохранение
    # =========================
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 Сохранено: {output_path}")