# ingestion_node.py
from app.agents.state import AgentState
from app.agents.utils import get_extraction_chain
from langchain_core.messages import AIMessage
from app.agents.services.parser import ResumeParser
from app.agents.services.taxonomy import IT_DS_TAXONOMY

def ingestion_node(state: AgentState):
    print("--- ЗАПУСК: INGESTION (Валидация и Очистка) ---")

    # Пустой профиль (fallback)
    empty_candidate = {
        "name": "Кандидат",
        "country": "Remote",
        "city": "Remote",
        "relocation": False,
        "grade": None,
        "specialization": "ML Engineer",
        "experience_years": 0.5,
        "desired_salary": 0,
        "work_format": [],
        "foreign_languages": [],
        "skills": []
    }

    raw_file = state.get("raw_file_content")
    file_name = state.get("file_name", "Resume_PDF.pdf")

    # ❗ 1. ЕСЛИ НЕТ ФАЙЛА — ПРОПУСКАЕМ INGESTION
    if not raw_file:
        print("[INGESTION SKIP] Нет файла — пропускаем ingestion")

        return {
            "candidate": state.get("candidate", empty_candidate),
            "next_step": "analysis",
            "error": None,
            "stage": state.get("stage", "analysis")
        }

    # ❗ 2. ПАРСИНГ
    try:
        raw_text = ResumeParser.parse(raw_file, file_name)
    except Exception as e:
        print(f"Ошибка при парсинге файла: {e}")
        raw_text = ""

    text = raw_text.strip()

    # ❗ 3. ФИЛЬТР НЕИНФОРМАТИВНОГО ТЕКСТА
    if (
        not text
        or len(text) < 700
        or len(text.split()) < 100
    ):
        print(f"[INGESTION WARNING] Слишком короткий текст: {len(text)} символов")
        print("[INGESTION] Используем старый candidate (если был)")

        return {
            "candidate": state.get("candidate", empty_candidate),
            "messages": [
                AIMessage(content=f"""
Файл '{file_name}' слишком короткий или неинформативный для анализа.

Попробуй:
- загрузить более полный PDF
- или вставить текст резюме прямо в чат
""")
            ],
            "error": None,
            "next_step": "chat",
            "stage": "chat"
        }

    # ❗ 4. LLM ИЗВЛЕЧЕНИЕ
    print("Текст валиден, запускаем извлечение...")
    extracted_data = get_extraction_chain(raw_text)

    # Опыт
    experience_years = extracted_data.experience_years
    if experience_years == 0:
        print("Стаж не найден. Ставим 0.5")
        experience_years = 0.5

    # Локация
    country = extracted_data.country or "Remote"
    city = extracted_data.city or "Remote"

    # Специализация
    specialization = extracted_data.specialization or "ML Engineer"

    # ❗ 5. ОБОГАЩЕНИЕ СКИЛЛОВ
    raw_skills = extracted_data.skills or []
    enriched_skills = set(raw_skills)
    raw_text_lower = raw_text.lower()

    print("Обогащаем единый список навыков...")

    for parent_skill, children in IT_DS_TAXONOMY.items():
        has_child_in_list = any(
            c.lower() in [s.lower() for s in raw_skills]
            for c in children
        )
        has_child_in_text = any(
            c.lower() in raw_text_lower
            for c in children
        )

        if has_child_in_list or has_child_in_text:
            if parent_skill not in enriched_skills:
                enriched_skills.add(parent_skill)
                print(f"Добавлена категория: {parent_skill}")

    final_skills = list(enriched_skills)
    print(final_skills)

    # ❗ 6. ФИНАЛЬНЫЙ ВОЗВРАТ
    return {
        "candidate": {
            "name": extracted_data.name,
            "country": country,
            "city": city,
            "relocation": extracted_data.relocation,
            "grade": extracted_data.grade,
            "specialization": specialization,
            "experience_years": experience_years,
            "desired_salary": extracted_data.desired_salary,
            "work_format": extracted_data.work_format,
            "foreign_languages": extracted_data.foreign_languages,
            "skills": final_skills
        },
        "raw_text": raw_text,  # ❗ важно для дальнейших шагов
        "next_step": "analysis",
        "error": None,
        "stage": "analysis"
    }