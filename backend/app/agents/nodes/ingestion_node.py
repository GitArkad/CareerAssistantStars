from app.agents.state import AgentState
from app.agents.services.parser import ResumeParser
from app.agents.utils import get_extraction_chain 
from app.agents.services.taxonomy import IT_DS_TAXONOMY

def ingestion_node(state: AgentState):
    print("--- ЗАПУСК: INGESTION (Валидация и Очистка) ---")
    
    # Подготовка ПУСТОГО профиля (Обнуление)
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
    
    # 2. Попытка извлечь текст
    try:
        raw_text = ResumeParser.parse(raw_file, file_name)
    except Exception as e:
        print(f"Ошибка при парсинге файла: {e}")
        raw_text = "" # Если парсер упал на битом файле

    # 3. ГЛАВНЫЙ ФИЛЬТР: Проверка на пустоту или картинку без OCR
    # Если текста меньше 100 символов — это либо пустой файл, либо скан/картинка
    if not raw_text or len(raw_text.strip()) < 100:
        print(f"Файл '{file_name}'/сообщение не содержит читаемого текста.")
        
        return {
            "candidate": empty_candidate, # Возвращаем пустую структуру
            "error": f"Файл '{file_name}' пуст или является изображением без текстового слоя. Пожалуйста, загрузите текстовый PDF или DOCX.",
            "next_step": "end" # Прерываем цепочку, не идем в Analysis
        }

    # 4. Если текст есть — отправляем в LLM
    print("Текст валиден, запускаем извлечение...")
    extracted_data = get_extraction_chain(raw_text)

    # Костыль для пропусков потом можно сделать дополнение через коммуникацию с клентом
    # Опыт, если не указан то 0.5
    experience_years = extracted_data.experience_years
    if experience_years == 0:
        print("Стаж не найден. Ставим 0.5")
        experience_years = 0.5

    # Страна/Город: если None, ставим Remote
    country = extracted_data.country or "Remote"
    city = extracted_data.city or "Remote"

    # Специализация, если пусто, то ML Engineer
    specialization = extracted_data.specialization or "ML Engineer"
    # Обогащение скилов
    # Берем "сырые" скиллы из единственного поля
    raw_skills = extracted_data.skills or []
    enriched_skills = set(raw_skills)
    raw_text_lower = raw_text.lower()

    print("Обогащаем единый список навыков...")
    for parent_skill, children in IT_DS_TAXONOMY.items():
        # Проверяем наличие дочерних навыков в извлеченных скиллах или в самом тексте
        has_child_in_list = any(c.lower() in [s.lower() for s in raw_skills] for c in children)
        has_child_in_text = any(c.lower() in raw_text_lower for c in children)
        
        if has_child_in_list or has_child_in_text:
            if parent_skill not in enriched_skills:
                enriched_skills.add(parent_skill)
                print(f"Добавлена категория: {parent_skill}")

    final_skills = list(enriched_skills)
    print(final_skills)

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
        "next_step": "analysis",
        "error": None # Сбрасываем прошлые ошибки
    }