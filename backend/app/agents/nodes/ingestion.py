from app.agents.state import AgentState
from app.agents.services.parser import ResumeParser
from app.agents.utils import get_extraction_chain 

# def ingestion_node(state: AgentState):
#     print("--- ЗАПУСК: INGESTION (Parsing & Extraction) ---")
    
#     raw_file = state.get("raw_file_content") 
#     file_name = state.get("file_name", "resume.pdf")
    
#     # Проверка: если пришел уже текст, а не байты файла
#     if isinstance(raw_file, str) and not file_name.endswith(('.pdf', '.docx', '.txt')):
#         print("📝 Получен прямой текст, пропускаем стадию парсинга файлов")
#         raw_text = raw_file
#     else:
#         # Если это байты (файл), вызываем твой готовый парсер
#         print(f"📄 Парсим файл: {file_name}")
#         raw_text = ResumeParser.parse(raw_file, file_name)

#     if not raw_text or len(raw_text.strip()) < 100:
#         print(f"ОШИБКА: Файл '{file_name}' пуст или не читается.")
        
#         # Возвращаем информацию об ошибке, НЕ заполняя объект candidate
#         return {
#             "candidate": {}, # Оставляем пустым, чтобы не было старых данных
#             "error": f"Файл {file_name} кажется пустым. Пожалуйста, загрузите заполненное резюме.",
#             "next_step": "end" # Направляем граф в тупик/выход
#         }

#     # 1. Парсинг (Service) — извлекаем чистый текст из PDF
#     # raw_text = ResumeParser.parse(raw_file, file_name)
    
#     # 2. Извлечение (Chain/LLM) 
#     # Функция возвращает объект ExtractionSchema со всеми новыми полями
#     extracted_data = get_extraction_chain(raw_text)
    
#     # 3. Подготовка данных (Объединяем все технические поля в skills)
#     # Используем правильные имена из последней версии схемы
#     all_skills = list(set(
#         extracted_data.programming_languages + 
#         extracted_data.frameworks + 
#         extracted_data.databases + 
#         extracted_data.tools
#     ))
    
#     # 4. Обновление State
#     # Формируем структуру candidate в соответствии с обновленной схемой
#     return {
#         "candidate": {
#             "name": extracted_data.name,
#             "country": extracted_data.country,
#             "city": extracted_data.city,
#             "relocation": extracted_data.relocation,
#             "grade": extracted_data.grade,
#             "specialization": extracted_data.specialization,
#             "experience_years": extracted_data.experience_years,
#             "desired_salary": extracted_data.desired_salary,
#             "work_format": extracted_data.work_format,
#             "foreign_languages": extracted_data.foreign_languages,
#             "skills": all_skills  # Объединенный стек технологий
#         },
#         "next_step": "analysis"
#     }

def ingestion_node(state: AgentState):
    print("--- ЗАПУСК: INGESTION (Валидация и Очистка) ---")
    
    # Подготовка ПУСТОГО профиля (Обнуление)
    empty_candidate = {
        "name": "",
        "country": None,
        "city": None,
        "relocation": False,
        "grade": "",
        "specialization": "",
        "experience_years": 0.0,
        "desired_salary": None,
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
    print("🚀 Текст валиден, запускаем извлечение...")
    extracted_data = get_extraction_chain(raw_text)
    
    # объединение skills...
    all_skills = list(set(
        (extracted_data.programming_languages or []) + 
        (extracted_data.frameworks or []) + 
        (extracted_data.databases or []) + 
        (extracted_data.tools or [])
    ))

    return {
        "candidate": {
            "name": extracted_data.name,
            "country": extracted_data.country,
            "city": extracted_data.city,
            "relocation": extracted_data.relocation,
            "grade": extracted_data.grade,
            "specialization": extracted_data.specialization,
            "experience_years": extracted_data.experience_years,
            "desired_salary": extracted_data.desired_salary,
            "work_format": extracted_data.work_format,
            "foreign_languages": extracted_data.foreign_languages,
            "skills": all_skills  # Объединенный стек технологий
        },
        "next_step": "analysis",
        "error": None # Сбрасываем прошлые ошибки
    }