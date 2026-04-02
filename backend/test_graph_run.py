import os
from dotenv import load_dotenv

# Импортируем наш скомпилированный граф
from app.agents.graph import career_app

# Загружаем переменные окружения (API ключи)
load_dotenv()

def test_full_pipeline(input_data: str, is_file: bool = True):
    print(f"Начинаем тест. Режим файла: {is_file}")
    
    # Логика подготовки данных
    if is_file:
        try:
            with open(input_data, "rb") as f:
                content = f.read()
            filename = os.path.basename(input_data)
        except FileNotFoundError:
            print(f"Файл {input_data} не найден!")
            return
    else:
        # Если это просто текст (сообщение)
        content = input_data
        filename = "manual_input.txt"

    # Формируем начальное состояние
    initial_state = {
        "raw_file_content": content,
        "file_name": filename,
        "messages": [],
        "next_step": "" 
    }

    # 3. Запуск графа с защитой от зацикливания
    # Указываем лимит шагов (для нашей цепочки из 3 узлов 10 — с запасом)
    config = {"recursion_limit": 10}
    
    print("Граф запущен (Ingestion -> Analysis -> Summary)...")
    
    try:
        # invoke выполняет все узлы по порядку до END
        final_state = career_app.invoke(initial_state, config=config)
        
        print("\n--- ТЕСТ ЗАВЕРШЕН УСПЕШНО ---")
        
        # 4. Проверка результатов
        print("\n--- [ДАННЫЕ КАНДИДАТА] ---")
        candidate = final_state.get("candidate", {})
        print(f"Имя: {candidate.get('name')}")
        print(f"Специализация: {candidate.get('specialization')} ({candidate.get('grade')})")
        print(f"Скиллы: {candidate.get('skills')}")

        print("\n--- [АНАЛИЗ РЫНКА] ---")
        market = final_state.get("market", {})
        print(f"Match Score: {market.get('match_score')}%")
        print(f"Skill Gaps: {market.get('skill_gaps')}")
        print(f"Медиана ЗП: {market.get('salary_median')}")

        print("\n--- [ИТОГОВОЕ САММАРИ] ---")
        print(final_state.get("summary"))

    except Exception as e:
        print(f"\nПроизошла ошибка при выполнении графа: {e}")

if __name__ == "__main__":
    # Укажи путь к своему тестовому PDF
    # PATH_TO_PDF = "app/agents/test/Resume_PDF.pdf" 
    # test_full_pipeline(PATH_TO_PDF)

    # ВАРИАНТ 2: Проверка через ТЕКСТОВОЕ СООБЩЕНИЕ (как из Streamlit)
    user_message = """
    Имя: Эльбрус. 
    Специализация: ML Engineer. 
    Страна 'Russia'
    Навыки: Python, SQL, Docker, Scikit-learn.
    Опыт: 1 год разработки моделей классификации.
    """
    test_full_pipeline(user_message, is_file=False)