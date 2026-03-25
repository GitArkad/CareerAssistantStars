import asyncio
import os
from app.agents.nodes.ingestion_node import ingestion_node
from app.agents.nodes.analysis_node import analysis_node
from app.agents.nodes.summary_node import summary_node

async def run_test():
    print("🚀 НАЧАЛО ТЕСТА: AI Career Assistant\n")

    # 1. Формируем путь
    base_path = os.path.dirname(os.path.abspath(__file__))
    pdf_path = os.path.join(base_path, "app/agents/test/Resume_PDF.pdf")
    
    print(f"DEBUG: Пытаюсь открыть файл по пути: {pdf_path}")

    # 2. ЧИТАЕМ ФАЙЛ В БАЙТЫ (Важно!)
    if not os.path.exists(pdf_path):
        print(f"❌ ОШИБКА: Файл не найден по пути {pdf_path}")
        return

    with open(pdf_path, "rb") as f:
        file_bytes = f.read()
    
    print(f"DEBUG: Файл прочитан успешно, размер: {len(file_bytes)} байт")

    initial_state = {
        "raw_file_content": file_bytes,  # ТЕПЕРЬ ТУТ РЕАЛЬНЫЕ БАЙТЫ
        "file_name": "Resume_PDF.pdf",
        "candidate": {},
        "market": {},
        "messages": [],
        "next_step": "",
        "error": None
    }

    # 3. Шаг 1: Ingestion
    print("--- Шаг 1: Извлечение данных из резюме ---")
    ingestion_result = ingestion_node(initial_state)
    
    if ingestion_result.get("error"):
        print(f"❌ Ошибка на этапе Ingestion: {ingestion_result['error']}")
        return

    # Обновляем состояние
    state = {**initial_state, **ingestion_result}
    print(f"✅ Данные извлечены для: {state['candidate'].get('name')}")
    print(f"📊 Грейд: {state['candidate'].get('grade')}")

    # 4. Шаг 2: Analysis
    print("\n--- Шаг 2: Поиск в Qdrant ---")
    analysis_result = analysis_node(state)
    
    if analysis_result.get("error"):
        print(f"❌ Ошибка на этапе Analysis: {analysis_result['error']}")
    else:
        print(f"✅ Успех! Найдено вакансий: {len(analysis_result['market']['top_vacancies'])}")
    
    state = {**state, **analysis_result}


    # 5. Шаг 3: Strategy (Генерация совета)
    print("\n--- Шаг 3: Формирование стратегии ---")
    summary_result = summary_node(state)

    if "messages" in summary_result:
        print("\n📝 ФИНАЛЬНЫЙ СОВЕТ СИСТЕМЫ:")
        print("-" * 30)
        print(summary_result["messages"][0])
        print("-" * 30)
    

if __name__ == "__main__":
    asyncio.run(run_test())