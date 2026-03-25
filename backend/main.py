import os
import asyncio
from langchain_core.messages import HumanMessage
# Убедись, что в graph.py career_app скомпилирован с MemorySaver!
from app.agents.graph import career_app

async def run_career_assistant():
    print("Запуск Career AI Assistant (Memory Mode)")
    
    # 1. Читаем реальный файл Resume_PDF.pdf
    file_path = "Resume_PDF.pdf" 
    
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден в папке backend!")
        return

    # Читаем байты файла
    with open(file_path, "rb") as f:
        pdf_bytes = f.read()

    # Формируем начальное состояние с реальными данными
    initial_input = {
        "messages": [HumanMessage(content="Проанализируй моё резюме")], 
        "raw_file_content": pdf_bytes, # ТЕПЕРЬ ТУТ РЕАЛЬНЫЙ PDF
        "file_name": file_path,
        "candidate": {}, 
        "market": {}
    }

    config = {"configurable": {"thread_id": "andrey_session_1"}}

    print(f"--- Шаг 1: Анализ резюме '{file_path}' ---")
    current_state = await career_app.ainvoke(initial_input, config=config)
    
    # Печатаем отчет из summary_node
    if "summary" in current_state:
        print(f"\n[AGENT]: {current_state['summary']}")

    # 2. ЦИКЛ ДИАЛОГА (Чат)
    while True:
        try:
            user_text = input("\n[YOU]: ")
            if user_text.lower() in ["exit", "quit", "стоп", "пока"]:
                print("До связи!")
                break

            # Отправляем ТОЛЬКО новое сообщение. 
            # Благодаря thread_id граф НЕ пойдет в ingestion/analysis снова.
            input_data = {"messages": [HumanMessage(content=user_text)]}
            
            # Запускаем граф. Он подхватит историю из памяти.
            output = await career_app.ainvoke(input_data, config=config)

            # ЛОГИКА ВЫВОДА: ищем последний текстовый ответ от AI
            all_messages = output.get("messages", [])
            found_reply = False
            
            for msg in reversed(all_messages):
                # Нам нужно именно сообщение от ассистента с текстом
                if msg.type == "ai" and msg.content.strip():
                    print(f"\n[AGENT]: {msg.content}")
                    found_reply = True
                    break
            
            if not found_reply:
                print("\n[SYSTEM]: Агент думает или выполняет задачу...")

        except Exception as e:
            print(f"\n[ERROR]: Произошла ошибка: {e}")
            break

if __name__ == "__main__":
    # Проверка переменных окружения перед стартом
    if not os.getenv("GROQ_API_KEY"):
        print("❌ Ошибка: GROQ_API_KEY не найден в переменных окружения!")
    else:
        try:
            asyncio.run(run_career_assistant())
        except KeyboardInterrupt:
            print("\nСессия завершена пользователем.")