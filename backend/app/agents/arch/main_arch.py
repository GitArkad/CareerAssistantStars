# main.py
import os
import asyncio
from langchain_core.messages import HumanMessage
# Убедись, что в graph.py career_app скомпилирован с MemorySaver!
from app.agents.graph import career_app

async def run_career_assistant():
    print("Запуск Career AI Assistant (Memory Mode)")
    config = {"configurable": {"thread_id": "andrey_session_1"}}

    print("Напиши: 'загрузить резюме' или вставь текст")

    file_loaded = False  # ✅ теперь на месте

    while True:
        user_text = input("\n[YOU]: ")

        if user_text.lower() in ["exit", "quit", "стоп", "пока"]:
            break

        input_data = {
            "messages": [HumanMessage(content=user_text)]
        }

        # 👉 загрузка файла по триггеру
        if "резюме" in user_text.lower() and not file_loaded:
            file_path = "Resume_PDF.pdf"

            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    pdf_bytes = f.read()

                input_data["raw_file_content"] = pdf_bytes
                input_data["file_name"] = file_path

                file_loaded = True
            else:
                print("[SYSTEM]: файл не найден")
                continue

        output = await career_app.ainvoke(input_data, config=config)

        # вывод ответа
        for msg in reversed(output.get("messages", [])):
            if msg.type == "ai" and msg.content.strip():
                print(f"\n[AGENT]: {msg.content}")
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