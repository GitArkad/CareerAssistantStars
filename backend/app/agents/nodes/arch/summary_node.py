import os
from groq import Groq
from app.agents.state import AgentState
from langchain_core.messages import ToolMessage

# Инициализация клиента (можно вынести в отдельный конфиг)
client = Groq(
    api_key=os.getenv("GROQ_API_KEY"),
    timeout=60.0, 
    max_retries=3
              )

SUMMARY_PROMPT = """
Ты — главный аналитик карьерного центра. Твоя задача: дать краткий вердикт по текущему состоянию кандидата.
Если есть PostgresSQL или MySQL, то значит кандидат знает SQL. Не нужно это отражать. По похожим ситуациям также.
Обращайся к нему 

ВХОДНЫЕ ДАННЫЕ:
- Кандидат: {name}, Специализация: {spec}, Опыт: {experience_years}
- Навыки: {skills}
- Совпадение с рынком (Match Score): {match_score}%
- Топ вакансия: {top_vacancy_name} от {company}

ТВОЙ ОТВЕТ (Markdown):
### 📊 Общий вердикт
Коротко (2-3 предложения): насколько кандидат конкурентоспособен.
- **Сильные стороны**: перечисли 3 главных технических скилла из его стека.
- **Главный барьер**: один ключевой навык, которого не хватает для топ-вакансии.
- **Рыночная позиция**: "В рынке" / "Выше рынка" / "Нужно подтянуться".
"""

def summary_node(state: AgentState):
    print("\n--- [DEBUG] Проверка содержимого State ---")
    print(f"Ключи в стейте: {list(state.keys())}")
    
    # Проверяем конкретные важные поля
    has_candidate = "candidate" in state and bool(state["candidate"])
    has_market = "market" in state and bool(state["market"])
    
    print(f"Наличие данных кандидата: {has_candidate}")
    print(f"Наличие данных рынка: {has_market}")
    
    if has_market:
        print(f"Найдено вакансий в market: {len(state['market'].get('top_vacancies', []))}")
    # ------------------------------------------
    print("\n--- [START] STRATEGY: Generating Career Advice ---")
    
    market = state.get("market", {})
    candidate = state.get("candidate", {})
    
    # ПРОВЕРКА: Если данных нет в state, берем их принудительно (для теста)
    if not market.get("top_vacancies"):
        print("ВНИМАНИЕ: Данные рынка пусты в state!")

    # Формируем контекст максимально явно
    full_context = f"""
    КАНДИДАТ: {candidate.get('name', 'Не указано')} ({candidate.get('experience_years', 0.5)})
    ЕГО НАВЫКИ: {candidate.get('skills', [])}
    ЖЕЛАЕМАЯ З/П: {candidate.get('desired_salary', 'Не указана')}
    
    АНАЛИТИКА РЫНКА:
    - Match Score: {market.get('match_score', 0)}%
    - Медиана рынка: {market.get('salary_median', 0)}
    - Пробелы (Gaps): {market.get('skill_gaps', [])}
    
    ТОП ВАКАНСИЙ ИЗ БАЗЫ:
    {market.get('top_vacancies', [])}
    """

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SUMMARY_PROMPT},
                {"role": "user", "content": f"Проанализируй эти данные и резюме по кандидату:\n{full_context}"}
            ],
            temperature=0.3
        )
        
        advice = response.choices[0].message.content
        
        # Сохраняем совет в сообщения или отдельное поле состояния
        return {
            "summary": advice,
            # "messages": [advice],
            # "next_step": "end"
        }
        
    except Exception as e:
        print(f"Ошибка в Summary Node: {e}")
        return {"error": str(e), "next_step": "end"}