import os
from groq import Groq
from app.agents.state import AgentState

# Инициализация клиента (можно вынести в отдельный конфиг)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

STRATEGY_PROMPT = """
Ты — экспертный IT-карьерный коуч и технический рекрутер. 
Твоя задача: проанализировать данные кандидата и рыночную ситуацию. Укажи на слабые стороны, развитие которых позолить вырасти как специалисту.
Укажи вакансии, на которые можно откликнуться сейчас, и вакансии, для которых нужно дополнительно обучиться.

ВХОДНЫЕ ДАННЫЕ:
1. Профиль кандидата: {candidate_info}
2. Найденные вакансии: {top_vacancies}
3. Аналитика рынка: {market_stats}

Пиши профессионально, честно, коротко, по делу. Используй Markdown для форматирования.
"""

# ТВОЙ ОТВЕТ ДОЛЖЕН СОДЕРЖАТЬ:
# - Резюме (Summary): Насколько кандидат "в рынке" (используй Match Score).
# - Gap Analysis: Какие 2-3 навыка критично подтянуть прямо сейчас (из списка skill_gaps).
# - Зарплатная стратегия: Адекватны ли запросы кандидата медиане рынка?
# - Список целей: Какие из найденных компаний (Ozon, SberAI и т.д.) наиболее перспективны для него.
# - Конкретный "Action Plan": Что сделать в ближайшие 7 дней. Добавь на какую зарплату можно претендовать, если будут изучены отсутствующие скилы

def strategy_node(state: AgentState):
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
        print("⚠️ ВНИМАНИЕ: Данные рынка пусты в state!")

    # Формируем контекст максимально явно
    full_context = f"""
    КАНДИДАТ: {candidate.get('name', 'Не указано')} ({candidate.get('grade', 'Junior')})
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
                {"role": "system", "content": STRATEGY_PROMPT},
                {"role": "user", "content": f"Проанализируй эти данные и дай совет:\n{full_context}"}
            ],
            temperature=0.7
        )
        
        advice = response.choices[0].message.content
        
        # Сохраняем совет в сообщения или отдельное поле состояния
        return {
            "messages": [advice],
            "next_step": "end"
        }
        
    except Exception as e:
        print(f"❌ Ошибка в Strategy Node: {e}")
        return {"error": str(e), "next_step": "end"}