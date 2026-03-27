import os
from dotenv import load_dotenv
from groq import Groq
from app.agents.state import AgentState

load_dotenv()

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def roadmap_node(state: AgentState):
    print("\n--- [START] ROADMAP & SALARY PREDICTION ---")
    
    candidate = state.get("candidate", {})
    market = state.get("market", {})
    
    # Собираем контекст для LLM
    gaps = market.get("skill_gaps", [])
    current_salary = candidate.get("desired_salary", 0)
    median_market = market.get("salary_median", 0)

    prompt = f"""
    Ты — карьерный коуч для {candidate.get('specialization')}.
    Основываясь на пробелах в навыках ({', '.join(gaps)}) и медиане рынка ({median_market}), 
    сделай две вещи:
    1. Составь пошаговый Roadmap обучения (3-4 шага).
    2. Предскажи потенциал роста зарплаты после освоения этих навыков.
    3. Покажи какие вакансии откроются после освоения навыков.
    
    Пиши кратко, профессионально, используя Markdown.
    """

    response = client.chat.completions.create(
        model=GROQ_MODEL, # Для качественного планирования лучше модель помощнее
        messages=[{"role": "user", "content": prompt}]
    )

    return {
        "roadmap": response.choices[0].message.content,
        "next_step": "end"
    }