from langchain_core.messages import AIMessage
from app.agents.state import AgentState


def summary_node(state: AgentState):
    print("\n--- [SUMMARY NODE] ---")

    candidate = state.get("candidate", {})
    market = state.get("market", {})

    report = f"""
✅ Резюме проанализировано!

Кандидат: {candidate.get('name', 'Кандидат')}
Специализация: {candidate.get('specialization', '—')}
Опыт: {candidate.get('experience_years', 0)} лет
Match Score: {market.get('match_score', 0)}%

🧠 Навыки:
{", ".join(candidate.get('skills', [])[:10])}

⚠️ Пробелы:
{", ".join(market.get('skill_gaps', [])[:5]) or "не выявлены"}

🔍 Топ вакансии:
"""

    # Добавляем реальные вакансии из анализа
    for v in market.get('top_vacancies', [])[:3]:
        report += f"- {v['title']} в {v['company']} ({v['match_score']}%)\n"

    return {
        "messages": [AIMessage(content=report)],

        # 👇 КРИТИЧНО: прокидываем данные дальше
        "market": market,
        "candidate": candidate,

        "stage": "chat",
        "just_summarized": True
        
    }