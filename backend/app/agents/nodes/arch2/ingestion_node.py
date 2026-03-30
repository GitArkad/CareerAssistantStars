
from langchain_core.messages import AIMessage
from app.agents.state import AgentState, CandidateProfile, MarketContext
from app.agents.services.parser import ResumeParser
from app.agents.services.qdrant_service import search_vacancies
from app.agents.utils import get_extraction_chain
from app.agents.services.taxonomy import IT_DS_TAXONOMY

def get_default_candidate() -> CandidateProfile:
    return {
        "name": "Кандидат", "country": None, "city": None, "relocation": False,
        "grade": "Junior", "specialization": "ML Engineer", "experience_years": 0.5,
        "desired_salary": 0, "work_format": [], "foreign_languages": [], "skills": []
    }

def get_default_market() -> MarketContext:
    return {"match_score": 0.0, "skill_gaps": [], "top_vacancies": [], "salary_median": 0, "salary_top_10": 0, "market_range": [0, 0]}

def ingestion_node(state: AgentState):
    print("\n--- [INGESTION] ---")
    
    raw_file = state.get("raw_file_content")
    file_name = state.get("file_name", "resume.pdf")
    
    # Парсинг
    if raw_file:
        raw_text = ResumeParser.parse(raw_file, file_name)
    else:
        messages = state.get("messages", [])
        raw_text = messages[-1].content if messages else ""
    
    if not raw_text or len(raw_text) < 100:
        return {"messages": [AIMessage(content="❌ Текст слишком короткий (<100 символов)")],
                "stage": "agent", "candidate": get_default_candidate(), "market": get_default_market()}
    
    # Извлечение данных
    try:
        extracted = get_extraction_chain(raw_text)
    except Exception as e:
        return {"messages": [AIMessage(content=f"❌ Ошибка: {str(e)[:100]}")],
                "stage": "agent", "candidate": get_default_candidate(), "market": get_default_market()}
    
    skills = set(extracted.skills or [])
    for parent, children in IT_DS_TAXONOMY.items():
        if any(c.lower() in raw_text.lower() for c in children):
            skills.add(parent)
    
    candidate: CandidateProfile = {
        "name": extracted.name or "Кандидат",
        "country": getattr(extracted, 'country', None),
        "city": getattr(extracted, 'city', None),
        "relocation": bool(getattr(extracted, 'relocation', False)),
        "grade": getattr(extracted, 'grade', None) or "Junior",
        "specialization": extracted.specialization or "ML Engineer",
        "experience_years": float(extracted.experience_years) if extracted.experience_years else 0.5,
        "desired_salary": int(extracted.desired_salary) if getattr(extracted, 'desired_salary', None) else 0,
        "work_format": getattr(extracted, 'work_format', []) or [],
        "foreign_languages": getattr(extracted, 'foreign_languages', []) or [],
        "skills": list(skills)
    }
    
    print(f"✅ Кандидат: {candidate['name']}, {candidate['specialization']}, {len(candidate['skills'])} навыков")
    
    # 🔥 ПОИСК ВАКАНСИЙ (твоя рабочая функция)
    market: MarketContext = search_vacancies(
        specialization=candidate["specialization"],
        skills=candidate["skills"],
        city=candidate["city"],
        country=candidate["country"],
        limit=5
    )
    
    print(f"🔍 Market: {len(market.get('top_vacancies', []))} вакансий из базы")
    
    # Саммари
    report = f"""✅ **Резюме проанализировано!**

👤 {candidate['name']} | {candidate['specialization']} | {candidate['grade']}
📊 Опыт: {candidate['experience_years']} лет | Match: {market['match_score']}%

🧠 Навыки: {", ".join(candidate['skills'][:10])}
⚠️ Пробелы: {", ".join(market['skill_gaps'][:5]) or 'не выявлены'}

💰 Зарплаты: Медиана {market['salary_median']} ₽ | Топ-10% {market['salary_top_10']} ₽

🔍 Топ вакансии:
"""
    for v in market['top_vacancies'][:3]:
        salary = f"{v.get('salary_from', '?')}-{v.get('salary_to', '?')}" if v.get('salary_from') else 'не указана'
        report += f"- {v['title']} в {v['company']} ({v['match_score']}%) | ЗП: {salary}\n"
    
    return {
        "messages": [AIMessage(content=report)],
        "candidate": candidate,
        "market": market,
        "stage": "agent",
        "error": None
    }
