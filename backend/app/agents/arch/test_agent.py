# tests/test_agent_run.py
from pathlib import Path
from app.agents.graph import create_career_graph
from app.agents.state import AgentState, CandidateProfile, MarketContext

def test_agent_with_pdf():
    # 1. Создаём workflow
    career_graph = create_career_graph()

    # 2. Загружаем PDF
    pdf_path = Path("backend/tests/Resume_PDF.pdf")
    with open(pdf_path, "rb") as f:
        resume_bytes = f.read()

    # 3. Создаём state
    state = AgentState(
        candidate=CandidateProfile(
            name="Иван Иванов"
        ),
        market=MarketContext(
            match_score=0,
            skill_gaps=[],
            top_vacancies=[],
            salary_median=0,
            salary_top_10=0,
            market_range=[]
        ),
        raw_file_content=resume_bytes,
        file_name=pdf_path.name,
        summary=None,
        user_input=None,
        interview=None,
        tailored_resume=None,
        next_step="start",
        error=None,
        messages=[],
        stage="start"
    )

    # 4. Запускаем workflow
    state = career_graph.run(state)

    # 5. Выводим результаты
    print("\n=== Результаты тестового запуска ===")
    print("Top vacancies:", state.market.top_vacancies)
    print("Skill gaps:", state.market.skill_gaps)
    print("Salary median:", state.market.salary_median)
    print("Salary top 10:", state.market.salary_top_10)
    print("Salary range:", state.market.market_range)
    print("Summary:", state.summary)

if __name__ == "__main__":
    test_agent_with_pdf()