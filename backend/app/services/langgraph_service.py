def run_analysis(profile: dict):
    """
    Временная заглушка.
    Потом здесь будет вызов реального LangGraph.
    """

    return {
        "candidate": {
            "name": profile.get("name"),
            "grade": profile.get("grade"),
            "specialization": profile.get("specialization"),
        },
        "match_score": 0.78,
        "missing_skills": ["Docker", "Kubernetes"],
        "top_jobs": [
            {
                "job_id": "1",
                "title": "Backend Python Developer",
                "company": "TechCorp",
                "score": 0.91
            }
        ],
        "recommendations": [
            "Добавь Docker в стек",
            "Уточни опыт работы с PostgreSQL",
            "Выдели API-проекты в резюме"
        ]
    }