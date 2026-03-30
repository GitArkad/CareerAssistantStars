def conduct_mini_interview(candidate, vacancy):
    return {
        "questions": [
            "Tell me about yourself",
            f"Explain {vacancy.get('skills', ['ML'])[0]}"
        ]
    }