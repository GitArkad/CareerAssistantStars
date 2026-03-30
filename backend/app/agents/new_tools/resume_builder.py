def build_resume(candidate, vacancy):
    return {
        "target_role": vacancy.get("title"),
        "skills": candidate.get("skills", [])
    }