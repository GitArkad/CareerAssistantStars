def detect_intent(user_input: str) -> str:
    if not user_input:
        return "search"

    text = user_input.lower()

    if "собесед" in text:
        return "interview"
    if "резюме" in text:
        return "resume"
    if "roadmap" in text or "план" in text:
        return "roadmap"
    if "рынок" in text or "зарплат" in text:
        return "market"

    return "search"