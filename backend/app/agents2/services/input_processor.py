from app.agents2.services.resume_parser import parse_pdf

class InputProcessor:
    """
    Обработка входа пользователя:
    - PDF → candidate
    - Message → action / добавление скилов
    """

    def process(self, message=None, file_bytes=None, state=None):
        state = state or {}

        # -------------------------
        # FILE → parse resume
        # -------------------------
        if file_bytes and not state.get("candidate"):
            parsed = parse_pdf(file_bytes, True)
            if isinstance(parsed, str):
                parsed = {
                    "skills": parsed.lower().split()[:10],
                    "city": None,
                    "relocation": True
                }
            state["candidate"] = parsed
            # не ставим action, пусть сообщение решает
            # state["action"] = "search"

        # -------------------------
        # MESSAGE → intent
        # -------------------------
        if message:
            msg = message.lower()

            if "roadmap" in msg:
                state["action"] = "roadmap"
            elif "интерв" in msg:
                state["action"] = "interview"
            elif "резюме" in msg:
                state["action"] = "resume"
            elif "ваканс" in msg or "job" in msg:
                state["action"] = "search"

            # добавление скиллов
            if "добавь" in msg:
                skills = self.extract_skills(msg)
                candidate = state.get("candidate", {})
                current = set(candidate.get("skills", []))
                current.update(skills)
                candidate["skills"] = list(current)
                state["candidate"] = candidate

        return state

    def extract_skills(self, text):
        known = ["python", "sql", "docker", "pytorch", "kubernetes", "airflow", "spark"]
        return [s for s in known if s in text]