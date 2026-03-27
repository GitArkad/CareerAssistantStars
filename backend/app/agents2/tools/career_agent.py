from app.agents2.tools.qdrant_tools import search_vacancies

class CareerAgent:
    """
    Универсальный карьерный агент:
    - ищет вакансии
    - строит roadmap
    - делает мини-интервью
    - адаптирует резюме
    """

    # -----------------------------
    # Гибридный скоринг вакансий
    # -----------------------------
    def calculate_score(self, vacancy, candidate_skills):
        vacancy_skills = set(vacancy.get("skills", []))
        candidate_skills = set(candidate_skills)

        overlap = len(vacancy_skills & candidate_skills)
        overlap_score = overlap / (len(vacancy_skills) + 1)

        base_score = vacancy.get("score", 0)
        final_score = base_score * 0.6 + overlap_score * 0.4
        return final_score

    # -----------------------------
    # MAIN ROUTE
    # -----------------------------
    def route(self, state):
        action = state.get("action", "search")
        candidate = state.get("candidate", {})
        skills = candidate.get("skills", [])
        city = candidate.get("city")
        relocation = candidate.get("relocation", True)

        # fallback
        if not skills:
            skills = ["python"]

        # -----------------------------
        # 1. SEARCH VACANCIES
        # -----------------------------
        if action == "search":
            vacancies = search_vacancies(
                query=" ".join(skills),
                city=city,
                relocation=relocation,
                limit=5
            )
            for v in vacancies:
                v["final_score"] = self.calculate_score(v, skills)
            state["top_vacancies"] = sorted(vacancies, key=lambda x: x["final_score"], reverse=True)

        # -----------------------------
        # 2. ROADMAP
        # -----------------------------
        elif action == "roadmap":
            roadmap = {}
            top_vacancy = state.get("top_vacancies", [{}])[0]
            vacancy_skills = set(top_vacancy.get("skills", []))
            missing_skills = list(vacancy_skills - set(skills))
            if not missing_skills:
                missing_skills = list(vacancy_skills)
            for skill in missing_skills[:3]:
                roadmap[skill] = (
                    f"Освой {skill} на уровне production. "
                    f"Сделай 1-2 проекта с использованием {skill}. "
                    f"Подготовься к вопросам по {skill}."
                )
            state["roadmap"] = roadmap

        # -----------------------------
        # 3. MINI INTERVIEW
        # -----------------------------
        elif action == "interview":
            interview_questions = []
            top_vacancy = state.get("top_vacancies", [{}])[0]
            vacancy_skills = top_vacancy.get("skills", [])[:3]
            for skill in vacancy_skills:
                interview_questions.append(f"Объясни основы {skill}")
                interview_questions.append(f"Какие проекты ты делал с {skill}?")
            if not interview_questions:
                interview_questions = [f"Объясни основы {skills[0]}", f"Какие проекты с {skills[0]}?"]
            state["mini_interview"] = interview_questions[:6]

        # -----------------------------
        # 4. CUSTOM RESUME
        # -----------------------------
        elif action == "resume":
            top_vacancy = state.get("top_vacancies", [{}])[0]
            vacancy_skills = set(top_vacancy.get("skills", []))
            missing = list(vacancy_skills - set(skills))
            state["custom_resume"] = (
                f"Кандидат обладает навыками: {', '.join(skills)}.\n"
                f"Рекомендуется усилить: {', '.join(missing[:3]) if missing else 'углубить текущие навыки'}."
            )

        state["last_action"] = f"Агент отработал: {action}"
        return state


# from app.agents2.tools.qdrant_tools import search_vacancies


# class CareerAgent:
#     """
#     Универсальный карьерный агент:
#     - ищет вакансии
#     - строит roadmap
#     - делает мини интервью
#     - адаптирует резюме
#     """

#     # -----------------------------
#     # 🔥 ГИБРИДНЫЙ СКОРИНГ
#     # -----------------------------
#     def calculate_score(self, vacancy, candidate_skills):
#         vacancy_skills = set(vacancy.get("skills", []))
#         candidate_skills = set(candidate_skills)

#         # пересечение навыков
#         overlap = len(vacancy_skills & candidate_skills)

#         # нормализация
#         overlap_score = overlap / (len(vacancy_skills) + 1)

#         # базовый скор из qdrant
#         base_score = vacancy.get("score", 0)

#         # финальный скор (тюнимо)
#         final_score = base_score * 0.6 + overlap_score * 0.4

#         return final_score

#     # -----------------------------
#     # 🚀 MAIN ROUTE
#     # -----------------------------
#     def route(self, state):
#         candidate = state.get("candidate")

#         if not candidate:
#             state["last_action"] = "Нет данных кандидата"
#             return state

#         skills = candidate.get("skills", [])
#         city = candidate.get("city")
#         relocation = candidate.get("relocation", True)

#         # fallback если пусто
#         if not skills:
#             skills = ["python"]

#         # -----------------------------
#         # 🔍 Поиск вакансий
#         # -----------------------------
#         vacancies = search_vacancies(
#             query=" ".join(skills),
#             city=city,
#             relocation=relocation,
#             limit=5
#         )

#         # -----------------------------
#         # 🔥 Пересчет скоринга
#         # -----------------------------
#         for v in vacancies:
#             v["final_score"] = self.calculate_score(v, skills)

#         vacancies = sorted(vacancies, key=lambda x: x["final_score"], reverse=True)

#         state["top_vacancies"] = vacancies

#         # -----------------------------
#         # 🧠 ROADMAP (по gap)
#         # -----------------------------
#         roadmap = {}

#         if vacancies:
#             top_vacancy = vacancies[0]
#             vacancy_skills = set(top_vacancy.get("skills", []))
#             candidate_skills = set(skills)

#             missing_skills = list(vacancy_skills - candidate_skills)

#             # fallback если всё совпадает
#             if not missing_skills:
#                 missing_skills = list(vacancy_skills)

#             for skill in missing_skills[:3]:
#                 roadmap[skill] = (
#                     f"Освой {skill} на уровне production. "
#                     f"Сделай 1-2 проекта с использованием {skill}. "
#                     f"Подготовься к вопросам по {skill} для собеседований."
#                 )

#         state["roadmap"] = roadmap

#         # -----------------------------
#         # 🎤 MINI INTERVIEW (по вакансии)
#         # -----------------------------
#         interview_questions = []

#         if vacancies:
#             top_vacancy = vacancies[0]
#             vacancy_skills = top_vacancy.get("skills", [])

#             for skill in vacancy_skills[:3]:
#                 interview_questions.append(f"Объясни основы {skill}")
#                 interview_questions.append(f"Какие проекты ты делал с {skill}?")

#         # fallback
#         if not interview_questions:
#             interview_questions = [
#                 f"Объясни основы {skills[0]}",
#                 f"Какие проекты ты делал с {skills[0]}?",
#             ]

#         state["mini_interview"] = interview_questions[:6]

#         # -----------------------------
#         # 📄 CUSTOM RESUME
#         # -----------------------------
#         if vacancies:
#             top_vacancy = vacancies[0]
#             vacancy_skills = set(top_vacancy.get("skills", []))
#             candidate_skills = set(skills)

#             missing = list(vacancy_skills - candidate_skills)

#             state["custom_resume"] = (
#                 f"Кандидат обладает навыками: {', '.join(skills)}.\n"
#                 f"Хорошее совпадение с вакансией.\n"
#                 f"Рекомендуется усилить: {', '.join(missing[:3]) if missing else 'углубить текущие навыки'}."
#             )
#         else:
#             state["custom_resume"] = (
#                 f"Кандидат обладает навыками: {', '.join(skills)}."
#             )

#         state["last_action"] = "Агент полностью отработал"

#         return state