from app.agents2.services.vacancy_search import search_vacancies


class CareerAgent:

    def route(self, state):
        action = state.get("action")
        candidate = state.get("candidate", {})

        skills = candidate.get("skills", [])
        city = candidate.get("city")
        relocation = candidate.get("relocation", True)

        # -----------------------------
        # 🔍 SEARCH
        # -----------------------------
        if action == "search":
            vacancies = search_vacancies(
                query=" ".join(skills),
                city=city,
                relocation=relocation,
                limit=5
            )

            state["top_vacancies"] = vacancies

        # -----------------------------
        # 🧠 ROADMAP (<= 3 skills)
        # -----------------------------
        elif action == "roadmap":
            top_skills = skills[:3] if skills else ["python"]

            roadmap = {}
            for skill in top_skills:
                roadmap[skill] = (
                    f"Изучи продвинутый {skill}. "
                    f"Сделай 2 проекта. "
                    f"Разбери реальные кейсы."
                )

            state["roadmap"] = roadmap

        # -----------------------------
        # 🎤 INTERVIEW (6 вопросов)
        # -----------------------------
        elif action == "interview":
            top_skills = skills[:3] if skills else ["python"]

            questions = []
            for skill in top_skills:
                questions.append(f"Объясни основы {skill}")
                questions.append(f"Какие проекты ты делал с {skill}?")

            state["mini_interview"] = questions[:6]

        # -----------------------------
        # 📄 RESUME
        # -----------------------------
        elif action == "resume":
            state["custom_resume"] = (
                f"Кандидат обладает навыками: {', '.join(skills)}. "
                f"Рекомендуется усилить профиль под вакансии."
            )

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