import logging
from typing import List, Dict, Any
# Импортируем нашу обновленную функцию
from app.agents2.tools.qdrant_tools import search_vacancies

logger = logging.getLogger(__name__)

class CareerAgent:
    def calculate_score(self, vacancy: Dict, candidate_skills: List[str]) -> float:
        vacancy_skills = set(vacancy.get("skills", []))
        candidate_skills = set(candidate_skills)

        if not vacancy_skills:
            return vacancy.get("score", 0)

        overlap = len(vacancy_skills & candidate_skills)
        # +1 во избежание деления на ноль, если список пуст
        overlap_score = overlap / (len(vacancy_skills) + 1)

        base_score = vacancy.get("score", 0)
        # Смешиваем векторную близость (60%) и соответствие скиллам (40%)
        return base_score * 0.6 + overlap_score * 0.4

    def route(self, state: Dict) -> Dict:
        # -----------------------------
        # ОПРЕДЕЛЕНИЕ ACTION
        # -----------------------------
        message = (state.get("message") or "").lower()

        if "roadmap" in message:
            action = "roadmap"
        elif any(word in message for word in ["резюме", "resume", "cv"]):
            action = "resume"
        elif "интерв" in message:
            action = "interview"
        else:
            action = "search"

        state["action"] = action

        # -----------------------------
        # ПОДГОТОВКА ДАННЫХ
        # -----------------------------
        candidate = state.get("candidate", {})
        skills = candidate.get("skills", []) or ["python"]
        city = candidate.get("city")
        # Важно: берем нормализованный город, если он есть в state
        normalized_city = candidate.get("city_normalized") or city
        relocation = candidate.get("relocation", False)
        
        user_query = state.get("message") or "Junior Machine Learning Engineer"

        # -----------------------------
        # SEARCH (ГЛАВНОЕ ИЗМЕНЕНИЕ ТУТ)
        # -----------------------------
        if action == "search":
            # Передаем ТЕКСТ, а не вектор. 
            # qdrant_tools сам векторизует его через FastEmbed.
            vacancies = search_vacancies(
                query_text=user_query,
                skills=skills,
                normalized_city=normalized_city,
                relocation=relocation,
                limit=5
            )

            # Дополнительный реранжинг внутри агента
            for v in vacancies:
                v["final_score"] = self.calculate_score(v, skills)

            vacancies = sorted(
                vacancies,
                key=lambda x: x.get("final_score", 0),
                reverse=True
            )

            state["top_vacancies"] = vacancies
            state["response"] = vacancies

        # -----------------------------
        # ОБРАБОТКА ОСТАЛЬНЫХ ACTION (Roadmap, Resume, Interview)
        # -----------------------------
        # Берем лучшую вакансию для контекста (если поиска не было, будет {})
        top_vacancies = state.get("top_vacancies") or []
        top_vacancy = top_vacancies[0] if top_vacancies else {}

        if action == "roadmap":
            vacancy_skills = set(top_vacancy.get("skills", []))
            missing = list(vacancy_skills - set(skills)) or list(vacancy_skills)
            
            roadmap = {
                skill: f"Изучи {skill} для позиции в {top_vacancy.get('company', 'компании')}."
                for skill in missing[:3]
            }
            state["roadmap"] = roadmap
            state["response"] = roadmap

        elif action == "resume":
            vacancy_skills = set(top_vacancy.get("skills", []))
            missing = list(vacancy_skills - set(skills))
            res_text = f"Для вакансии {top_vacancy.get('title')} добавь в резюме: {', '.join(missing[:3])}"
            state["custom_resume"] = res_text
            state["response"] = res_text

        elif action == "interview":
            v_skills = top_vacancy.get("skills", [])[:3] or ["Machine Learning"]
            questions = [f"Расскажи про свой опыт с {s}" for s in v_skills]
            state["mini_interview"] = questions
            state["response"] = questions

        state["last_action"] = f"Агент отработал: {action}"
        return state


# from app.agents2.tools.qdrant_tools import search_vacancies
# from sentence_transformers import SentenceTransformer

# _model = None


# def get_model():
#     global _model
#     if _model is None:
#         _model = SentenceTransformer("intfloat/multilingual-e5-large")
#     return _model


# def embed_text(text: str) -> list[float]:
#     model = get_model()

#     text = f"query: {text}"  # обязательно для e5
#     vector = model.encode(text, normalize_embeddings=True)

#     return vector.tolist()


# class CareerAgent:
#     def calculate_score(self, vacancy, candidate_skills):
#         vacancy_skills = set(vacancy.get("skills", []))
#         candidate_skills = set(candidate_skills)

#         overlap = len(vacancy_skills & candidate_skills)
#         overlap_score = overlap / (len(vacancy_skills) + 1)

#         base_score = vacancy.get("score", 0)
#         return base_score * 0.6 + overlap_score * 0.4

#     def route(self, state):
#         # -----------------------------
#         # ACTION
#         # -----------------------------
#         message = (state.get("message") or "").lower()

#         if "roadmap" in message:
#             action = "roadmap"
#         elif "резюме" in message or "resume" in message:
#             action = "resume"
#         elif "интерв" in message:
#             action = "interview"
#         else:
#             action = "search"

#         state["action"] = action

#         # -----------------------------
#         # ДАННЫЕ
#         # -----------------------------
#         candidate = state.get("candidate", {})
#         skills = candidate.get("skills", []) or ["python"]
#         city = candidate.get("city")
#         relocation = candidate.get("relocation", True)

#         # 👉 ВОТ ГЛАВНОЕ ИСПРАВЛЕНИЕ
#         user_query = state.get("message") or ""

#         # -----------------------------
#         # SEARCH
#         # -----------------------------
#         if action == "search":
#             query_vector = embed_text(user_query)

#             vacancies = search_vacancies(
#                 query_vector=query_vector,
#                 skills=skills,
#                 city=city,
#                 relocation=relocation,
#                 limit=5,
#             )

#             for v in vacancies:
#                 v["final_score"] = self.calculate_score(v, skills)

#             vacancies = sorted(
#                 vacancies,
#                 key=lambda x: x["final_score"],
#                 reverse=True
#             )

#             state["top_vacancies"] = vacancies
#             state["response"] = vacancies

#         # -----------------------------
#         # TOP VACANCY
#         # -----------------------------
#         top_vacancy = (state.get("top_vacancies") or [{}])[0]

#         # -----------------------------
#         # ROADMAP
#         # -----------------------------
#         if action == "roadmap":
#             roadmap = {}

#             vacancy_skills = set(top_vacancy.get("skills", []))
#             missing = list(vacancy_skills - set(skills)) or list(vacancy_skills)

#             for skill in missing[:3]:
#                 roadmap[skill] = (
#                     f"Освой {skill} на уровне production. "
#                     f"Сделай 1-2 проекта с использованием {skill}. "
#                     f"Подготовься к вопросам по {skill}."
#                 )

#             state["roadmap"] = roadmap
#             state["response"] = roadmap

#         # -----------------------------
#         # RESUME
#         # -----------------------------
#         elif action == "resume":
#             vacancy_skills = set(top_vacancy.get("skills", []))
#             missing = list(vacancy_skills - set(skills))

#             text = (
#                 f"Кандидат обладает навыками: {', '.join(skills)}.\n"
#                 f"Рекомендуется усилить: {', '.join(missing[:3]) if missing else 'углубить текущие навыки'}."
#             )

#             state["custom_resume"] = text
#             state["response"] = text

#         # -----------------------------
#         # INTERVIEW
#         # -----------------------------
#         elif action == "interview":
#             questions = []
#             vacancy_skills = top_vacancy.get("skills", [])[:3]

#             for skill in vacancy_skills:
#                 questions.append(f"Объясни основы {skill}")
#                 questions.append(f"Какие проекты ты делал с {skill}?")

#             if not questions:
#                 questions = [
#                     f"Объясни основы {skills[0]}",
#                     f"Какие проекты с {skills[0]}?"
#                 ]

#             state["mini_interview"] = questions[:6]
#             state["response"] = questions[:6]

#         state["last_action"] = f"Агент отработал: {action}"
#         return state