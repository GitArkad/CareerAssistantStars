# career_agent.py

import logging
from typing import List, Dict

from app.agents2.tools.qdrant_tools import search_vacancies
from app.agents2.tools.market_tools import get_market_context
from app.agents2.llm_client import run_local_llm

logger = logging.getLogger(__name__)


class CareerAgent:

    # -----------------------------
    # SCORING
    # -----------------------------
    def calculate_score(self, vacancy: Dict, candidate_skills: List[str]) -> float:
        vacancy_skills = set(s.lower() for s in vacancy.get("skills", []))
        candidate_skills = set(s.lower() for s in candidate_skills)

        if not vacancy_skills:
            return vacancy.get("score", 0)

        overlap = len(vacancy_skills & candidate_skills)
        overlap_score = overlap / (len(vacancy_skills) + 1)

        base_score = vacancy.get("score", 0)
        return base_score * 0.6 + overlap_score * 0.4
    
    def handle_vacancy_choice(self, state: Dict) -> Dict:
        print("✅ INSIDE HANDLE VACANCY CHOICE")
        message = state.get("message", "")
        top_vacancies = state.get("top_vacancies", [])
        
        # Используем твой экстрактор
        idx = extract_number(message)
        
        if idx is not None and 0 < idx <= len(top_vacancies):
            choice_idx = idx - 1
            selected = top_vacancies[choice_idx]
            
            print(f"✅ SELECTED VACANCY: {selected['title']} at {selected['company']}")
            
            state["selected_vacancy"] = selected
            state["action"] = "resume"  # Переключаем на режим резюме
            state["stage"] = "generating_resume" # Меняем стейт
            
            # ВАЖНО: чтобы не ждать следующего запроса от пользователя, 
            # мы рекурсивно вызываем route, но уже с новым action="resume"
            return self.route(state)
            
        state["response"] = "Пожалуйста, введите корректный номер вакансии (1, 2, 3...)"
        return state


    def llm_parse(self, message: str) -> dict:
        prompt = f"""
            Верни JSON: {{"vacancy_index": number или null}}

            Сообщение: {message}
            """
        # prompt = f"""
        #     Ты парсер пользовательского ввода.

        #     Задача:
        #     Определи, хочет ли пользователь выбрать вакансию по номеру.

        #     Ответь строго JSON без пояснений.

        #     Формат:
        #     {{
        #     "vacancy_index": number | null
        #     }}

        #     Примеры:
        #     "2" → {{ "vacancy_index": 2 }}
        #     "вторая" → {{ "vacancy_index": 2 }}
        #     "возьми третью" → {{ "vacancy_index": 3 }}
        #     "не знаю" → {{ "vacancy_index": null }}

        #     Сообщение:
        #     {message}
        #     """

        try:
            response = run_local_llm(prompt)
            return json_safe_load(response)
        except Exception:
            return {}


    # -----------------------------
    # MAIN ROUTE
    # -----------------------------
    def route(self, state: Dict) -> Dict:
        message = (state.get("message") or "").strip().lower()
        stage = (state.get("stage") or "").strip()
        
        print(f"DEBUG AGENT: stage='{stage}', action='{state.get('action')}'")

        # --- КРИТИЧЕСКАЯ ПРАВКА ---
        # Если мы ждем выбора, не даем коду идти дальше к логике переопределения action
        if stage == "waiting_vacancy_choice":
            print("👉 AGENT: handling vacancy choice")
            return self.handle_vacancy_choice(state)
        # --------------------------

        action = state.get("action", "search")
        
        # Вторичная защита от смены action на цифрах
        if not message.isdigit():
            if "resume" in message:
                action = "resume"
            elif "roadmap" in message:
                action = "roadmap"
            elif "interview" in message:
                action = "interview"
            # Если это просто текст, который не совпал с командами — оставляем как есть или search
        
        state["action"] = action

        # message = (state.get("message") or "").lower()
        # stage = (state.get("stage") or "").strip()
        # print(stage)
        # if stage == "waiting_vacancy_choice":
        #     print("👉 HANDLE VACANCY CHOICE TRIGGERED")  # debug
        #     return self.handle_vacancy_choice(state)
        
        # action = state.get("action", "search")
        # print(f"DEBUG action до условия {action}")
        # if not message.isdigit():   # ← ВАЖНО
        #     if "resume" in message:
        #         action = "resume"
        #     elif "roadmap" in message:
        #         action = "roadmap"
        #     else:
        #         action = "search"
        # print(f"DEBUG action после условия {action}")

        # state["action"] = action

        candidate = state.get("candidate", {})
        skills = candidate.get("skills", []) or ["python"]

        city = candidate.get("city")
        normalized_city = candidate.get("city_normalized") or city
        relocation = candidate.get("relocation", False)

        user_query = state.get("message") or "Junior ML Engineer"

        # -----------------------------
        # SEARCH
        # -----------------------------
        if action == "search":
            vacancies = search_vacancies(
                query_text=user_query,
                skills=skills,
                normalized_city=normalized_city,
                relocation=relocation,
                limit=5
            )

            for v in vacancies:
                v["final_score"] = self.calculate_score(v, skills)

            vacancies = sorted(
                vacancies,
                key=lambda x: x.get("final_score", 0),
                reverse=True
            )

            state["top_vacancies"] = vacancies

            if vacancies:
                state["stage"] = "waiting_vacancy_choice"
            else:
                state["stage"] = "idle"

            market_context = get_market_context(
                vacancies=vacancies,
                user_skills=skills,
            )

            state["market"] = market_context
            state["response"] = vacancies
            state["last_action"] = "Агент отработал: search"

            return state

        # -----------------------------
        # ROADMAP
        # -----------------------------
        if action == "roadmap":

            # 🔥 AUTO SEARCH
            if not state.get("market") or not state.get("top_vacancies"):
                print("⚠️ AUTO SEARCH TRIGGERED")

                vacancies = search_vacancies(
                    query_text=user_query,
                    skills=skills,
                    normalized_city=normalized_city,
                    relocation=relocation,
                    limit=5
                )

                for v in vacancies:
                    v["final_score"] = self.calculate_score(v, skills)

                vacancies = sorted(
                    vacancies,
                    key=lambda x: x.get("final_score", 0),
                    reverse=True
                )

                state["top_vacancies"] = vacancies
                state["market"] = get_market_context(vacancies, skills)

            market = state.get("market", {})
            market_gaps = market.get("skill_gaps", [])

            top_vacancies = state.get("top_vacancies") or []

            # -----------------------------
            # SKILL GAP по всем вакансиям
            # -----------------------------
            all_vacancy_skills = set()

            for v in top_vacancies:
                for s in v.get("skills", []):
                    all_vacancy_skills.add(s.lower())

            candidate_skills = set(s.lower() for s in skills)

            vacancy_missing = list(all_vacancy_skills - candidate_skills)

            # приоритет рынку
            missing = market_gaps or vacancy_missing

            if not missing:
                missing = ["Machine Learning", "System Design", "MLOps"]

            # -----------------------------
            # MARKET SKILLS
            # -----------------------------
            market_skills = list({
                skill
                for v in top_vacancies
                for skill in v.get("skills", [])
            })

            # # -----------------------------
            # # LLM
            # # -----------------------------
            # if missing:
            #     prompt = f"""
            #     Ты карьерный консультант.

            #     У кандидата НЕ хватает навыков:
            #     {missing}

            #     Вот требования рынка:
            #     {market_skills}

            #     Твоя задача:
            #     - выбрать 2-3 самых важных навыка из missing
            #     - объяснить КРАТКО зачем каждый нужен

            #     Формат ответа:
            #     Каждый навык с новой строки:
            #     Навык — короткое объяснение (5-10 слов)

            #     Пример:
            #     Docker — используется для контейнеризации приложений  
            #     Kubernetes — нужен для оркестрации сервисов  
            #     Airflow — управление пайплайнами данных  

            #     Нельзя:
            #     - длинные тексты
            #     - абзацы
            #     - лишние слова
            #     - навыки вне missing

            #     Ответ:
            #     """
            # else:
            #     prompt = f"""
            #     Ты карьерный консультант.

            #     У кандидата есть навыки:
            #     {skills}

            #     Вот требования рынка:
            #     {market_skills}

            #     Твоя задача:
            #     - предложить 2-3 новых навыка
            #     - не повторять текущие навыки
            #     - объяснить кратко зачем каждый нужен

            #     Формат ответа:
            #     Каждый навык с новой строки:
            #     Навык — короткое объяснение (5-10 слов)

            #     Пример:
            #     MLflow — трекинг ML экспериментов  
            #     Kubernetes — деплой и масштабирование моделей  
            #     Airflow — автоматизация data pipeline  

            #     Нельзя:
            #     - длинные тексты
            #     - абзацы
            #     - лишний текст

            #     Ответ:
            #     """



            # roadmap = run_local_llm(prompt, use_smart_model=True)
                       

            roadmap = {
                skill: f"Изучи {skill} и сделай 1-2 проекта"
                for skill in missing[:5]
            }

            if isinstance(roadmap, dict):
                # если это dict — превращаем в строку
                roadmap = "\n".join([
                    f"{k} — {v}" if isinstance(v, str) else str(v)
                    for k, v in roadmap.items()
                ])

            elif not isinstance(roadmap, str):
                roadmap = str(roadmap)

            lines = roadmap.split("\n")
            cleaned = [l.strip() for l in lines if "—" in l]

            if len(cleaned) < 2:
                cleaned = [f"{s} — востребован на рынке" for s in market_skills[:3]]

            roadmap = "\n".join(cleaned)

            # если выдал  ответ пустой или меньше 10 символов
            if not roadmap or len(roadmap) < 10:
                roadmap = "Рекомендуется изучить ключевые навыки из вакансий: " + ", ".join(market_skills[:3])

            print(f"РОАДМАП {roadmap}")

            state["roadmap"] = roadmap
            state["response"] = roadmap
            state["last_action"] = "Агент отработал: roadmap"

            return state

        # -----------------------------
        # RESUME
        # -----------------------------
        if action == "resume":
            print("ROUTE VERSION 2")

            top_vacancies = state.get("top_vacancies", [])
            # resume_skills = state.get("resume_skills")
            resume_skills = state.get("resume_skills") or candidate.get("skills")
            stage = state.get("stage")
            message = (state.get("message") or "").strip()

            selected = state.get("selected_vacancy")
            print("DEBUG SELECTED:", state.get("selected_vacancy"))

            if selected:
                prompt = f"""
                Ты карьерный консультант.

                Навыки кандидата:
                {resume_skills}

                Вакансия:
                {selected}

                Сделай резюме:
                - под требования вакансии
                - кратко
                - структурировано
                """

                resume = run_local_llm(prompt)

                if not isinstance(resume, str):
                    resume = str(resume)

                state["custom_resume"] = resume
                state["response"] = resume
                state["stage"] = "resume_ready"
                state["selected_vacancy"] = None  # 🔥 фикс

                return state

            # -----------------------------
            # CASE 1: НЕТ РЕЗЮМЕ
            # -----------------------------
            if not resume_skills:
                state["response"] = (
                    "Чтобы сформировать резюме, загрузите файл с резюме."
                )
                state["stage"] = "waiting_resume"
                return state

            # -----------------------------
            # AUTO SEARCH (если есть резюме, но нет вакансий)
            # -----------------------------
            if not top_vacancies:
                vacancies = search_vacancies(
                    query_text=user_query,
                    skills=skills,
                    normalized_city=normalized_city,
                    relocation=relocation,
                    limit=5
                )

                for v in vacancies:
                    v["final_score"] = self.calculate_score(v, skills)

                vacancies = sorted(
                    vacancies,
                    key=lambda x: x.get("final_score", 0),
                    reverse=True
                )

                state["top_vacancies"] = vacancies
                top_vacancies = vacancies

            # -----------------------------
            # CASE 2: есть резюме, но вакансий нет
            # -----------------------------
            if not top_vacancies:
                prompt = f"""
                    Ты карьерный консультант.

                    Навыки кандидата:
                    {resume_skills}

                    Сформируй улучшенное резюме:
                    - кратко
                    - структурировано
                    """

                resume = run_local_llm(prompt)

                if not isinstance(resume, str):
                    resume = str(resume)

                state["custom_resume"] = resume
                state["response"] = resume
                state["stage"] = "resume_ready"
                return state

            # -----------------------------
            # CASE: выбор вакансии
            # -----------------------------
            if stage == "waiting_vacancy_choice":

                idx = extract_number(message)

                # fallback на LLM
                if idx is None and len(message) > 3:
                    parsed = self.llm_parse(message)
                    idx = parsed.get("vacancy_index")

                if idx is not None and idx > 0:
                    choice = idx - 1

                    if 0 <= choice < len(top_vacancies):
                        vacancy = top_vacancies[choice]
                        state["selected_vacancy"] = vacancy

                        prompt = f"""
                            Ты карьерный консультант.

                            Навыки кандидата:
                            {resume_skills}

                            Вакансия:
                            {vacancy}

                            Сделай резюме:
                            - под требования вакансии
                            - кратко
                            - структурировано
                            """

                        resume = run_local_llm(prompt)

                        if not isinstance(resume, str):
                            resume = str(resume)

                        state["custom_resume"] = resume
                        state["response"] = resume
                        state["stage"] = "resume_ready"

                    else:
                        state["response"] = (
                            f"В списке только {len(top_vacancies)} вакансий.\n"
                            f"Выберите номер от 1 до {len(top_vacancies)}."
                        )

                else:
                    state["response"] = (
                        "Не понял выбор.\n"
                        "Напишите номер вакансии (например: 1 или 2)"
                    )

                return state

            # -----------------------------
            # CASE 3: есть всё → предлагаем выбор
            # -----------------------------
            state["stage"] = "waiting_vacancy_choice"

            state["response"] = {
                "message": "Выберите вакансию:",
                "vacancies": [
                    {"id": i + 1, "title": v.get("title")}
                    for i, v in enumerate(top_vacancies[:5])
                ]
            }

            return state

        # -----------------------------
        # INTERVIEW
        # -----------------------------
        if action == "interview":

            # 🔥 AUTO SEARCH
            if not state.get("market"):
                vacancies = search_vacancies(
                    query_text=user_query,
                    skills=skills,
                    normalized_city=normalized_city,
                    relocation=relocation,
                    limit=5
                )

                state["top_vacancies"] = vacancies
                state["market"] = get_market_context(vacancies, skills)

            market = state.get("market", {})
            skills_focus = market.get("skill_gaps", [])

            if not skills_focus:
                skills_focus = skills[:2] or ["Python"]

            questions = [
                f"Расскажи про опыт с {skill}"
                for skill in skills_focus[:5]
            ]

            state["mini_interview"] = questions
            state["response"] = questions
            state["last_action"] = "Агент отработал: interview"

            return state

        return state
    
# =========================================
# 🔧 HELPERS
# =========================================

import re
import json


def extract_number(text: str):
    text = text.lower()

    # цифры
    match = re.search(r"\d+", text)
    if match:
        return int(match.group())

    # слова
    mapping = {
        "первая": 1,
        "вторая": 2,
        "третья": 3,
        "четвертая": 4,
        "пятая": 5,
    }

    for word, num in mapping.items():
        if word in text:
            return num

    return None


def json_safe_load(text: str):
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        return json.loads(text[start:end])
    except Exception:
        return {}

