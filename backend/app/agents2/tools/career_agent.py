# career_agent.py

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
        print("\n" + "="*30)
        print("🚀 ENTERING handle_vacancy_choice")
        
        message = state.get("message", "")
        top_vacancies = state.get("top_vacancies", [])

        # --- УМНОЕ ВОССТАНОВЛЕНИЕ ИЗ ИСТОРИИ ---
        if not top_vacancies and state.get("history"):
            print("⚠️ vacancies_count is 0! Trying to recover from history...")
            for entry in reversed(state["history"]):
                raw_data = entry.get("assistant")
                
                # 1. Если это строка, похожая на словарь, превращаем в словарь
                if isinstance(raw_data, str) and ("'vacancies':" in raw_data or '"vacancies":' in raw_data):
                    try:
                        # Заменяем одинарные кавычки на двойные для json.loads или используем eval
                        # Безопаснее всего в данном случае ast.literal_eval
                        import ast
                        raw_data = ast.literal_eval(raw_data)
                    except:
                        continue

                # 2. Если теперь это словарь, ищем в нем ключ 'vacancies'
                if isinstance(raw_data, dict) and "vacancies" in raw_data:
                    top_vacancies = raw_data["vacancies"]
                # 3. Или если это был просто список
                elif isinstance(raw_data, list):
                    top_vacancies = raw_data

                if top_vacancies:
                    state["top_vacancies"] = top_vacancies
                    print(f"✅ Recovered {len(top_vacancies)} vacancies from history!")
                    break
        # ---------------------------------------

        print(f"DEBUG DATA: msg='{message}', vacancies_count={len(top_vacancies)}")
        
        idx = extract_number(message)
        
        # Теперь len(top_vacancies) должен быть > 0
        if idx is not None and len(top_vacancies) > 0 and 0 < idx <= len(top_vacancies):
            choice_idx = idx - 1
            selected = top_vacancies[choice_idx]
            
            print(f"✅ SELECTED: {selected.get('title')}")
            
            # Дальше твой код генерации резюме...
            state["selected_vacancy"] = selected
            state["action"] = "resume"
            state["stage"] = "generating_resume"
            
            # Вызываем LLM (не забудь про try/except из-за лимитов Groq)
            try:
                candidate = state.get("candidate", {})
                skills = candidate.get("skills", "Python, ML")
                prompt = f"Напиши резюме для {selected.get('title')}. Мои навыки: {skills}"
                state["response"] = str(run_local_llm(prompt))
                state["stage"] = "resume_ready"
            except Exception as e:
                state["response"] = f"Ошибка LLM: {str(e)}"
            
            return state

        state["response"] = f"Пожалуйста, введите корректный номер от 1 до {len(top_vacancies)}."
        return state


    # -----------------------------
    # MAIN ROUTE
    # -----------------------------
    def route(self, state: Dict) -> Dict:
        print(f"career main_route do  state['action'] - {state.get('action', 'search')}")
        message = (state.get("message") or "").strip().lower()
        stage = (state.get("stage") or "").strip()
        # -----------------------------
        # UNIVERSAL VACANCY CHOICE
        # работает не только для resume-stage,
        # но и для выбора вакансии со страницы вакансий
        # -----------------------------
        if message.isdigit() and state.get("top_vacancies"):
            idx = int(message)
            top_vacancies = state.get("top_vacancies", [])

            if 0 < idx <= len(top_vacancies):
                selected = top_vacancies[idx - 1]
                state["selected_vacancy"] = selected
                state["response"] = {
                    "message": f"Выбрана вакансия: {selected.get('title', 'Без названия')}",
                    "vacancy": selected,
                }
                state["last_action"] = "Агент отработал: select_vacancy"
                return state
        print(f"DEBUG AGENT: stage='{stage}', action='{state.get('action')}'")

        # --- КРИТИЧЕСКАЯ ПРАВКА ---
        # Если мы ждем выбора, не даем коду идти дальше к логике переопределения action
        if stage == "waiting_vacancy_choice":
            print("👉 AGENT: handling vacancy choice")
            return self.handle_vacancy_choice(state)
        # --------------------------
        print(f"career main_route do  state['action'] - {state.get('action', 'search')}")
        action = state.get("action", "search")
        print(f"career main_route posle  state['action'] - {state['action']}")
        
        # Вторичная защита от смены action на цифрах
        if not message.isdigit():
            if "resume" in message:
                action = "resume"
            elif "roadmap" in message:
                action = "roadmap"
            elif "interview" in message:
                action = "interview"
        print(f"career main_route  state['action'] - {state['action']}")
        state["action"] = action
        print(f"career main_route  state['action'] - {state['action']}")

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

            all_vacancy_skills = set()
            for v in top_vacancies:
                for s in v.get("skills", []):
                    all_vacancy_skills.add(s.lower())

            candidate_skills = set(s.lower() for s in skills)

            vacancy_missing = list(all_vacancy_skills - candidate_skills)
            missing = market_gaps or vacancy_missing

            if not missing:
                missing = ["Machine Learning", "System Design", "MLOps"]

            # генерация roadmap
            roadmap = {
                skill: f"Изучи {skill} и сделай 1-2 проекта"
                for skill in missing[:5]
            }

            roadmap = "\n".join([f"{k} — {v}" for k, v in roadmap.items()])

            state["response"] = roadmap
            state["roadmap"] = roadmap
            state["last_action"] = "Агент отработал: roadmap"

            return state   # ✅ ВОТ ЭТО КРИТИЧНО

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
        if action == "fit":
            selected = state.get("selected_vacancy")
            candidate = state.get("candidate", {})

            if not selected:
                state["response"] = "Сначала выбери вакансию (введи номер из списка)."
                return state

            candidate_skills = set(s.lower() for s in candidate.get("skills", []))
            vacancy_skills = set(s.lower() for s in selected.get("skills", []))

            # пересечение
            match = candidate_skills & vacancy_skills
            missing = vacancy_skills - candidate_skills

            # score
            if vacancy_skills:
                score = int(len(match) / len(vacancy_skills) * 100)
            else:
                score = 50

            response = f"""
            📊 Fit analysis: {selected.get("title")}

            Match score: {score}%

            ✅ Сильные стороны:
            {', '.join(match) if match else 'нет совпадений'}

            ❌ Пробелы:
            {', '.join(missing) if missing else 'нет'}

            💡 Рекомендации:
            - Добавь 1-2 проекта с недостающими навыками
            - Подготовь кейсы под требования вакансии
            """

            state["response"] = response
            state["last_action"] = "Агент отработал: fit"

            return state
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
            top_vacancies = state.get("top_vacancies", [])
            resume_skills = state.get("resume_skills") or candidate.get("skills")
            stage = state.get("stage")
            message = (state.get("message") or "").strip()

            selected = state.get("selected_vacancy")
            print("DEBUG SELECTED:", state.get("selected_vacancy"))

            # FIX: защита от повторной генерации
            if selected and stage != "resume_ready":
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
        

                return state

            if not resume_skills:
                state["response"] = (
                    "Чтобы сформировать резюме, загрузите файл с резюме."
                )
                state["stage"] = "waiting_resume"
                return state

            if not top_vacancies:
                vacancies = search_vacancies(
                    query_text=user_query,
                    skills=skills,
                    normalized_city=normalized_city,
                    relocation=relocation,
                    limit=5
                )

                state["top_vacancies"] = vacancies
                top_vacancies = vacancies

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

