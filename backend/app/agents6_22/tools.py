"""
Инструмент семантического поиска вакансий в Qdrant.

ИСПРАВЛЕНИЯ:
- generate_roadmap: возвращает dict вместо None при ошибке.
- Согласован ключ skill_gaps (вместо skill_g).
"""
 
import os
import statistics
import logging
from typing import Dict, Any, List, Optional

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from qdrant_client.http import models

from .utils.normalizers import normalize_city, normalize_country
from .utils.skill_normalizer import extract_skills_from_text, normalize_skills, compare_skills

logger = logging.getLogger(__name__)


def _get_salary_bounds_rub(payload: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    salary_from_rub = payload.get("salary_from_rub")
    salary_to_rub = payload.get("salary_to_rub")

    if salary_from_rub is not None or salary_to_rub is not None:
        return salary_from_rub, salary_to_rub

    return payload.get("salary_from"), payload.get("salary_to")


class VacancySearchTool:
    """Инструмент для семантического поиска вакансий в Qdrant."""

    def __init__(self):
        self.qdrant_url = os.getenv("QDRANT_URL", "http://16.54.110.212:6333")
        self.collection_name = os.getenv("QDRANT_COLLECTION_NAME", "vacancies")
        self.api_key = os.getenv("QDRANT_API_KEY")

        if self.api_key:
            self.client = QdrantClient(url=self.qdrant_url, api_key=self.api_key)
        else:
            self.client = QdrantClient(url=self.qdrant_url)

        self.embedding_model = None
        logger.info(f"✅ VacancySearchTool: {self.qdrant_url}, коллекция: {self.collection_name}")

    def _get_embedding_model(self) -> SentenceTransformer:
        if self.embedding_model is None:
            self.embedding_model = SentenceTransformer("intfloat/multilingual-e5-large")
        return self.embedding_model

    async def search_vacancies(
        self, query: str, location_filter: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Семантический поиск вакансий с фильтром по локации.

        Returns:
            Dict: top_vacancies, salary_median, salary_top_10, market_range, skill_gaps, match_score
        """
        try:
            logger.info(f"🔍 Поиск: query='{query}', location={location_filter}")

            query_embedding = self._get_embedding_model().encode(f"query: {query}").tolist()

            # Фильтры по локации
            filters = None
            if location_filter:
                must_conditions = []
                if location_filter.get("country"):
                    norm = normalize_country(location_filter["country"])
                    if norm:
                        must_conditions.append(
                            models.FieldCondition(key="country", match=models.MatchValue(value=norm))
                        )
                if location_filter.get("city"):
                    norm = normalize_city(location_filter["city"])
                    if norm:
                        must_conditions.append(
                            models.FieldCondition(key="city", match=models.MatchValue(value=norm))
                        )
                if must_conditions:
                    filters = models.Filter(must=must_conditions)
                    logger.info(f"🔧 Фильтры: {must_conditions}")

            search_response = self.client.query_points(
                collection_name=self.collection_name,
                query=query_embedding,
                using="fast-multilingual-e5-large",
                limit=5,
                with_payload=True,
                query_filter=filters,
            )
            search_results = search_response.points

            logger.info(f"✅ Qdrant: {len(search_results)} результатов")

            vacancies = []
            estimated_salaries = []
            all_skills = []  # Собираем все навыки из вакансий

            for result in search_results:
                payload = result.payload
                vacancy_data = {
                    "id": result.id,
                    "score": round(result.score, 4),
                    **payload,
                }
                vacancies.append(vacancy_data)

                # Собираем навыки
                skills_raw = payload.get("skills") or payload.get("requirements") or []
                if isinstance(skills_raw, str):
                    skills_raw = [s.strip() for s in skills_raw.split(",") if s.strip()]
                if isinstance(skills_raw, list):
                    all_skills.extend(skills_raw)

                # Зарплатная статистика
                salary_from, salary_to = _get_salary_bounds_rub(payload)

                if salary_from is not None and salary_to is not None:
                    estimated_salaries.append((salary_from + salary_to) / 2)
                elif salary_from is not None:
                    estimated_salaries.append(salary_from * 1.2)
                elif salary_to is not None:
                    estimated_salaries.append(salary_to * 0.8)

            salary_median = int(statistics.median(estimated_salaries)) if estimated_salaries else 0
            salary_top_10 = int(max(estimated_salaries)) if estimated_salaries else 0
            market_range = (
                [int(min(estimated_salaries)), int(max(estimated_salaries))]
                if estimated_salaries
                else [0, 0]
            )

            # Нормализуем собранные навыки для skill_gaps
            normalized_market_skills = normalize_skills(list(set(all_skills)))

            logger.info(f"📊 Медиана={salary_median}, диапазон={market_range}, навыков рынка={len(normalized_market_skills)}")

            return {
                "match_score": 0.0,
                "skill_gaps": normalized_market_skills,  # ← СОГЛАСОВАННЫЙ ключ
                "top_vacancies": vacancies,
                "salary_median": salary_median,
                "salary_top_10": salary_top_10,
                "market_range": market_range,
            }

        except Exception as e:
            logger.exception(f"❌ Ошибка search_vacancies: {e}")
            return {"error": str(e), "top_vacancies": [], "salary_median": 0, "skill_gaps": []}

    async def calculate_skill_gaps(
        self, candidate_skills: List[str], vacancies: List[Dict[str, Any]]
    ) -> List[str]:
        """Рассчитывает недостающие навыки."""
        logger.info(f"🧮 Skill gaps: {len(candidate_skills)} навыков, {len(vacancies)} вакансий")

        all_vacancy_skills = set()
        for vac in vacancies:
            if not isinstance(vac, dict):
                continue
            payload = vac.get("payload", vac)
            skills = payload.get("skills") or payload.get("requirements") or []
            if isinstance(skills, list):
                all_vacancy_skills.update(skills)
            elif isinstance(skills, str):
                all_vacancy_skills.update(s.strip() for s in skills.split(",") if s.strip())

        candidate_normalized = set(s.lower() for s in normalize_skills(candidate_skills))
        missing = [s for s in all_vacancy_skills if s.lower().strip() not in candidate_normalized]
        logger.info(f"✅ {len(missing)} недостающих: {missing[:10]}")
        return missing[:10]

    async def select_vacancy(self, identifier: str) -> Dict[str, Any]:
        """Получает полную информацию о вакансии по ID."""
        logger.info(f"🎯 Выбор вакансии: {identifier}")
        try:
            result = self.client.retrieve(
                collection_name=self.collection_name,
                ids=[identifier],
                with_payload=True,
                with_vectors=False,
            )
            if result:
                logger.info(f"✅ Вакансия: {result[0].payload.get('title', 'N/A')}")
                return {"id": identifier, "payload": result[0].payload}
            else:
                return {"id": identifier, "error": "Vacancy not found"}
        except Exception as e:
            logger.exception(f"❌ Ошибка select_vacancy: {e}")
            return {"id": identifier, "error": str(e)}

    async def tailor_resume(
        self,
        candidate_resume: Optional[str],
        vacancy_payload: Dict[str, Any],
        declared_skills: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Адаптирует резюме под вакансию.

        Args:
            candidate_resume: Текст резюме
            vacancy_payload: Данные вакансии
            declared_skills: Навыки из state.candidate.skills
        """
        logger.info(f"✏️ tailor_resume: resume={bool(candidate_resume)}, declared={declared_skills}")

        try:
            # 1. Навыки кандидата из двух источников
            resume_skills = extract_skills_from_text(candidate_resume) if candidate_resume else []
            declared_norm = normalize_skills(declared_skills or [])
            candidate_skills = normalize_skills(resume_skills + declared_norm)
            logger.info(f"✅ candidate_skills: {candidate_skills}")

            # 2. Навыки вакансии
            vacancy_raw = vacancy_payload.get("skills") or vacancy_payload.get("requirements") or []
            if isinstance(vacancy_raw, str):
                vacancy_raw = [s.strip() for s in vacancy_raw.split(",") if s.strip()]
            vacancy_skills = normalize_skills(vacancy_raw)
            logger.info(f"✅ vacancy_skills: {vacancy_skills}")

            # 3. Сравниваем
            comparison = compare_skills(candidate_skills, vacancy_skills)

            # 4. Рекомендации
            recommendations = []
            if comparison["missing"]:
                keywords = ", ".join(f'"{s}"' for s in comparison["missing"][:5])
                recommendations.append(f"🔑 Добавь ключевые слова: {keywords}")

            weak_phrases = ["работал с", "участвовал", "помогал", "знаю"]
            strong_phrases = ["разрабатывал", "внедрил", "оптимизировал", "автоматизировал"]
            if candidate_resume:
                for phrase in weak_phrases:
                    if phrase in candidate_resume.lower():
                        recommendations.append(f"💡 Замени «{phrase}» на «{strong_phrases[0]}» для усиления")
                        break

            current_match = comparison["match_percentage"]
            projected_match = min(95, current_match + 15) if comparison["missing"] else current_match

            return {
                "status": "adapted",
                "match_percentage": current_match,
                "projected_match": projected_match,
                "matched_skills": comparison["match"],
                "missing_skills": comparison["missing"][:5],
                "extra_skills": comparison["extra"][:3],
                "recommendations": recommendations or ["✅ Резюме хорошо соответствует вакансии"],
                "vacancy_title": vacancy_payload.get("title", "вакансии"),
                "vacancy_company": vacancy_payload.get("company", ""),
                "vacancy_url": vacancy_payload.get("url"),
            }

        except Exception as e:
            logger.exception(f"❌ Ошибка tailor_resume: {e}")
            return {"error": str(e), "recommendations": ["Не удалось проанализировать"], "status": "error"}

    async def generate_roadmap(
        self,
        current_skills: List[str],
        market_context: Dict[str, Any],
        target_role: Optional[str] = None,
        timeframe_months: int = 3,
    ) -> Dict[str, Any]:
        """
        Генерирует план развития на основе реальных данных вакансий.

        ИСПРАВЛЕНИЕ: возвращает dict с error вместо None при ошибке.
        """
        logger.info(f"🗺️ [ROADMAP] навыки={current_skills}, горизонт={timeframe_months} мес.")

        try:
            current_skills = normalize_skills(current_skills)

            # 1. Навыки из вакансий
            skill_stats = {}
            vacancies = market_context.get("top_vacancies", [])

            for vac in vacancies:
                if not isinstance(vac, dict):
                    continue
                skills = vac.get("skills", [])
                if isinstance(skills, str):
                    skills = [s.strip() for s in skills.split(",") if s.strip()]
                skills = normalize_skills(skills)

                for skill in skills:
                    key = skill.strip().lower()
                    if key not in skill_stats:
                        skill_stats[key] = {"count": 0, "salaries": [], "grades": [], "titles": []}
                    skill_stats[key]["count"] += 1

                    salary = vac.get("salary_from") or vac.get("salary_to")
                    if salary:
                        skill_stats[key]["salaries"].append(salary)
                    if vac.get("grade"):
                        skill_stats[key]["grades"].append(vac["grade"])
                    if vac.get("title"):
                        skill_stats[key]["titles"].append(vac["title"])

            # 2. Недостающие навыки
            current_lower = set(s.lower() for s in current_skills)
            missing_skills = {k: v for k, v in skill_stats.items() if k not in current_lower}

            # 3. Приоритезация
            prioritized = []
            for skill, stats in missing_skills.items():
                avg_salary = statistics.mean(stats["salaries"]) if stats["salaries"] else 0
                priority_score = stats["count"] * (1 + avg_salary / 100000)
                estimated_weeks = max(1, 4 - stats["count"])

                prioritized.append({
                    "skill": skill.title(),
                    "market_demand": stats["count"],
                    "avg_salary_impact": int(avg_salary) if avg_salary else None,
                    "seen_in_roles": list(set(stats["titles"]))[:3],
                    "estimated_weeks": estimated_weeks,
                    "priority_score": round(priority_score, 2),
                    "suggested_search": f"{skill} tutorial для {target_role or 'специалиста'}",
                })

            prioritized = sorted(prioritized, key=lambda x: -x["priority_score"])[:5]

            # 4. Прогноз зарплаты
            base_salary = market_context.get("salary_median", 0)
            skills_with_impact = min(2, len(prioritized))
            salary_growth_factor = 1 + (0.08 * skills_with_impact)
            projected_salary = int(base_salary * salary_growth_factor)

            return {
                "target_role": target_role or "Специалист",
                "timeframe_months": timeframe_months,
                "current_skills": current_skills,
                "analyzed_vacancies": len(vacancies),
                "skill_priorities": prioritized,
                "expected_salary_range": [base_salary, projected_salary],
                "next_step": prioritized[0]["skill"] if prioritized else None,
                "methodology": "Приоритет = частота × средняя зарплата; прогноз +8% за навык (макс. 2)",
                "growth_explanation": f"Освоение {skills_with_impact} навыков → +{int((salary_growth_factor - 1) * 100)}%",
            }

        except Exception as e:
            logger.exception(f"❌ [ROADMAP] Ошибка: {e}")
            # ИСПРАВЛЕНО: возвращаем dict вместо None
            return {"error": str(e), "skill_priorities": [], "target_role": target_role}


# Глобальный экземпляр
vacancy_search_tool = VacancySearchTool()
