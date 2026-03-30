import os
import statistics
import logging
from typing import Dict, Any, List, Optional

from qdrant_client import AsyncQdrantClient
from sentence_transformers import SentenceTransformer
from qdrant_client.http import models

# ✅ Импортируем нормализаторы из централизованного модуля
from .utils.normalizers import normalize_city, normalize_country
# 🔥 Импортируем нормализацию навыков (с использованием skills_map.py)
from .utils.skill_normalizer import extract_skills_from_text, normalize_skills, compare_skills


# Настройка логгера
logger = logging.getLogger(__name__)


class VacancySearchTool:
    """Инструмент для семантического поиска вакансий в Qdrant."""
    
    def __init__(self):
        self.qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
        self.collection_name = os.getenv("QDRANT_COLLECTION_NAME", "vacancies")
        self.api_key = os.getenv("QDRANT_API_KEY")
        
        # Инициализация клиента Qdrant
        if self.api_key:
            self.client = AsyncQdrantClient(url=self.qdrant_url, api_key=self.api_key)
        else:
            self.client = AsyncQdrantClient(url=self.qdrant_url)
            
        # Модель для эмбеддингов (E5 требует префиксов "query: " и "passage: ")
        self.embedding_model = SentenceTransformer('intfloat/multilingual-e5-large')
        logger.info(f"✅ VacancySearchTool инициализирован: {self.qdrant_url}, коллекция: {self.collection_name}")
    
    async def search_vacancies(self, query: str, location_filter: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Выполняет семантический поиск вакансий по запросу с опциональным фильтром по локации.
        
        Args:
            query: Поисковый запрос пользователя (например, "Junior ML Engineer")
            location_filter: Словарь с фильтрами {'city': 'Москва', 'country': 'Россия'}
        
        Returns:
            Dict с результатами поиска и статистикой:
            - top_vacancies: список топ-5 вакансий
            - salary_median: медианная зарплата
            - salary_top_10: зарплата топ-10%
            - market_range: [min, max] диапазон
            - skill_gaps: пока пусто (заполняется отдельно)
            - match_score: релевантность запроса
        """
        try:
            logger.info(f"🔍 Поиск вакансий: query='{query}', location={location_filter}")
            
            # Кодируем запрос с префиксом "query: " для E5-моделей
            query_embedding = self.embedding_model.encode(f"query: {query}").tolist()
            
            # Формируем фильтры по локации с нормализацией
            filters = None
            if location_filter:
                must_conditions = []
                
                # Нормализуем и добавляем фильтр по стране
                if location_filter.get('country'):
                    norm_country = normalize_country(location_filter['country'])
                    logger.debug(f"🌍 normalize_country('{location_filter['country']}') → '{norm_country}'")
                    if norm_country:
                        must_conditions.append(models.FieldCondition(
                            key="country",
                            match=models.MatchValue(value=norm_country)
                        ))
                
                # Нормализуем и добавляем фильтр по городу
                if location_filter.get('city'):
                    norm_city = normalize_city(location_filter['city'])
                    logger.debug(f"🏙️ normalize_city('{location_filter['city']}') → '{norm_city}'")
                    if norm_city:
                        must_conditions.append(models.FieldCondition(
                            key="city",
                            match=models.MatchValue(value=norm_city)
                        ))
                
                if must_conditions:
                    filters = models.Filter(must=must_conditions)
                    logger.info(f"🔧 Применены фильтры: {must_conditions}")

            # 🔧 ИСПРАВЛЕНИЕ: добавлен параметр using="fast-multilingual-e5-large"
            search_response = await self.client.query_points(
                collection_name=self.collection_name,
                query=query_embedding,
                using="fast-multilingual-e5-large",
                limit=5,
                with_payload=True,
                query_filter=filters
            )
            # Ответ теперь в .points
            search_results = search_response.points
            
            logger.info(f"✅ Qdrant вернул {len(search_results)} результатов")
            
            # Обрабатываем результаты
            vacancies = []
            estimated_salaries = []
            
            for result in search_results:
                payload = result.payload
                vacancy_data = {
                    "id": result.id,
                    "payload": payload,
                    "score": round(result.score, 4)
                }
                vacancies.append(vacancy_data)
                
                # 🔧 УМНЫЙ РАСЧЁТ ЗАРПЛАТЫ ДЛЯ СТАТИСТИКИ
                salary_from = payload.get("salary_from")
                salary_to = payload.get("salary_to")
                
                if salary_from and salary_to:
                    estimated_salary = (salary_from + salary_to) / 2
                elif salary_from:
                    estimated_salary = salary_from * 1.2
                elif salary_to:
                    estimated_salary = salary_to * 0.8
                else:
                    continue
                
                estimated_salaries.append(estimated_salary)
            
            # Расчёт статистики по оценённым зарплатам
            salary_median = int(statistics.median(estimated_salaries)) if estimated_salaries else 0
            salary_top_10 = int(max(estimated_salaries)) if estimated_salaries else 0
            market_range = [
                int(min(estimated_salaries)),
                int(max(estimated_salaries))
            ] if estimated_salaries else [0, 0]
            
            logger.info(f"📊 Статистика: медиана={salary_median}, топ-10%={salary_top_10}, диапазон={market_range}")
            
            market_context = {
                "match_score": 0.0,
                "skill_gaps": [],
                "top_vacancies": vacancies,
                "salary_median": salary_median,
                "salary_top_10": salary_top_10,
                "market_range": market_range
            }
            
            return market_context
            
        except Exception as e:
            logger.exception(f"❌ Ошибка в search_vacancies: {e}")
            return {"error": f"Не удалось найти вакансии: {str(e)}", "top_vacancies": [], "salary_median": 0}
    
    async def calculate_skill_gaps(self, candidate_skills: List[str], vacancies: List[Dict[str, Any]]) -> List[str]:
        """Рассчитывает недостающие навыки кандидата относительно требований вакансий."""
        logger.info(f"🧮 Расчёт skill gaps: кандидат имеет {len(candidate_skills)} навыков, вакансий: {len(vacancies)}")
        
        all_vacancy_skills = set()
        for vac in vacancies:
            payload = vac.get('payload', {})
            skills = payload.get('skills', [])
            if isinstance(skills, list):
                all_vacancy_skills.update(skills)
            elif isinstance(skills, str):
                all_vacancy_skills.update(s.strip() for s in skills.split(",") if s.strip())
        
        # 🔥 НОРМАЛИЗАЦИЯ перед сравнением (используем SKILL_SYNONYMS + SKILL_IMPLIES)
        candidate_normalized = set(s.lower() for s in normalize_skills(candidate_skills))
        vacancy_normalized = set(s.lower() for s in normalize_skills(list(all_vacancy_skills)))
        
        missing = [skill for skill in all_vacancy_skills if skill.lower().strip() not in candidate_normalized]
        logger.info(f"✅ Найдено {len(missing)} недостающих навыков: {missing[:10]}")
        
        return missing[:10]
    
    async def select_vacancy(self, identifier: str) -> Dict[str, Any]:
        """Получает полную информацию о вакансии по ID."""
        logger.info(f"🎯 Выбор вакансии: {identifier}")
        try:
            result = await self.client.retrieve(
                collection_name=self.collection_name,
                ids=[identifier],
                with_payload=True,
                with_vectors=False
            )
            if result:
                logger.info(f"✅ Вакансия найдена: {result[0].payload.get('title', 'N/A')}")
                return {"id": identifier, "payload": result[0].payload}
            else:
                logger.warning(f"⚠️ Вакансия {identifier} не найдена")
                return {"id": identifier, "error": "Vacancy not found"}
        except Exception as e:
            logger.exception(f"❌ Ошибка в select_vacancy: {e}")
            return {"id": identifier, "error": str(e)}
    
    async def tailor_resume(
        self,
        candidate_resume: Optional[str],
        vacancy_payload: Dict[str, Any],
        declared_skills: Optional[List[str]] = None  # ← НОВЫЙ ПАРАМЕТР
    ) -> Dict[str, Any]:
        """
        Адаптирует резюме под требования вакансии.
        
        Args:
            candidate_resume: Текст резюме (может быть None)
            vacancy_payload: Данные вакансии из Qdrant
            declared_skills: Навыки, заявленные пользователем (из state.candidate.skills)
        
        Returns:
            Dict с рекомендациями для улучшения резюме
        """
        logger.info(f"✏️ tailor_resume: resume={bool(candidate_resume)}, declared={declared_skills}")
        
        try:
            
            # 🔥 1. Извлекаем навыки кандидата из ДВУХ источников
            resume_skills = extract_skills_from_text(candidate_resume) if candidate_resume else []
            declared_norm = normalize_skills(declared_skills or [])
            
            # Объединяем и нормализуем
            candidate_skills = normalize_skills(resume_skills + declared_norm)
            logger.info(f"✅ candidate_skills: {candidate_skills}")
            
            # 🔥 2. Извлекаем навыки вакансии (учитываем оба поля + строку/список)
            vacancy_raw = vacancy_payload.get("skills") or vacancy_payload.get("requirements") or []
            if isinstance(vacancy_raw, str):
                vacancy_raw = [s.strip() for s in vacancy_raw.split(",") if s.strip()]
            vacancy_skills = normalize_skills(vacancy_raw)
            logger.info(f"✅ vacancy_skills: {vacancy_skills}")
            
            # 🔥 3. Сравниваем (уже с нормализацией внутри compare_skills)
            comparison = compare_skills(candidate_skills, vacancy_skills)
            
            # 🔥 4. Формируем рекомендации
            recommendations = []
            
            # 🔑 Ключевые слова для добавления (только недостающие)
            if comparison["missing"]:
                keywords = ", ".join(f'"{s}"' for s in comparison["missing"][:5])
                recommendations.append(f"🔑 Добавь ключевые слова: {keywords}")
            
            # 💡 Усиление формулировок (простая эвристика для MVP)
            weak_phrases = ["работал с", "участвовал", "помогал", "знаю"]
            strong_phrases = ["разрабатывал", "внедрил", "оптимизировал", "автоматизировал"]
            
            if candidate_resume:
                for phrase in weak_phrases:
                    if phrase in candidate_resume.lower():
                        recommendations.append(f"💡 Замени «{phrase}» на «{strong_phrases[0]}» для усиления")
                        break
            
            # 📊 Прогноз улучшения
            current_match = comparison["match_percentage"]
            projected_match = min(95, current_match + 15) if comparison["missing"] else current_match
            
            result = {
                "status": "adapted",
                "match_percentage": current_match,
                "projected_match": projected_match,
                "matched_skills": comparison["match"],
                "missing_skills": comparison["missing"][:5],
                "extra_skills": comparison["extra"][:3],
                "recommendations": recommendations if recommendations else ["✅ Резюме хорошо соответствует вакансии"],
                "vacancy_title": vacancy_payload.get("title", "вакансии"),
                "vacancy_company": vacancy_payload.get("company", ""),
                "vacancy_url": vacancy_payload.get("url")
            }
            
            logger.info(f"✅ tailor_resume: match={current_match}%, missing={len(comparison['missing'])} навыков")
            return result
            
        except Exception as e:
            logger.exception(f"❌ Ошибка в tailor_resume: {e}")
            return {"error": str(e), "recommendations": ["Не удалось проанализировать резюме"], "status": "error"}
    
    async def generate_roadmap(
        self,
        current_skills: List[str],
        market_context: Dict[str, Any],
        target_role: Optional[str] = None,
        timeframe_months: int = 3
    ) -> Dict[str, Any]:
        """
        Генерирует план развития навыков НА ОСНОВЕ РЕАЛЬНЫХ ДАННЫХ ИЗ БАЗЫ.
        """
        logger.info(f"🗺️ [ROADMAP] Начало: навыки={current_skills}, горизонт={timeframe_months} мес.")
        
        try:
            # 🔥 НОРМАЛИЗАЦИЯ НАВЫКОВ КАНДИДАТА (применяет SKILL_SYNONYMS + SKILL_IMPLIES)
            current_skills = normalize_skills(current_skills)
            logger.info(f"✅ Нормализованные навыки кандидата: {current_skills}")
            
            # 1. Собираем ВСЕ навыки из реальных вакансий в market_context
            logger.info("[ROADMAP] Шаг 1: Собираем навыки из вакансий...")
            skill_stats = {}
            
            vacancies = market_context.get("top_vacancies", [])
            logger.info(f"[ROADMAP] Найдено вакансий для анализа: {len(vacancies)}")
            
            for vac in vacancies:
                payload = vac.get("payload", {})
                skills = payload.get("skills", [])
                
                if isinstance(skills, str):
                    skills = [s.strip() for s in skills.split(",") if s.strip()]
                
                # 🔥 НОРМАЛИЗАЦИЯ НАВЫКОВ ИЗ ВАКАНСИИ
                skills = normalize_skills(skills)
                
                for skill in skills:
                    skill_key = skill.strip().lower()
                    if skill_key not in skill_stats:
                        skill_stats[skill_key] = {"count": 0, "salaries": [], "grades": [], "titles": []}
                    
                    skill_stats[skill_key]["count"] += 1
                    
                    salary = payload.get("salary_from") or payload.get("salary_to")
                    if salary:
                        skill_stats[skill_key]["salaries"].append(salary)
                    
                    if payload.get("grade"):
                        skill_stats[skill_key]["grades"].append(payload["grade"])
                    if payload.get("title"):
                        skill_stats[skill_key]["titles"].append(payload["title"])
            
            logger.info(f"[ROADMAP] ✅ Шаг 1 завершён: {len(skill_stats)} уникальных навыков")
            
            # 2. Определяем недостающие навыки (сравнение в lowercase)
            logger.info("[ROADMAP] Шаг 2: Определяем недостающие навыки...")
            current_skills_lower = set(s.lower() for s in current_skills)
            missing_skills = {
                skill: stats 
                for skill, stats in skill_stats.items() 
                if skill not in current_skills_lower
            }
            logger.info(f"[ROADMAP] ✅ Шаг 2 завершён: {len(missing_skills)} недостающих навыков")
            
            # 3. Приоритезация
            logger.info("[ROADMAP] Шаг 3: Приоритезация навыков...")
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
                    "suggested_search": f"{skill} tutorial для {target_role or 'ML инженера'}"
                })
            
            prioritized = sorted(prioritized, key=lambda x: -x["priority_score"])[:5]
            logger.info(f"[ROADMAP] ✅ Шаг 3 завершён: {len(prioritized)} навыков в приоритете")
            
            # 4. Прогноз по зарплате
            logger.info("[ROADMAP] Шаг 4: Расчёт прогноза зарплаты...")
            base_salary = market_context.get("salary_median", 0)
            logger.info(f"[ROADMAP] Базовая зарплата (медиана): {base_salary:,} ₽")
            
            skills_with_impact = min(2, len(prioritized))
            salary_growth_factor = 1 + (0.08 * skills_with_impact)
            projected_salary = int(base_salary * salary_growth_factor)
            
            logger.info(f"[ROADMAP] Прогноз: {base_salary:,} → {projected_salary:,} ₽ (+{int((salary_growth_factor-1)*100)}%)")
            
            roadmap = {
                "target_role": target_role or "ML Engineer",
                "timeframe_months": timeframe_months,
                "current_skills": current_skills,
                "analyzed_vacancies": len(vacancies),
                "skill_priorities": prioritized,
                "expected_salary_range": [base_salary, projected_salary],
                "next_step": prioritized[0]["skill"] if prioritized else None,
                "methodology": "Приоритет = частота упоминания × средняя зарплата; прогноз +8% за навык (макс. 2)",
                "growth_explanation": f"Освоение {skills_with_impact} ключевых навыков может повысить зарплату на ~{int((salary_growth_factor - 1) * 100)}%"
            }
            
            logger.info(f"✅ [ROADMAP] Roadmap сгенерирован успешно!")
            return roadmap
            
        except Exception as e:
            logger.exception(f"❌ [ROADMAP] Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
            return None


# Глобальный экземпляр инструмента
vacancy_search_tool = VacancySearchTool()