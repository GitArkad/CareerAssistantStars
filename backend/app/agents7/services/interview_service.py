"""
Сервис проведения мини-интервью с динамической генерацией вопросов.
Гибридный подход: детекция по ключевым словам + LLM-резерв.
Оптимизирован для надёжности, масштабируемости и контроля затрат.
"""

import logging
from typing import Dict, List, Optional, Any

from langchain_core.messages import SystemMessage, HumanMessage

logger = logging.getLogger(__name__)

# Глобальные клиенты (инициализируются из main.py)
_llm_fast = None
_llm_smart = None


def set_llm_clients(fast_llm, smart_llm=None):
    """
    Инициализирует LLM-клиенты для сервиса.
    
    Args:
        fast_llm: Клиент для 8B модели (основной)
        smart_llm: Клиент для 70B модели (fallback, опционально)
    """
    global _llm_fast, _llm_smart
    _llm_fast = fast_llm
    _llm_smart = smart_llm  # Если None — fallback будет на fast


def should_trigger_interview(message: str) -> bool:
    """Проверяет, является ли запрос запросом на интервью."""
    if not message:
        return False
    
    message_lower = message.lower()
    
    keywords = [
        # === Явные запросы ===
        "хочу интервью", "проведи собеседование", "мини-интервью",
        "интервью на позицию", "симуляция собеседования", "мокап интервью",
        "техническое интервью", "скрининг интервью",
        
        # === Тренировка / практика ===
        "потренируйся со мной", "потренировать меня", "практика собеседования",
        "подготовь к интервью", "репетиция собеседования", "тренировка перед собеседованием",
        "попрактиковаться в ответах", "симуляция интервью",
        
        # === Вопросы / проверка ===
        "задай вопросы", "проверь мои знания", "тест на позицию",
        "опрос по навыкам", "проверка навыков", "квиз по профессии",
        "задай мне вопросы", "спроси меня", "проверь меня",
        
        # === Естественные формулировки ===
        "хочу попрактиковаться", "давай потренируемся", "можешь меня спросить",
        "проведи мне тест", "устрой мне опрос", "давай симуляцию",
        "подготовь меня к собеседованию", "помоги подготовиться к интервью",
        
        # === На английском ===
        "mock interview", "practice interview", "interview simulation",
        "technical interview", "job interview practice"
    ]
    
    return any(kw in message_lower for kw in keywords)


def extract_interview_context(current_state: Dict, input_state: Dict) -> Dict[str, Any]:
    """Извлекает контекст для интервью из state."""
    candidate = current_state.get("candidate") or input_state.get("candidate") or {}
    market = current_state.get("market") or input_state.get("market") or {}
    
    role = candidate.get("specialization") or candidate.get("grade") or "Junior ML Engineer"
    
    vacancy_payload = None
    if market.get("top_vacancies"):
        vacancy_payload = market["top_vacancies"][0].get("payload", {})
    
    vacancy_skills = []
    if vacancy_payload:
        raw = vacancy_payload.get("requirements") or vacancy_payload.get("skills") or []
        if isinstance(raw, str):
            vacancy_skills = [s.strip() for s in raw.split(",") if s.strip()]
        elif isinstance(raw, list):
            vacancy_skills = [s for s in raw if isinstance(s, str)]
    
    candidate_skills = candidate.get("skills") or []
    if isinstance(candidate_skills, str):
        candidate_skills = [s.strip() for s in candidate_skills.split(",") if s.strip()]
    
    return {
        "role": role,
        "vacancy_payload": vacancy_payload,
        "vacancy_skills": vacancy_skills,
        "candidate_skills": candidate_skills
    }


def _get_llm(use_smart: bool = False):
    """Возвращает нужный LLM-клиент."""
    if use_smart and _llm_smart:
        return _llm_smart
    return _llm_fast


async def generate_interview_questions(
    role: str,
    vacancy_skills: Optional[List[str]] = None,
    candidate_skills: Optional[List[str]] = None,
    count: int = 5,
    use_smart_model: bool = False
) -> List[Dict[str, Any]]:
    """
    Генерирует вопросы для мини-собеседования.
    
    🔧 Оптимизация: 8B по умолчанию, 70B только при use_smart_model=True или после ошибки.
    """
    llm = _get_llm(use_smart_model)
    if not llm:
        logger.error("❌ LLM не инициализирован в interview_service")
        return _get_fallback_questions(role, count)
    
    logger.info(f"🎤 Generating {count} questions for {role} (model: {llm.model_name})")
    
    skills_context = ""
    if vacancy_skills:
        skills_context += f"Требуемые навыки: {', '.join(vacancy_skills)}. "
    if candidate_skills:
        skills_context += f"У кандидата есть: {', '.join(candidate_skills)}. "
    
    prompt = f"""Ты — технический интервьюер для позиции {role}.

{skills_context}

Сгенерируй {count} вопросов для короткого собеседования (5-7 минут).

ТРЕБОВАНИЯ:
1. Формат: 60% multiple_choice (2-3 варианта, один правильный), 40% short_text (1-2 предложения)
2. Темы: баланс теории и практики, релевантные указанным навыкам
3. Сложность: Junior уровень
4. Каждый вопрос проверяет конкретный навык

ФОРМАТ ОТВЕТА (СТРОГО JSON-массив, без пояснений):
[
  {{
    "id": 1,
    "question": "Текст вопроса?",
    "type": "multiple_choice",
    "options": ["Вариант A", "Вариант B", "Вариант C"],
    "correct_option_index": 0,
    "expected_answer": "Краткое пояснение правильного ответа",
    "skills_tested": ["навык1", "навык2"]
  }},
  {{
    "id": 2,
    "question": "Текст вопроса?",
    "type": "short_text",
    "expected_answer": "Ключевые идеи для проверки",
    "skills_tested": ["навык"]
  }}
]

Важно: верни ТОЛЬКО валидный JSON-массив.
"""
    
    # 🔧 Пробуем 2 раза: сначала 8B, при ошибке — 70B (если доступен)
    for attempt in range(2):
        try:
            current_llm = _get_llm(use_smart=(attempt == 1 and _llm_smart is not None))
            
            from langchain_core.output_parsers import JsonOutputParser
            parser = JsonOutputParser()
            
            response = current_llm.invoke([
                SystemMessage(content="Ты — технический интервьюер. Отвечай строго в формате JSON-массива."),
                HumanMessage(content=prompt)
            ], config={"max_tokens": 2000})
            
            questions = parser.parse(response.content)
            
            if isinstance(questions, list) and questions:
                validated = [q for q in questions if isinstance(q, dict) and "question" in q]
                if validated:
                    logger.info(f"✅ Сгенерировано {len(validated)} вопросов (попытка {attempt + 1})")
                    return validated[:count]
            
            logger.warning(f"⚠️ LLM вернул невалидный список (попытка {attempt + 1})")
            
        except Exception as e:
            logger.warning(f"⚠️ Попытка {attempt + 1} генерации вопросов упала: {e}")
            if attempt == 0 and _llm_smart:
                logger.info("🔄 Пробуем с 70B моделью...")
                continue
            break
    
    logger.warning("⚠️ Генерация вопросов упала — используем fallback")
    return _get_fallback_questions(role, count)


def _get_fallback_questions(role: str, count: int = 5) -> List[Dict]:
    """Дефолтные вопросы, если LLM упал."""
    fallback = [
        {
            "id": 1, "question": "Что такое переобучение модели и как с ним бороться?",
            "type": "multiple_choice",
            "options": [
                "Модель запоминает шум в данных, а не общие закономерности",
                "Модели не хватает данных для обучения",
                "Оба варианта верны"
            ],
            "correct_option_index": 0,
            "expected_answer": "Переобучение — когда модель слишком подстраивается под обучающие данные. Лечится регуляризацией, кросс-валидацией, упрощением модели.",
            "skills_tested": ["ML", "overfitting", "validation"]
        },
        {
            "id": 2, "question": "Как загрузить CSV-файл в pandas?",
            "type": "short_text",
            "expected_answer": "pd.read_csv('filename.csv')",
            "skills_tested": ["Python", "pandas", "data_loading"]
        },
        {
            "id": 3, "question": "В чём разница между precision и recall?",
            "type": "short_text",
            "expected_answer": "Precision — доля верных положительных предсказаний среди всех положительных. Recall — доля найденных положительных среди всех реальных положительных.",
            "skills_tested": ["metrics", "evaluation", "classification"]
        },
        {
            "id": 4, "question": "Зачем нужна кросс-валидация?",
            "type": "multiple_choice",
            "options": [
                "Оценить, как модель обобщает данные на новых примерах",
                "Ускорить процесс обучения модели",
                "Сэкономить память при обучении"
            ],
            "correct_option_index": 0,
            "expected_answer": "Кросс-валидация помогает оценить обобщающую способность модели, используя разные подмножества данных для тренировки и теста.",
            "skills_tested": ["validation", "ML", "evaluation"]
        },
        {
            "id": 5, "question": "Что сделает код: df.dropna()?",
            "type": "multiple_choice",
            "options": [
                "Удалит строки или столбцы, содержащие пропущенные значения",
                "Заполнит пропущенные значения нулями",
                "Ничего не изменит в датафрейме"
            ],
            "correct_option_index": 0,
            "expected_answer": "dropna() удаляет строки/столбцы с пропусками. Можно настроить axis, how, thresh для гибкого управления.",
            "skills_tested": ["pandas", "data_cleaning", "Python"]
        }
    ]
    return fallback[:count]


def format_interview_question(q: Dict, question_number: int, total: int) -> str:
    """Форматирует вопрос для вывода пользователю."""
    text = f"🎤 **Вопрос {question_number} из {total}**\n\n"
    text += f"{q['question']}\n\n"
    
    if q.get("type") == "multiple_choice" and q.get("options"):
        for i, opt in enumerate(q["options"], 1):
            text += f"{i}. {opt}\n"
        text += "\n💡 Напишите номер варианта (1, 2, 3...) или свой ответ."
    else:
        text += "💡 Ответьте кратко, 1-2 предложениями."
    
    return text


async def evaluate_answer(question: Dict, user_answer: str) -> Dict[str, Any]:
    """
    Оценивает ответ пользователя.
    
    🔧 Оптимизация: multiple_choice — код (0 токенов), short_text — 8B с лимитом 300 токенов.
    """
    q_type = question.get("type", "short_text")
    
    # ✅ multiple_choice — ТОЛЬКО код (быстро, дёшево, надёжно)
    if q_type == "multiple_choice":
        try:
            user_choice = int(user_answer.strip()) - 1
            correct_idx = question.get("correct_option_index")
            
            if correct_idx is not None and 0 <= correct_idx < len(question.get("options", [])):
                is_correct = (user_choice == correct_idx)
                return {
                    "correct": is_correct,
                    "score": 100 if is_correct else 0,
                    "feedback": "✅ Верно!" if is_correct else "❌ Попробуйте ещё раз",
                    "hint": question.get("expected_answer"),
                    "eval_method": "rule_based"
                }
            else:
                return {
                    "correct": False, "score": 0,
                    "feedback": "⚠️ Неверный номер варианта",
                    "hint": f"Выберите число от 1 до {len(question.get('options', []))}",
                    "eval_method": "rule_based"
                }
        except (ValueError, IndexError, TypeError):
            return {
                "correct": False, "score": 0,
                "feedback": "⚠️ Введите номер варианта (1, 2, 3...) или напишите ответ текстом",
                "hint": None,
                "eval_method": "rule_based"
            }
    
    # ✅ short_text — LLM 8B с жёстким лимитом токенов
    llm = _get_llm(use_smart=False)  # 🔧 Всегда 8B для оценки
    if not llm:
        return {
            "correct": None, "score": 50,
            "feedback": "✅ Спасибо за ответ!",
            "hint": question.get("expected_answer"),
            "eval_method": "fallback"
        }
    
    try:
        from langchain_core.output_parsers import JsonOutputParser
        parser = JsonOutputParser()
        
        prompt = f"""Оцени ответ кандидата кратко и объективно.

ВОПРОС: {question['question']}
ОЖИДАЕМЫЙ ОТВЕТ: {question.get('expected_answer', '')}
ОТВЕТ: {user_answer}

Верни ТОЛЬКО валидный JSON: {{"score": 0-100, "feedback": "1 предложение", "hint": "подсказка или null"}}
"""
        response = llm.invoke([
            SystemMessage(content="Оценивай кратко. Верни только валидный JSON."),
            HumanMessage(content=prompt)
        ], config={"max_tokens": 300})  # 🔧 Жёсткий лимит для контроля затрат
        
        result = parser.parse(response.content)
        result["correct"] = result.get("score", 0) >= 70
        result["eval_method"] = "llm_8b"
        return result
        
    except Exception as e:
        logger.warning(f"⚠️ Оценка через LLM упала: {e}")
        return {
            "correct": None, "score": 50,
            "feedback": "✅ Спасибо за ответ!",
            "hint": question.get("expected_answer"),
            "eval_method": "fallback"
        }


async def generate_interview_summary(questions: List[Dict], answers_history: List[Dict]) -> str:
    """
    Генерирует итоговый фидбек.
    
    🔧 Оптимизация: шаблон + 8B для небольшой генерации, лимит 500 токенов.
    """
    if not answers_history:
        return "📭 Вы не ответили ни на один вопрос. Попробуйте пройти интервью ещё раз!"
    
    # 🔧 Считаем статистику детерминированно (0 токенов)
    total = len(answers_history)
    answered = sum(1 for a in answers_history if a.get("user_answer"))
    correct = sum(1 for a in answers_history if a.get("evaluation", {}).get("correct") is True)
    avg_score = sum(a.get("evaluation", {}).get("score", 50) for a in answers_history) // max(1, len(answers_history))
    
    # 🔧 Формируем шаблон с подстановкой данных
    strengths = []
    growth_areas = []
    
    for entry in answers_history:
        eval_ = entry.get("evaluation", {})
        if eval_.get("correct") is True:
            strengths.append("✅ Точные ответы на технические вопросы")
        elif eval_.get("score", 50) < 50:
            q_text = next((q.get("question", "") for q in questions if q.get("id") == entry.get("question_id")), "")
            growth_areas.append(f"📚 {q_text[:50]}...")
    
    # 🔧 Уникализируем списки
    strengths = list(dict.fromkeys(strengths))[:3]
    growth_areas = list(dict.fromkeys(growth_areas))[:3]
    
    # 🔧 Если нужно «оживить» текст — используем 8B с жёстким промптом
    llm = _get_llm(use_smart=False)
    if not llm:
        # Fallback: чистый шаблон без LLM
        summary = f"📊 Итоги мини-интервью:\n"
        summary += f"• Отвечено вопросов: {answered}/{total}\n"
        if correct > 0:
            summary += f"• Правильных ответов: {correct}\n"
        summary += f"• Средняя оценка: ~{avg_score}/100\n\n"
        if strengths:
            summary += "✅ Сильные стороны:\n" + "\n".join(f"• {s}" for s in strengths) + "\n\n"
        if growth_areas:
            summary += "📚 Зоны роста:\n" + "\n".join(f"• {g}" for g in growth_areas) + "\n\n"
        summary += "🎯 Рекомендации: Продолжай практиковаться — каждый ответ приближает тебя к цели! 🚀"
        return summary
    
    try:
        prompt = f"""Ты — карьерный консультант. Подведи итоги интервью КРАТКО.

СТАТИСТИКА:
• Отвечено: {answered}/{total} вопросов
• Правильных: {correct}
• Средняя оценка: ~{avg_score}/100

СИЛЬНЫЕ СТОРОНЫ:
{chr(10).join(f'• {s}' for s in strengths) if strengths else '• Пока нет данных'}

ЗОНЫ РОСТА:
{chr(10).join(f'• {g}' for g in growth_areas) if growth_areas else '• Пока нет данных'}

Сформируй итоговое сообщение (макс. 100 слов) в формате:
📊 Итоги: [кратко]
✅ Сильные стороны: [список]
📚 Зоны роста: [список]  
🎯 Рекомендации: [1 предложение]

Отвечай на русском, дружелюбно, без воды.
"""
        response = llm.invoke([
            SystemMessage(content="Отвечай кратко, по шаблону. Макс. 100 слов."),
            HumanMessage(content=prompt)
        ], config={"max_tokens": 500})  # 🔧 Лимит для контроля затрат
        
        return response.content.strip()
        
    except Exception as e:
        logger.warning(f"⚠️ Генерация саммари через LLM упала: {e}")
        # Fallback: чистый шаблон
        summary = f"📊 Итоги мини-интервью:\n"
        summary += f"• Отвечено вопросов: {answered}/{total}\n"
        if correct > 0:
            summary += f"• Правильных ответов: {correct}\n"
        summary += f"• Средняя оценка: ~{avg_score}/100\n\n"
        if strengths:
            summary += "✅ Сильные стороны:\n" + "\n".join(f"• {s}" for s in strengths) + "\n\n"
        if growth_areas:
            summary += "📚 Зоны роста:\n" + "\n".join(f"• {g}" for g in growth_areas) + "\n\n"
        summary += "🎯 Рекомендации: Продолжай практиковаться — каждый ответ приближает тебя к цели! 🚀"
        return summary


async def start_interview(
    message: str,
    current_state: Dict,
    input_state: Dict
) -> Dict[str, Any]:
    """Главная точка входа: обрабатывает запрос на начало интервью."""
    logger.info("🎤 Starting interview flow")
    
    context = extract_interview_context(current_state, input_state)
    
    questions = await generate_interview_questions(
        role=context["role"],
        vacancy_skills=context["vacancy_skills"],
        candidate_skills=context["candidate_skills"],
        count=5
    )
    
    if not questions:
        return {
            "response": "⚠️ Не удалось сгенерировать вопросы. Попробуйте уточнить позицию или навыки.",
            "interview_state": None
        }
    
    first_q = questions[0]
    response_text = f"🎤 Начнём мини-интервью на позицию **{context['role']}**.\n\n"
    response_text += format_interview_question(first_q, question_number=1, total=len(questions))
    
    interview_state = {
        "active": True,
        "questions": questions,
        "current_index": 0,
        "answers_history": [],
        "role": context["role"],
        "vacancy_payload": context["vacancy_payload"]
    }
    
    return {
        "response": response_text,
        "interview_state": interview_state
    }


async def handle_interview_answer(
    user_answer: str,
    interview_state: Dict
) -> Dict[str, Any]:
    """Обрабатывает ответ пользователя на текущий вопрос."""
    if not interview_state or not interview_state.get("active"):
        return {
            "response": "⚠️ Интервью не активно. Начните заново командой «хочу интервью».",
            "interview_state": interview_state,
            "is_finished": True
        }
    
    questions = interview_state.get("questions", [])
    current_idx = interview_state.get("current_index", 0)
    answers_history = interview_state.get("answers_history", [])
    
    if current_idx >= len(questions):
        summary = await generate_interview_summary(questions, answers_history)
        return {
            "response": summary,
            "interview_state": {**interview_state, "active": False},
            "is_finished": True
        }
    
    current_question = questions[current_idx]
    evaluation = await evaluate_answer(current_question, user_answer)
    
    answers_history.append({
        "question_id": current_question.get("id"),
        "user_answer": user_answer,
        "evaluation": evaluation
    })
    
    next_idx = current_idx + 1
    
    if next_idx < len(questions):
        next_q = questions[next_idx]
        response_text = f"✅ {evaluation.get('feedback', '')}"
        if evaluation.get("hint") and not evaluation.get("correct"):
            response_text += f"\n💡 Подсказка: {evaluation['hint']}"
        response_text += f"\n\n{format_interview_question(next_q, question_number=next_idx + 1, total=len(questions))}"
        
        return {
            "response": response_text,
            "interview_state": {
                **interview_state,
                "current_index": next_idx,
                "answers_history": answers_history
            },
            "is_finished": False
        }
    
    summary = await generate_interview_summary(questions, answers_history)
    return {
        "response": summary,
        "interview_state": {**interview_state, "active": False, "answers_history": answers_history},
        "is_finished": True
    }