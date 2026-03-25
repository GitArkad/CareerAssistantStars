"""
Клиент для взаимодействия с API
"""

import requests
from typing import Dict, List, Optional, Any
from datetime import datetime


class APIClient:
    """Клиент для работы с backend API"""
    
    def __init__(self, base_url: str, timeout: int = 30):
        """
        Инициализация API клиента
        
        Args:
            base_url: Базовый URL API
            timeout: Таймаут запросов в секундах
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
    
    def check_health(self) -> bool:
        """Проверить доступность API"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=self.timeout
            )
            return response.status_code == 200
        except:
            return False
    
    def get_vacancies(
        self,
        location: Optional[str] = None,
        experience: Optional[str] = None,
        salary_min: Optional[int] = None,
        search_query: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Получить список вакансий
        
        Args:
            location: Локация
            experience: Уровень опыта
            salary_min: Минимальная зарплата
            search_query: Поисковый запрос
            limit: Лимит результатов
        
        Returns:
            Список вакансий
        """
        params = {
            'limit': limit,
        }
        
        if location:
            params['location'] = location
        if experience:
            params['experience'] = experience
        if salary_min:
            params['salary_min'] = salary_min
        if search_query:
            params['query'] = search_query
        
        try:
            response = self.session.get(
                f"{self.base_url}/vacancies",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json().get('vacancies', [])
        except Exception as e:
            print(f"Error fetching vacancies: {e}")
            return []
    
    def analyze_resume(self, resume_text: str) -> Dict:
        """
        Проанализировать резюме
        
        Args:
            resume_text: Текст резюме
        
        Returns:
            Результаты анализа
        """
        try:
            response = self.session.post(
                f"{self.base_url}/resume/analyze",
                json={'text': resume_text},
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error analyzing resume: {e}")
            return {}
    
    def get_interview_questions(
        self,
        job_title: str,
        experience_level: str,
        tech_stack: List[str]
    ) -> List[Dict]:
        """
        Получить вопросы для интервью
        
        Args:
            job_title: Должность
            experience_level: Уровень опыта
            tech_stack: Технологический стек
        
        Returns:
            Список вопросов
        """
        try:
            response = self.session.post(
                f"{self.base_url}/interview/questions",
                json={
                    'job_title': job_title,
                    'experience_level': experience_level,
                    'tech_stack': tech_stack
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json().get('questions', [])
        except Exception as e:
            print(f"Error getting interview questions: {e}")
            return []
    
    def evaluate_answer(
        self,
        question: str,
        answer: str,
        expected_skills: List[str]
    ) -> Dict:
        """
        Оценить ответ на вопрос интервью
        
        Args:
            question: Вопрос
            answer: Ответ кандидата
            expected_skills: Ожидаемые навыки
        
        Returns:
            Оценка и обратная связь
        """
        try:
            response = self.session.post(
                f"{self.base_url}/interview/evaluate",
                json={
                    'question': question,
                    'answer': answer,
                    'expected_skills': expected_skills
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error evaluating answer: {e}")
            return {'score': 0, 'feedback': 'Ошибка оценки'}
    
    def get_market_analytics(
        self,
        period: str = '30d',
        location: Optional[str] = None
    ) -> Dict:
        """
        Получить аналитику рынка
        
        Args:
            period: Период ('7d', '30d', '90d', '1y')
            location: Локация
        
        Returns:
            Данные аналитики
        """
        params = {'period': period}
        if location:
            params['location'] = location
        
        try:
            response = self.session.get(
                f"{self.base_url}/analytics/market",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting analytics: {e}")
            return {}
    
    def get_salary_range(
        self,
        job_title: str,
        experience: str,
        location: Optional[str] = None
    ) -> Dict:
        """
        Получить диапазон зарплат для позиции
        
        Args:
            job_title: Должность
            experience: Уровень опыта
            location: Локация
        
        Returns:
            Диапазон зарплат
        """
        params = {
            'job_title': job_title,
            'experience': experience
        }
        if location:
            params['location'] = location
        
        try:
            response = self.session.get(
                f"{self.base_url}/analytics/salary",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting salary range: {e}")
            return {'min': 0, 'max': 0, 'avg': 0}


# Глобальный экземпляр клиента (ленивая инициализация)
_api_client: Optional[APIClient] = None


def get_api_client(base_url: str) -> APIClient:
    """
    Получить или создать экземпляр API клиента
    
    Args:
        base_url: Базовый URL API
    
    Returns:
        Экземпляр APIClient
    """
    global _api_client
    if _api_client is None or _api_client.base_url != base_url:
        _api_client = APIClient(base_url)
    return _api_client