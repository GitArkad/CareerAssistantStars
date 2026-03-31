"""
Клиент для взаимодействия с FastAPI backend
"""

import json
import requests
from typing import Dict, List, Optional, Any


class APIClient:
    """Клиент для работы с backend API"""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def check_health(self) -> bool:
        """Проверить доступность API"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=self.timeout,
            )
            return response.status_code == 200
        except Exception:
            return False

    # =========================================
    # RESUME
    # =========================================
    def upload_resume(self, uploaded_file) -> Dict[str, Any]:
        """
        Отправить резюме в FastAPI.
        FastAPI вернёт structured profile.
        """
        files = {
            "file": (
                uploaded_file.name,
                uploaded_file.getvalue(),
                uploaded_file.type or "application/octet-stream",
            )
        }

        response = self.session.post(
            f"{self.base_url}/api/v1/resume/upload",
            files=files,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def analyze_resume_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Отправить structured profile в analysis endpoint.
        """
        response = self.session.post(
            f"{self.base_url}/api/v1/analysis/resume",
            json=profile,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    # =========================================
    # CHAT / MVP FLOW
    # =========================================
    def chat(
        self,
        message: str = "",
        uploaded_file=None,
        state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Универсальный chat endpoint для MVP flow:
        - search
        - resume
        - roadmap
        - interview
        """
        state = state or {}

        data = {
            "message": message,
            "state": json.dumps(state, ensure_ascii=False),
        }

        files = None
        if uploaded_file is not None:
            files = {
                "file": (
                    uploaded_file.name,
                    uploaded_file.getvalue(),
                    uploaded_file.type or "application/octet-stream",
                )
            }

        response = self.session.post(
            f"{self.base_url}/api/v1/chat",
            data=data,
            files=files,
            timeout=120,
        )
        response.raise_for_status()
        return response.json()

    # =========================================
    # JOBS
    # =========================================
    def get_jobs(
        self,
        country: Optional[str] = None,
        seniority: Optional[str] = None,
        remote: Optional[bool] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        params = {"limit": limit}

        if country:
            params["country"] = country
        if seniority:
            params["seniority"] = seniority
        if remote is not None:
            params["remote"] = remote

        response = self.session.get(
            f"{self.base_url}/api/v1/jobs/",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def semantic_search(self, vector: List[float], limit: int = 5) -> Dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/api/v1/search/semantic",
            json={"vector": vector, "limit": limit},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    # =========================================
    # ANALYTICS
    # =========================================
    def get_salary_analytics(
        self,
        role: Optional[str] = None,
        country: Optional[str] = None,
        seniority: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        params = {"limit": limit}

        if role:
            params["role"] = role
        if country:
            params["country"] = country
        if seniority:
            params["seniority"] = seniority

        response = self.session.get(
            f"{self.base_url}/api/v1/analytics/salary",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_skills_analytics(
        self,
        role: Optional[str] = None,
        country: Optional[str] = None,
        seniority: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        params = {"limit": limit}

        if role:
            params["role"] = role
        if country:
            params["country"] = country
        if seniority:
            params["seniority"] = seniority

        response = self.session.get(
            f"{self.base_url}/api/v1/analytics/skills",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()
    
    
    def chat(self, message: str, state: dict):
        import requests

        response = requests.post(
            f"{self.base_url}/api/v1/chat",
            data={
                "message": message,
                "state": json.dumps(state, ensure_ascii=False),
            },
            timeout=120,
        )

        response.raise_for_status()
        return response.json()