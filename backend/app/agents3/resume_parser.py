import json
import re
import os
from typing import Dict, Any, Optional
from langchain_groq import ChatGroq
from .utils.pdf_parser import parse_pdf
from .utils.city_map import CITY_NORMALIZATION_MAP
from .utils.country_map import COUNTRY_NORMALIZATION_MAP

def normalize_city(city: Optional[str]) -> Optional[str]:
    if not city:
        return None
    normalized = city.lower().strip()
    return CITY_NORMALIZATION_MAP.get(normalized, city.upper())

def normalize_country(country: Optional[str]) -> Optional[str]:
    if not country:
        return None
    normalized = country.lower().strip()
    return COUNTRY_NORMALIZATION_MAP.get(normalized, country.upper())

def clean_json(text: str) -> str:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group() if match else "{}"

async def parse_resume_with_llm(text: str, use_smart_model: bool = True) -> Dict[str, Any]:
    groq_api_key = os.getenv("GROQ_API_KEY")
    model_name = os.getenv("GROQ_SMART_MODEL", "llama-3.3-70b-versatile") if use_smart_model else os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
    llm = ChatGroq(groq_api_key=groq_api_key, model_name=model_name)

    prompt = f"""Ты — AI парсер резюме. Верни ТОЛЬКО JSON без лишнего текста.

Формат:
{{
  "name": "",
  "country": "",
  "city": "",
  "relocation": false,
  "grade": "",
  "specialization": "",
  "experience_years": 0,
  "desired_salary": 0,
  "work_format": [],
  "foreign_languages": [],
  "skills": []
}}

Резюме:
{text[:4000]}
"""

    try:
        response = await llm.ainvoke([{"role": "user", "content": prompt}])
        json_text = clean_json(response.content)
        data = json.loads(json_text)

        if not isinstance(data.get("skills"), list):
            data["skills"] = []

        data["city_normalized"] = normalize_city(data.get("city"))
        data["country_normalized"] = normalize_country(data.get("country"))

        return data
    except Exception as e:
        print("JSON parse error:", e)
        return {
            "skills": ["python"],
            "city": None,
            "city_normalized": None,
            "country": None,
            "country_normalized": None,
            "specialization": None
        }

async def parse_resume_from_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    text = await parse_pdf(pdf_bytes)
    return await parse_resume_with_llm(text or "")

async def parse_resume_from_text(text: str) -> Dict[str, Any]:
    return await parse_resume_with_llm(text)