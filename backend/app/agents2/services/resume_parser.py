import json
import re
from app.agents2.llm_client import run_local_llm
from PyPDF2 import PdfReader
import io

from app.agents2.utils.normalizers import normalize_city, normalize_country


def clean_json(text: str) -> str:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else "{}"


def parse_text(text: str, use_smart_model: bool = False) -> dict:
    prompt = f"""
Ты — AI парсер резюме.

Верни ТОЛЬКО JSON без лишнего текста.

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

    response = run_local_llm(prompt, use_smart_model=use_smart_model)

    try:
        json_text = clean_json(response)
        data = json.loads(json_text)

        if not isinstance(data.get("skills"), list):
            data["skills"] = []

        # 🔥 нормализация
        data["city_normalized"] = normalize_city(data.get("city"))
        data["country_normalized"] = normalize_country(data.get("country"))

        return data

    except Exception as e:
        print("❌ JSON parse error:", e)
        print("RAW RESPONSE:", response)

        return {
            "skills": ["python"],
            "city": None,
            "city_normalized": None,
            "country": None,
            "country_normalized": None,
            "specialization": None
        }


def parse_pdf(file_bytes: bytes, use_smart_model: bool = False) -> dict:
    reader = PdfReader(io.BytesIO(file_bytes))

    text = ""
    for page in reader.pages:
        text += page.extract_text() or ""

    return parse_text(text, use_smart_model=use_smart_model)