# app/agents2/services/resume_parser.py
import os
import json
from typing import Dict
import fitz  # PyMuPDF

from app.agents2.llm_client import run_local_llm

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-large")

def parse_pdf(file_content: bytes, use_smart_model: bool = False) -> Dict:
    """
    Парсит PDF резюме → возвращает CandidateProfile-like dict
    """
    try:
        doc = fitz.open(stream=file_content, filetype="pdf")
        text = "".join(page.get_text() for page in doc)
    except Exception as e:
        print("PDF parsing failed:", e)
        text = ""

    return parse_text(text, use_smart_model=use_smart_model)

def parse_text(text: str, use_smart_model: bool = False) -> Dict:
    """
    Парсит текст через официальный Groq SDK
    """
    prompt = f"""
    Извлеки из резюме следующие поля для CareerAgent:
    name, country, city, relocation (True/False), grade, specialization,
    experience_years, desired_salary, work_format (список), foreign_languages, skills
    Текст резюме:
    {text}
    Ответь строго в формате JSON.
    """

    response = run_local_llm(prompt, use_smart_model=use_smart_model)

    try:
        data = json.loads(response)
    except Exception as e:
        print("LLM parse failed:", e)
        data = {}

    return data