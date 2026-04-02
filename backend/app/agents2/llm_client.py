# app/agents2/llm_client.py
import os
from groq import Groq
from dotenv import load_dotenv
load_dotenv()

# Берём ключ и модель из .env или settings
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_SMART_MODEL = os.getenv("GROQ_SMART_MODEL", "llama-3.3-70b-versatile")

client = Groq(api_key=GROQ_API_KEY)

def run_local_llm(prompt: str, use_smart_model: bool = False) -> str:
    """
    Вызывает официальное Groq API для LLM
    """
    
    model_name = GROQ_SMART_MODEL if use_smart_model else  GROQ_MODEL  # можно добавить поддержку GROQ_SMART_MODEL, если нужно
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content