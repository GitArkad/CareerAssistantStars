import os
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from .services.schemas import ExtractionSchema

def get_extraction_chain(text: str):
    llm = ChatGroq(model=os.getenv("GROQ_SMART_MODEL", "llama-3.1-8b-instant"), temperature=0)
    structured_llm = llm.with_structured_output(ExtractionSchema)
    prompt = ChatPromptTemplate.from_template("""Ты эксперт по анализу резюме IT/Data/ML.
Извлеки данные. Не выдумывай. skills — единый список всех технологий.
ТЕКСТ: {text}""")
    return (prompt | structured_llm).invoke({"text": text})