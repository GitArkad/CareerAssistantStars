# utils.py

import os
from dotenv import load_dotenv

from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from app.agents.services.schemas import ExtractionSchema

# Загружаем переменные из .env
load_dotenv()

def get_extraction_chain(text: str):
    api_key = os.getenv("GROQ_API_KEY")
    if api_key is None:
        raise ValueError("GROQ_API_KEY не определен")
    
    GROQ_SMART_MODEL = os.getenv("GROQ_SMART_MODEL", "llama-3.1-8b-instant")
    llm = ChatGroq(
        model=GROQ_SMART_MODEL, # модель для парсинга резюме
        api_key=api_key,
        temperature=0 
    )
    
    structured_llm = llm.with_structured_output(ExtractionSchema)

    # МАКСИМАЛЬНО простой промпт. Чем меньше текста, тем стабильнее 8b вызывает Tool.
      
    prompt = ChatPromptTemplate.from_template("""
        Ты — эксперт по анализу резюме в сфере IT, Data Engineering, Machine Learning и Data Science.
        Твоя задача: структурировать информацию из текста резюме.

        ПРАВИЛА ИЗВЛЕЧЕНИЯ:
        1. Если текст резюме пустой — оставь все поля пустыми или со значениями по умолчанию (None/[]).
        2. НИКОГДА не выдумывай данные. Если в тексте нет города, пиши None. Если нет зарплаты, пиши None.
        3. Внимательно собери ВСЕ библиотеки и инструменты (Pandas, PyTorch, SQL, Docker и т.д.) из всех разделов: навыки, опыт работы, описание проектов.
        4. 'skills' — это единый список, куда должны попасть: языки программирования, фреймворки, базы данных и инструменты.

        ТЕКСТ РЕЗЮМЕ:
        {text}
    """)

    chain = prompt | structured_llm
    
    return chain.invoke({"text": text})