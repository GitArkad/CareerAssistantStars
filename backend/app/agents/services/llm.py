from groq import Groq
from app.config.settings import GROQ_API_KEY, GROQ_MODEL

client = Groq(api_key=GROQ_API_KEY)


def llm_invoke(prompt: str) -> str:
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content