import io
from PyPDF2 import PdfReader
from docx import Document

from app.agents.state import AgentState
from app.agents.services.llm import llm_invoke
from app.agents.services.json_utils import safe_json_parse, merge_candidate


def extract_text_from_pdf(file_bytes: bytes) -> str:
    try:
        pdf_stream = io.BytesIO(file_bytes)
        reader = PdfReader(pdf_stream)

        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)

        return "\n".join(pages)
    except Exception:
        return ""


def extract_text_from_docx(file_bytes: bytes) -> str:
    try:
        doc_stream = io.BytesIO(file_bytes)
        doc = Document(doc_stream)

        paragraphs = [p.text for p in doc.paragraphs if p.text]
        return "\n".join(paragraphs)
    except Exception:
        return ""


def extract_text(state: AgentState) -> str:
    if state.get("raw_file_content"):
        file_bytes = state["raw_file_content"]
        file_name = (state.get("file_name") or "").lower()

        if file_name.endswith(".pdf"):
            return extract_text_from_pdf(file_bytes)

        if file_name.endswith(".docx"):
            return extract_text_from_docx(file_bytes)

        try:
            return file_bytes.decode("utf-8", errors="ignore")
        except Exception:
            return ""

    if state.get("user_input"):
        return state["user_input"]

    return ""


def parse_resume_node(state: AgentState) -> AgentState:
    print(">>> PARSE NODE START")

    text = extract_text(state)

    if not text:
        return state

    prompt = f"""
    Извлеки профиль кандидата из текста.

    Верни строго JSON:

    {{
        "name": "",
        "country": "",
        "city": "",
        "relocation": true,
        "grade": "",
        "specialization": "",
        "experience_years": 0,
        "desired_salary": null,
        "work_format": [],
        "foreign_languages": [],
        "skills": []
    }}

    Текст:
    {text}
    """

    print(">>> CALLING LLM...")
    response = llm_invoke(prompt)
    print(">>> LLM DONE")

    parsed = safe_json_parse(response)

    state["candidate"] = merge_candidate(
        state.get("candidate"),
        parsed
    )

    # 🔥 предотвращаем бесконечный цикл
    state["raw_file_content"] = None
    state["user_input"] = None

    return state