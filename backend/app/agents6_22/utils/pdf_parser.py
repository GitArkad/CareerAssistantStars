import fitz  # PyMuPDF
from typing import Optional

async def parse_pdf(file_bytes: bytes) -> Optional[str]:
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
        return text.strip()
    except Exception as e:
        print(f"PDF parse error: {e}")
        return None