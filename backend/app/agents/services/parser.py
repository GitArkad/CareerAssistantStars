import io
import fitz  # PyMuPDF
from docx import Document
from pathlib import Path

class ResumeParser:
    @staticmethod
    def parse(file_contents: bytes | str, filename: str = "raw_text") -> str | None:
        # Если пришла сразу строка (скопированный текст), просто возвращаем её
        if isinstance(file_contents, str):
            return file_contents.strip()

        # Если пришли байты, определяем расширение
        ext = Path(filename).suffix.lower()
        
        try:
            if ext == ".pdf":
                with fitz.open(stream=file_contents, filetype="pdf") as doc:
                    return "".join(page.get_text() for page in doc)

            elif ext == ".docx":
                doc = Document(io.BytesIO(file_contents))
                return "\n".join(p.text for p in doc.paragraphs)

            # Если это .txt или мы явно пометили вход как "text"
            elif ext in [".txt", ""] or filename == "raw_text":
                return file_contents.decode("utf-8", errors="ignore")

            return None
        except Exception as e:
            print(f"Parsing error for {filename}: {e}")
            return None