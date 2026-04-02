from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.agents6_2.resume_parser import parse_resume_from_pdf, parse_resume_from_text
from app.agents6_2.state import CandidateProfile

router = APIRouter()


def _normalize_candidate_payload(profile: Dict[str, Any]) -> Dict[str, Any]:
    try:
        candidate = CandidateProfile(
            **{k: v for k, v in profile.items() if k in CandidateProfile.model_fields}
        )
        return candidate.model_dump()
    except Exception:
        fallback = CandidateProfile()
        candidate_data = fallback.model_dump()
        candidate_data.update({k: v for k, v in profile.items() if k in candidate_data})
        return candidate_data


@router.post("/upload")
async def upload_resume(file: UploadFile = File(...)):
    """
    Загружает резюме, извлекает профиль кандидата
    и возвращает его вместе с backend state.
    """
    try:
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Файл пустой")

        suffix = Path(file.filename or "").suffix.lower()

        if suffix == ".pdf" or file.content_type == "application/pdf":
            profile = await parse_resume_from_pdf(file_bytes)
        elif suffix in {".txt", ".md"} or (file.content_type or "").startswith("text/"):
            text = file_bytes.decode("utf-8", errors="ignore")
            profile = await parse_resume_from_text(text)
        else:
            raise HTTPException(
                status_code=400,
                detail="Поддерживаются только PDF и текстовые файлы для анализа резюме.",
            )

        candidate = _normalize_candidate_payload(profile or {})
        state = {
            "candidate": candidate,
            "candidate_resume": "",
            "thread_id": None,
        }

        return {
            "status": "uploaded",
            "filename": file.filename,
            "profile": candidate,
            "state": state,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки резюме: {str(e)}")
