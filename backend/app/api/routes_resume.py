from fastapi import APIRouter, UploadFile, File, HTTPException
import os
import shutil

router = APIRouter()

UPLOAD_DIR = "data/resumes"


# ================================
# ВРЕМЕННАЯ ФУНКЦИЯ ПАРСИНГА РЕЗЮМЕ
# ================================
# Сейчас это заглушка.
# Потом сюда можно будет подключить:
# - парсер PDF/DOCX
# - LangGraph
# - отдельный resume parser service
def extract_profile_from_resume(file_path: str) -> dict:
    """
    Временная заглушка для извлечения профиля кандидата из резюме.
    Пока возвращает тестовый профиль.
    """

    return {
        "name": "Danila",
        "country": "Netherlands",
        "city": "Amsterdam",
        "relocation": True,
        "grade": "Middle",
        "specialization": "Backend Developer",
        "experience_years": 3,
        "desired_salary": 4000,
        "work_format": ["remote", "hybrid"],
        "foreign_languages": ["English", "Russian"],
        "skills": ["Python", "FastAPI", "PostgreSQL"]
    }


# ================================
# ЗАГРУЗКА РЕЗЮМЕ
# ================================
# Что делает этот endpoint:
# 1. принимает файл
# 2. временно сохраняет его
# 3. извлекает structured profile
# 4. удаляет файл
# 5. возвращает профиль фронту
@router.post("/upload")
def upload_resume(file: UploadFile = File(...)):
    """
    Загружает резюме, извлекает профиль кандидата
    и возвращает его без постоянного хранения.
    """

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    file_path = os.path.join(UPLOAD_DIR, file.filename)

    try:
        # 1. сохраняем файл временно
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 2. извлекаем профиль
        profile = extract_profile_from_resume(file_path)

        # 3. возвращаем structured profile
        return {
            "status": "uploaded",
            "filename": file.filename,
            "profile": profile
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки резюме: {str(e)}")

    finally:
        # 4. удаляем временный файл
        if os.path.exists(file_path):
            os.remove(file_path)