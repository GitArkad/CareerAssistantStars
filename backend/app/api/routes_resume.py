from fastapi import APIRouter, UploadFile, File
import shutil
import os

router = APIRouter()

UPLOAD_DIR = "data/resumes"

# ================================
# ЗАГРУЗКА РЕЗЮМЕ
# ================================
@router.post("/upload")
def upload_resume(file: UploadFile = File(...)):
    """
    Загружает файл резюме
    """

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    file_path = os.path.join(UPLOAD_DIR, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "status": "uploaded",
        "file": file.filename
    }