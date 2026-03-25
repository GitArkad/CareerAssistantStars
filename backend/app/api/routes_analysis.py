from fastapi import APIRouter
import json
import os

router = APIRouter()

@router.post("/")
def analyze_resume(filename: str):
    file_path = f"uploads/{filename}"

    if not os.path.exists(file_path):
        return {"error": "file not found"}

    with open(file_path, "r") as f:
        data = json.load(f)

    # пока заглушка
    return {
        "match_score": 0.75,
        "missing_skills": ["Docker"],
        "top_jobs": []
    }