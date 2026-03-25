import requests

BASE_URL = "http://localhost:8501/api/v1"

def upload_resume(file):
    files = {"file": file}
    response = requests.post(f"{BASE_URL}/resume/upload", files=files)
    return response.json()

def analyze_resume(filename):
    response = requests.post(
        f"{BASE_URL}/analysis",
        params={"filename": filename}
    )
    return response.json()