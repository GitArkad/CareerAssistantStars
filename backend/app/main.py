from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
load_dotenv(override=True)

# 👇 импорт роутов
from app.api import (
    routes_resume,
    routes_jobs,
    routes_analysis,
    routes_search,
    routes_analytics,
    routes_chat,
)

# 👇 сначала создаём app
app = FastAPI(
    title="AI Career Market Analyzer",
    version="0.1.0"
)

# 👇 потом middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 👇 потом подключаем роуты
app.include_router(routes_resume.router, prefix="/api/v1/resume", tags=["Resume"])
app.include_router(routes_jobs.router, prefix="/api/v1/jobs", tags=["Jobs"])
app.include_router(routes_analysis.router, prefix="/api/v1/analysis", tags=["Analysis"])
app.include_router(routes_search.router, prefix="/api/v1/search", tags=["Search"])
app.include_router(routes_analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(routes_chat.router, prefix="/api/v1", tags=["Chat"])

# 👇 health check
@app.get("/health")
def health():
    return {"status": "ok"}
