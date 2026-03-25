from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# routers
from app.api import routes_resume, routes_jobs, routes_analysis


app = FastAPI(
    title="AI Career Market Analyzer",
    description="API for resume analysis, market matching, and career simulation",
    version="0.1.0",
)

# CORS (потом лучше ограничить)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: заменить на frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ROUTES ---
app.include_router(routes_resume.router, prefix="/api/v1/resume", tags=["Resume"])
app.include_router(routes_jobs.router, prefix="/api/v1/jobs", tags=["Jobs"])
app.include_router(routes_analysis.router, prefix="/api/v1/analysis", tags=["Analysis"])


# --- SYSTEM ---
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "AI Career Market Analyzer API",
        "version": app.version,
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "ai-career-backend"
    }