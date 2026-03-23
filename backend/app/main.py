from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="AI Career Market Analyzer",
    description="API for resume analysis, market matching, and career simulation",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "service": "AI Career Market Analyzer API"}


@app.get("/health")
def health():
    return {"status": "healthy"}
