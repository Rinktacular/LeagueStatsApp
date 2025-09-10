# api/main.py
from fastapi import FastAPI
from api.routes import flexible
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="League Stats API",
    version="0.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # tighten this in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health Check
@app.get("/")
def root():
    return {"status": "ok", "service": "League Stats API"}

# New normalized, single flexible endpoint
app.include_router(flexible.router)
