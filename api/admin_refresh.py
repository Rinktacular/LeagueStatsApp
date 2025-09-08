# api/admin_refresh.py
import os
from typing import Optional, List
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

# Match your main.py driver (psycopg3 or 2)
try:
    import psycopg  # psycopg3
except ImportError:
    import psycopg2 as psycopg  # psycopg2

load_dotenv()  # harmless if already loaded elsewhere

router = APIRouter(prefix="/admin", tags=["admin"])

def _get_admin_token() -> str:
    token = os.getenv("ADMIN_TOKEN", "")
    if not token:
        # Surface a clear error instead of crashing on import
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not set")
    return token

def _get_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise HTTPException(status_code=500, detail="PG_DSN not set")
    return dsn

def require_admin(authorization: Optional[str] = Header(None)) -> None:
    token = _get_admin_token()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    if authorization.split(" ", 1)[1] != token:
        raise HTTPException(status_code=403, detail="Forbidden")

DEFAULT_VIEWS: List[str] = [
    "lane_matchup_stats",
    "ctx_lane_vs_enemyjg",
    "ctx_bot_2v2",
    "ctx_lane_vs_enemyjg_builds",
    "ctx_bot_2v2_builds",
]

class RefreshBody(BaseModel):
    views: Optional[List[str]] = None
    analyze_after: bool = True

@router.post("/refresh")
def refresh_materialized_views(body: RefreshBody, _=Depends(require_admin)):
    views = body.views or DEFAULT_VIEWS

    def ident(s: str) -> str:
        return "".join(c for c in s if c.isalnum() or c == "_")

    refreshed: List[str] = []
    errors: List[str] = []

    dsn = _get_dsn()
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            for v in views:
                name = ident(v)
                try:
                    cur.execute(f"refresh materialized view {name};")
                    refreshed.append(name)
                except Exception as e:
                    errors.append(f"{name}: {e}")
            if body.analyze_after:
                for v in views:
                    name = ident(v)
                    try:
                        cur.execute(f"analyze {name};")
                    except Exception:
                        pass
        con.commit()

    return {"refreshed": refreshed, "errors": errors}
