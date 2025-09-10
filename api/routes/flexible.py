# api/routes/flexible.py
from __future__ import annotations
import os
import json
from dataclasses import dataclass
from typing import Optional, List, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

load_dotenv()

# ----------------------------
# Config / PG connection pool
# ----------------------------
def build_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD")  # required if server enforces password
    dbname = os.getenv("DB_NAME", "league")
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return f"dbname={dbname} user={user} host={host} port={port}"

PG_DSN = build_dsn()
SQL_PATH = os.getenv(
    "FLEX_SQL_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "sql", "flexible_filters.sql")
)

_pool: ConnectionPool | None = None
def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            PG_DSN, min_size=1, max_size=10,
            timeout=10,  # wait up to 10s for a conn
            kwargs={"connect_timeout": 5}
        )
    return _pool

# ----------------------------
# Load & split SQL file
# ----------------------------
@dataclass
class SqlBundle:
    agg_summary: str
    top_items: str

def load_sql_bundle(path: str) -> SqlBundle:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    parts: Dict[str, str] = {}
    current_name = None
    buf: List[str] = []
    for line in text.splitlines():
        if line.strip().lower().startswith("-- name:"):
            if current_name and buf:
                parts[current_name] = "\n".join(buf).strip()
                buf = []
            current_name = line.split(":", 1)[1].strip()
        else:
            buf.append(line)
    if current_name and buf:
        parts[current_name] = "\n".join(buf).strip()

    if "agg_summary" not in parts or "top_items" not in parts:
        raise RuntimeError("flexible_filters.sql must contain queries named 'agg_summary' and 'top_items'")

    return SqlBundle(agg_summary=parts["agg_summary"], top_items=parts["top_items"])

SQL = load_sql_bundle(SQL_PATH)

# ----------------------------
# Models
# ----------------------------
class RoleFilter(BaseModel):
    role: Optional[str] = Field(None, description="MID, JUNGLE, TOP, BOT_CARRY, SUPPORT")
    champ_id: Optional[int] = Field(None, description="Riot champion ID")

class FlexibleBody(BaseModel):
    # NEW: explicit subject (your pick)
    subject: Optional[RoleFilter] = None
    patch: Optional[str] = None
    skill_tier: Optional[str] = None
    minute: int = 10
    min_n: int = 20
    ally_filters: List[RoleFilter] = []
    enemy_filters: List[RoleFilter] = []

# ----------------------------
# Router
# ----------------------------
router = APIRouter(prefix="/stats", tags=["stats"])

@router.post("/flexible")
def flexible(body: FlexibleBody):
    # Back-compat & validation:
    # If subject is missing, use the first ally filter as subject (if any).
    subject = body.subject
    extra_allies: List[RoleFilter] = body.ally_filters

    if subject is None:
        if not extra_allies:
            raise HTTPException(status_code=400, detail="Provide `subject` or at least one ally filter.")
        subject = extra_allies[0]
        extra_allies = extra_allies[1:]

    # Ensure subject has at least role or champ_id populated
    if (subject.role is None or subject.role.strip() == "") and subject.champ_id is None:
        raise HTTPException(status_code=400, detail="Subject must include role and/or champ_id.")

    params = {
        "patch": body.patch,
        "skill_tier": body.skill_tier,
        "minute": body.minute,
        "min_n": body.min_n,
        "subject": json.dumps(subject.dict()),
        "ally_filters": json.dumps([f.dict() for f in extra_allies]),
        "enemy_filters": json.dumps([f.dict() for f in body.enemy_filters]),
    }

    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # summary
            cur.execute(sql.SQL(SQL.agg_summary), params)  # type: ignore[arg-type]
            row = cur.fetchone()
            if not row:
                return {
                    "summary": {"n_games": 0, "winrate": 0.0, "gold_at_min": 0.0, "xp_at_min": 0.0},
                    "top_items": []
                }
            n_games, winrate, gold_at_min, xp_at_min = row

            # top items
            cur.execute(sql.SQL(SQL.top_items), params)  # type: ignore[arg-type]
            items = [
                {"item_id": item_id, "item_name": item_name, "picks": int(picks)}
                for item_id, item_name, picks in cur.fetchall()
            ]

            return {
                "summary": {
                    "n_games": int(n_games),
                    "winrate": float(winrate),
                    "gold_at_min": float(gold_at_min),
                    "xp_at_min": float(xp_at_min),
                },
                "top_items": items,
            }
