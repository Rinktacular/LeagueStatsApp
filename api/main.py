import os
from typing import Optional, List, Tuple, Literal
from fastapi import FastAPI, Query, Depends, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from app.schemas.params import CommonQueryParams, _key, canonicalize_display
from app.stats.ci import wilson_ci

def attach_uncertainty(rows: list[dict], warn_n: int) -> list[dict]:
    out = []
    for r in rows:
        n = int(r["n"])
        p = float(r["winrate"])
        lo, hi = wilson_ci(p, n, z=1.96)
        r2 = dict(r)
        r2["wr_ci_low"] = lo
        r2["wr_ci_high"] = hi
        r2["low_sample"] = n < warn_n
        out.append(r2)
    return out


app = FastAPI(title="League Context Matchups API", version="0.1")

# psycopg2/3 compatible
try:
    import psycopg
    PG3 = True
except ImportError:
    import psycopg2 as psycopg
    PG3 = False

load_dotenv()
PG_DSN = os.environ["PG_DSN"]

app = FastAPI(title="League Context Matchups API", version="0.1")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class MatchupRow(BaseModel):
    patch: str
    team_position: str
    champ: str
    opponent: str
    n: int
    winrate: float
    avg_gd10: Optional[float] = None
    avg_xpd10: Optional[float] = None
    avg_cs10: Optional[float] = None
    # smoothing
    smoothed_wr: Optional[float] = None
    # ✅ Uncertainty
    wr_ci_low: Optional[float] = None
    wr_ci_high: Optional[float] = None
    low_sample: Optional[bool] = None
    # optional builds/runes
    mythic_id: Optional[int] = None
    boots_id: Optional[int] = None
    primary_keystone: Optional[int] = None
    secondary_style: Optional[int] = None
    # optional context
    enemy_jungler: Optional[str] = None
    ally_jungler: Optional[str] = None
    my_duo: Optional[str] = None
    opp_duo: Optional[str] = None


class PageMeta(BaseModel):
    limit: int
    offset: int
    total: Optional[int] = None
    next_offset: Optional[int] = None
    prev_offset: Optional[int] = None


class Envelope(BaseModel):
    params: dict
    rows: list[MatchupRow] | list[dict]
    page: PageMeta


def eq_ci(column: str) -> str:
    # punctuation-insensitive + case-insensitive equality
    return f"regexp_replace(lower(trim({column})), '[^a-z0-9]', '', 'g') = %s"


def fetch_rows(sql: str, params: Tuple) -> list[dict]:
    with psycopg.connect(PG_DSN) as con:
        with con.cursor() as cur:
            cur.execute(sql, params)  # pyright: ignore[reportArgumentType]
            cols = [c[0] for c in cur.description]  # pyright: ignore[reportOptionalIterable]
            out: list[dict] = []
            for r in cur.fetchall():
                out.append(dict(zip(cols, r)))
            return out


# ---------- New, strict sort parsing & per-endpoint allowlists ----------
SortKey = Literal["n", "winrate", "gd10", "smoothed_wr"]

def parse_sort(raw: Optional[str], default: Tuple[SortKey, str] = ("n", "desc")) -> Tuple[SortKey, str]:
    """
    Supports: n, -n, winrate, -winrate, gd10, -gd10, smoothed_wr, -smoothed_wr
    Returns (key, 'asc'|'desc') or raises 422 if invalid.
    """
    if not raw or not raw.strip():
        return default
    s = raw.strip().lower()
    direction = "asc"
    if s.startswith("-"):
        direction = "desc"
        s = s[1:]
    if s not in {"n", "winrate", "gd10", "smoothed_wr"}:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid sort '{raw}'. Allowed: n, -n, winrate, -winrate, gd10, -gd10, smoothed_wr, -smoothed_wr",
        )
    return (s, direction)

def order_clause(raw_sort: Optional[str], allowed: set[str]) -> str:
    key, dirn = parse_sort(raw_sort)
    if key not in allowed:
        raise HTTPException(status_code=422, detail=f"Sort '{key}' not allowed for this endpoint")
    mapping = {
        "n": "n",
        "winrate": "winrate",
        "gd10": "avg_gd10",
        "smoothed_wr": "smoothed_wr",
    }
    primary = mapping[key]
    # Stable tie-breaker for deterministic paging
    then = "n desc" if primary != "n" else "winrate desc"
    return f"order by {primary} {dirn}, {then}"


# ---------- Helpers for page math ----------
def resolve_offset(limit: int, offset: int, page: Optional[int]) -> int:
    if page is not None:
        return (page - 1) * limit
    return offset


# ======================== Endpoints ========================

@app.get(
    "/matchup",
    summary="Lane vs Opponent (per-patch aggregates)",
    description=(
        "Returns lane-vs-opponent matchup stats with Bayesian smoothing and Wilson 95% CI.\n\n"
        "**Sorting**: `n`, `-n`, `winrate`, `-winrate`, `gd10`, `-gd10`, `smoothed_wr`, `-smoothed_wr`.\n"
        "**Paging**: use `page` (1-based) or `offset` (0-based)."
    ),
    responses={
        200: {
            "description": "Paginated matchup rows.",
            "content": {
                "application/json": {
                    "examples": {
                        "TopByN": {
                            "summary": "Most sampled matchups (Top lane, Aatrox, Patch 14.15)",
                            "value": {
                                "params": {
                                    "lane": "TOP", "champ": "Aatrox", "opponent": None, "patch": "14.15",
                                    "min_n": 10, "limit": 50, "offset": 0, "page": 1,
                                    "sort": "-n", "alpha": 20, "prior_wr": 0.5, "warn_n": 25
                                },
                                "rows": [
                                    {
                                        "patch": "14.15", "team_position": "TOP",
                                        "champ": "Aatrox", "opponent": "Darius",
                                        "n": 1245, "winrate": 0.514,
                                        "avg_gd10": 105.2, "avg_xpd10": 82.7, "avg_cs10": 74.3,
                                        "smoothed_wr": 0.512,
                                        "wr_ci_low": 0.488, "wr_ci_high": 0.540, "low_sample": False
                                    }
                                ],
                                "page": {"limit": 50, "offset": 0, "total": 9876, "next_offset": 50, "prev_offset": None}
                            }
                        },
                        "TopBySmoothedWR": {
                            "summary": "Best smoothed WR mirror (Mid vs Zed)",
                            "value": {
                                "params": {
                                    "lane": "MIDDLE", "champ": "Ahri", "opponent": "Zed", "patch": None,
                                    "min_n": 25, "limit": 25, "offset": 0, "page": 1,
                                    "sort": "-smoothed_wr", "alpha": 20, "prior_wr": 0.5, "warn_n": 25
                                },
                                "rows": [],
                                "page": {"limit": 25, "offset": 0, "total": 0, "next_offset": None, "prev_offset": None}
                            }
                        },
                    }
                }
            }
        },
        422: {"description": "Validation error (e.g., invalid sort key)"}
    },
)
def matchup_basic(
    lane: Optional[str] = Query(
        None, example="TOP"
    ),
    champ: Optional[str] = Query(
        None, example="Aatrox"
    ),
    opponent: Optional[str] = Query(
        None, example="Darius"
    ),
    patch: Optional[str] = Query(
        None, description="e.g., 14.15",
        example="14.15"
    ),
    min_n: int = Query(10, ge=0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    page: Optional[int] = Query(
        None, ge=1, description="1-based page",
        example=1
    ),
    sort: Optional[str] = Query(
        None,
        description="n|-n|winrate|-winrate|gd10|-gd10|smoothed_wr|-smoothed_wr",
        example="-n",
    ),
    alpha: int = Query(20, ge=0, description="Bayesian prior weight"),
    prior_wr: float = Query(0.50, ge=0.0, le=1.0, description="Bayesian prior mean (0..1)"),
    warn_n: int = Query(25, ge=0, description="Rows with n < warn_n get low_sample=true"),
):
    """
    Base lane-vs-opponent stats (materialized view: lane_matchup_stats)
    Returns an envelope with rows and page metadata.
    """
    where = ["1=1"]
    params: list = []
    if lane:
        where.append("team_position = %s"); params.append(lane)
    if champ:
        where.append(eq_ci("champ")); params.append(_key(champ))
    if opponent:
        where.append(eq_ci("opponent")); params.append(_key(opponent))
    if patch:
        where.append("patch = %s"); params.append(patch)

    # ✅ enforce min_n
    where.append("n >= %s"); params.append(min_n)

    order_sql = order_clause(sort, {"n", "winrate", "gd10", "smoothed_wr"})
    eff_offset = resolve_offset(limit, offset, page)

    sql = f"""
      select
        count(*) over() as __total__,
        patch, team_position, champ, opponent, n,
        winrate, avg_gd10, avg_xpd10, avg_cs10,
        ((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr
      from lane_matchup_stats
      where {' and '.join(where)}
      {order_sql}
      limit %s offset %s
    """
    q_params = [prior_wr, alpha, alpha, *params, limit, eff_offset]
    rows = fetch_rows(sql, tuple(q_params))
    rows = attach_uncertainty(rows, warn_n)

    total = rows[0]["__total__"] if rows else 0
    for r in rows:
        r.pop("__total__", None)

    page_meta = {
        "limit": limit,
        "offset": eff_offset,
        "total": total,
        "next_offset": eff_offset + len(rows) if len(rows) == limit else None,
        "prev_offset": max(eff_offset - limit, 0) if eff_offset > 0 else None,
    }

    return {
        "params": {
            "lane": lane, "champ": champ, "opponent": opponent, "patch": patch,
            "min_n": min_n, "limit": limit, "offset": eff_offset, "page": page,
            "sort": sort, "alpha": alpha, "prior_wr": prior_wr, "warn_n": warn_n,
        },
        "rows": rows,
        "page": page_meta,
    }


@app.get(
    "/ctx/lane_jg",
    summary="Lane + Enemy Jungler context view",
    description=(
        "Contextual matchup stats conditioned on **lane** + **enemy jungler** (optionally ally jungler). "
        "Returns builds when `include_builds=true`."
    ),
    responses={
        200: {
            "description": "Paginated contextual rows.",
            "content": {
                "application/json": {
                    "examples": {
                        "Basic": {
                            "summary": "Mid Ahri vs Zed with Elise ganking",
                            "value": {
                                "params": {
                                    "lane": "MIDDLE", "champ": "Ahri", "opponent": "Zed",
                                    "enemy_jungler": "Elise", "ally_jungler": None, "patch": "14.15",
                                    "min_n": 10, "limit": 50, "offset": 0, "page": 1,
                                    "sort": "-n", "alpha": 20, "prior_wr": 0.5, "warn_n": 25,
                                    "include_builds": True
                                },
                                "rows": [
                                    {
                                        "patch": "14.15", "lane": "MIDDLE",
                                        "champ": "Ahri", "opponent": "Zed",
                                        "enemy_jungler": "Elise", "ally_jungler": "Lee Sin",
                                        "n": 212, "winrate": 0.547,
                                        "wr_ci_low": 0.478, "wr_ci_high": 0.613, "low_sample": False,
                                        "avg_gd10": 65.1, "avg_xpd10": 44.0, "avg_cs10": 81.3,
                                        "smoothed_wr": 0.540,
                                        "mythic_id": 6655, "boots_id": 3020,
                                        "primary_keystone": 8112, "secondary_style": 8200
                                    }
                                ],
                                "page": {"limit": 50, "offset": 0, "total": 212, "next_offset": None, "prev_offset": None}
                            }
                        },
                        "NoBuilds": {
                            "summary": "Top Aatrox vs Darius, enemy JG Jarvan (no builds)",
                            "value": {
                                "params": {
                                    "lane": "TOP", "champ": "Aatrox", "opponent": "Darius",
                                    "enemy_jungler": "Jarvan IV", "ally_jungler": None, "patch": None,
                                    "min_n": 25, "limit": 25, "offset": 0, "page": 1,
                                    "sort": "-smoothed_wr", "alpha": 20, "prior_wr": 0.5,
                                    "warn_n": 25, "include_builds": False
                                },
                                "rows": [],
                                "page": {"limit": 25, "offset": 0, "total": 0, "next_offset": None, "prev_offset": None}
                            }
                        },
                    }
                }
            }
        },
        422: {"description": "Validation error (e.g., invalid sort or params)"}
    },
)
def ctx_lane_vs_enemyjg(
    params: CommonQueryParams = Depends(),
    page: Optional[int] = Query(
        None, ge=1, description="1-based page",
        example=1
    ),
):
    table = "ctx_lane_vs_enemyjg_builds" if params.include_builds else "ctx_lane_vs_enemyjg"

    select_cols = (
        "count(*) over() as __total__, "
        "patch, team_position, champ, opponent, enemy_jungler, ally_jungler, n, "
        "winrate, avg_gd10, avg_xpd10, avg_cs10, "
        "((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr"
    )
    if params.include_builds:
        select_cols += ", mythic_id, boots_id, primary_keystone, secondary_style"

    where, sql_params = ["1=1"], []
    if params.lane:
        where.append("team_position = %s"); sql_params.append(params.lane)
    if params.champ:
        where.append(eq_ci("champ"));  sql_params.append(_key(params.champ))
    if params.opponent:
        where.append(eq_ci("opponent")); sql_params.append(_key(params.opponent))
    if params.enemy_jungler:
        where.append(eq_ci("enemy_jungler")); sql_params.append(_key(params.enemy_jungler))
    if params.ally_jungler:
        where.append(eq_ci("ally_jungler")); sql_params.append(_key(params.ally_jungler))
    if params.patch:
        where.append("patch = %s"); sql_params.append(params.patch)

    # Keep your existing hard filter
    where.append("n >= %s"); sql_params.append(params.min_n)

    order_sql = order_clause(params.sort, {"n", "winrate", "gd10", "smoothed_wr"})
    eff_offset = resolve_offset(params.limit, params.offset, page)

    sql = f"""
      select {select_cols}
      from {table}
      where {' and '.join(where)}
      {order_sql}
      limit %s offset %s
    """
    full_params = [params.prior_wr, params.alpha, params.alpha, *sql_params, params.limit, eff_offset]
    raw_rows = fetch_rows(sql, tuple(full_params))

    if raw_rows and not isinstance(raw_rows[0], dict):
        raise RuntimeError("fetch_rows must return dict-like rows")

    # Post-process: Wilson CI + low_sample flag
    rows = []
    total = raw_rows[0]["__total__"] if raw_rows else 0
    for r in raw_rows:
        n = int(r["n"])
        p = float(r["winrate"])
        ci_low, ci_high = wilson_ci(p, n, z=1.96)
        rows.append({
            "patch": r["patch"],
            "lane": r["team_position"],  # keep existing key for UI compatibility
            "champ": r["champ"],
            "opponent": r["opponent"],
            "enemy_jungler": r["enemy_jungler"],
            "ally_jungler": r["ally_jungler"],
            "n": n,
            "winrate": p,
            "wr_ci_low": ci_low,
            "wr_ci_high": ci_high,
            "low_sample": n < params.warn_n,
            "avg_gd10": r["avg_gd10"],
            "avg_xpd10": r["avg_xpd10"],
            "avg_cs10": r["avg_cs10"],
            "smoothed_wr": r["smoothed_wr"],
            **({
                "mythic_id": r.get("mythic_id"),
                "boots_id": r.get("boots_id"),
                "primary_keystone": r.get("primary_keystone"),
                "secondary_style": r.get("secondary_style"),
            } if params.include_builds else {}),
        })

    page_meta = {
        "limit": params.limit,
        "offset": eff_offset,
        "total": total,
        "next_offset": eff_offset + len(rows) if len(rows) == params.limit else None,
        "prev_offset": max(eff_offset - params.limit, 0) if eff_offset > 0 else None,
    }

    return {
        "params": { **params.model_dump(), "alpha": params.alpha, "prior_wr": params.prior_wr, "offset": eff_offset, "page": page },
        "rows": rows,
        "page": page_meta,
    }


@app.get(
    "/ctx/bot2v2",
    summary="Bot 2v2 context view (ADC+SUP vs ADC+SUP)",
    description=(
        "Returns bot-lane duo vs duo context. Supports builds when `include_builds=true`."
    ),
    responses={
        200: {
            "description": "Paginated duo context rows.",
            "content": {
                "application/json": {
                    "examples": {
                        "DuoVsDuo": {
                            "summary": "Ashe+Brand vs Jinx+Thresh, patch 14.15, sorted by WR",
                            "value": {
                                "params": {
                                    "champ": "Ashe", "my_duo": "Brand",
                                    "opponent": "Jinx", "opp_duo": "Thresh",
                                    "patch": "14.15", "min_n": 10, "limit": 50, "offset": 0, "page": 1,
                                    "sort": "-winrate", "alpha": 20, "prior_wr": 0.5,
                                    "include_builds": True, "warn_n": 25
                                },
                                "rows": [
                                    {
                                        "patch": "14.15", "team_position": "BOTTOM",
                                        "champ": "Ashe", "my_duo": "Brand",
                                        "opponent": "Jinx", "opp_duo": "Thresh",
                                        "n": 163, "winrate": 0.564,
                                        "wr_ci_low": 0.489, "wr_ci_high": 0.634, "low_sample": False,
                                        "avg_gd10": 78.3, "avg_xpd10": 55.2, "avg_cs10": 89.9,
                                        "smoothed_wr": 0.556,
                                        "mythic_id": 6671, "boots_id": 3006,
                                        "primary_keystone": 8008, "secondary_style": 8300
                                    }
                                ],
                                "page": {"limit": 50, "offset": 0, "total": 163, "next_offset": None, "prev_offset": None}
                            }
                        },
                        "LargeSample": {
                            "summary": "Filter by large sample size and sort by N",
                            "value": {
                                "params": {
                                    "champ": "Caitlyn", "my_duo": None, "opponent": None, "opp_duo": None,
                                    "patch": None, "min_n": 100, "limit": 25, "offset": 0, "page": 1,
                                    "sort": "-n", "alpha": 20, "prior_wr": 0.5,
                                    "include_builds": False, "warn_n": 25
                                },
                                "rows": [],
                                "page": {"limit": 25, "offset": 0, "total": 0, "next_offset": None, "prev_offset": None}
                            }
                        }
                    }
                }
            }
        },
        422: {"description": "Validation error"},
    },
)
def ctx_bot_2v2(
    champ: Optional[str] = Query(
        None, example="Ashe"
    ),
    my_duo: Optional[str] = Query(
        None, example="Brand"
    ),
    opponent: Optional[str] = Query(
        None, example="Jinx"
    ),
    opp_duo: Optional[str] = Query(
        None, example="Thresh"
    ),
    patch: Optional[str] = Query(
        None, description="e.g., 14.15",
        example="14.15"
    ),
    min_n: int = Query(10, ge=0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    page: Optional[int] = Query(
        None, ge=1, description="1-based page",
        example=1
    ),
    sort: Optional[str] = Query(
        None,
        description="n|-n|winrate|-winrate|gd10|-gd10|smoothed_wr|-smoothed_wr",
        example="-winrate"
    ),
    alpha: int = Query(20, ge=0),
    prior_wr: float = Query(0.50, ge=0.0, le=1.0),
    include_builds: bool = True,
    warn_n: int = Query(25, ge=0),
):
    table = "ctx_bot_2v2_builds" if include_builds else "ctx_bot_2v2"

    select_cols = (
        "count(*) over() as __total__, "
        "patch, team_position, champ, my_duo, opponent, opp_duo, n, "
        "winrate, avg_gd10, avg_xpd10, avg_cs10, "
        "((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr"
    )
    if include_builds:
        select_cols += ", mythic_id, boots_id, primary_keystone, secondary_style"

    where, params_list = ["1=1"], []
    if champ:
        where.append(eq_ci("champ")); params_list.append(_key(champ))
    if my_duo:
        where.append(eq_ci("my_duo")); params_list.append(_key(my_duo))
    if opponent:
        where.append(eq_ci("opponent")); params_list.append(_key(opponent))
    if opp_duo:
        where.append(eq_ci("opp_duo")); params_list.append(_key(opp_duo))
    if patch:
        where.append("patch = %s"); params_list.append(patch)
    where.append("n >= %s"); params_list.append(min_n)

    order_sql = order_clause(sort, {"n", "winrate", "gd10", "smoothed_wr"})
    eff_offset = resolve_offset(limit, offset, page)

    sql = f"""
      select {select_cols}
      from {table}
      where {' and '.join(where)}
      {order_sql}
      limit %s offset %s
    """
    q_params = [prior_wr, alpha, alpha, *params_list, limit, eff_offset]
    rows = fetch_rows(sql, tuple(q_params))
    total = rows[0]["__total__"] if rows else 0
    for r in rows:
        r.pop("__total__", None)

    rows = attach_uncertainty(rows, warn_n)

    page_meta = {
        "limit": limit,
        "offset": eff_offset,
        "total": total,
        "next_offset": eff_offset + len(rows) if len(rows) == limit else None,
        "prev_offset": max(eff_offset - limit, 0) if eff_offset > 0 else None,
    }

    return {
        "params": {
            "champ": champ, "my_duo": my_duo, "opponent": opponent, "opp_duo": opp_duo,
            "patch": patch, "min_n": min_n, "limit": limit, "offset": eff_offset, "page": page,
            "sort": sort, "alpha": alpha, "prior_wr": prior_wr, "include_builds": include_builds, "warn_n": warn_n,
        },
        "rows": rows,
        "page": page_meta,
    }


def fetch_values(sql: str, params: Tuple) -> List[str]:
    """Helper: return a flat list[str] from a single-column query."""
    with psycopg.connect(PG_DSN) as con:
        with con.cursor() as cur:
            cur.execute(sql, params)  # pyright: ignore[reportArgumentType]
            return [row[0] for row in cur.fetchall()]


@app.get("/champions")
def list_champions(
    search: Optional[str] = Query(None, description="case/ punctuation-insensitive contains"),
    limit: int = Query(50, ge=1, le=200),
):
    base_sql = """
      with names as (
        select champ as name from lane_matchup_stats
        union select champ from ctx_lane_vs_enemyjg
        union select opponent from ctx_lane_vs_enemyjg
        union select enemy_jungler from ctx_lane_vs_enemyjg
        union select ally_jungler from ctx_lane_vs_enemyjg
        union select champ from ctx_bot_2v2
        union select my_duo from ctx_bot_2v2
        union select opponent from ctx_bot_2v2
        union select opp_duo from ctx_bot_2v2
      )
      select distinct name
      from names
      where name is not null
        {where}
      order by name
      limit %s
    """
    where, params = "", []
    if search:
        where = "and regexp_replace(lower(name), '[^a-z0-9]', '', 'g') like %s"
        params.append(f"%{_key(search)}%")
    params.append(limit)

    sql = base_sql.format(where=where)
    raw = fetch_values(sql, tuple(params))

    # map to canonical display & dedupe by normalized key
    seen, items = set(), []
    for n in raw:
        disp = canonicalize_display(n)
        k = _key(disp)
        if k not in seen:
            seen.add(k)
            items.append(disp)

    return {"items": items, "count": len(items), "search": search, "limit": limit}

# main API setup (same file where you have: app = FastAPI(...))
from app.meta.ddragon import DD  # noqa: E402
from app.meta.router import router as meta_router  # noqa: E402

@app.on_event("startup")
async def _boot_meta():
    # Warm to latest version so the UI has data on first load
    await DD.refresh()

# mount the admin router at the end (order doesn’t really matter)
from . import admin_refresh  # noqa: E402
app.include_router(meta_router)
app.include_router(admin_refresh.router)
