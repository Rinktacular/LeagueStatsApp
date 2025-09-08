import os
from typing import Optional, List, Tuple
from fastapi import FastAPI, Query
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi import FastAPI, Query

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


def eq_ci(column: str) -> str:
    # case-insensitive + trim on both sides
    return f"lower(trim({column})) = lower(trim(%s))"

def fetch_rows(sql: str, params: Tuple) -> list[MatchupRow]:
    with psycopg.connect(PG_DSN) as con:
        with con.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            out = []
            for r in cur.fetchall():
                d = dict(zip(cols, r))
                out.append(MatchupRow(**d))
            return out

def order_clause(key: Optional[str]) -> str:
    if key == "winrate":
        return "order by winrate desc, n desc"
    if key == "gd10":
        return "order by avg_gd10 desc, n desc"
    if key == "smoothed_wr":
        return "order by smoothed_wr desc, n desc"
    # default: by sample size
    return "order by n desc"


@app.get("/matchup", response_model=list[MatchupRow])
def matchup_basic(
    lane: Optional[str] = None,
    champ: Optional[str] = None,
    opponent: Optional[str] = None,
    patch: Optional[str] = Query(None),
    min_n: int = 10,
    limit: int = 50,
    sort: Optional[str] = Query(None, description="winrate|n|gd10|smoothed_wr"),
    alpha: int = Query(20, ge=0, description="Bayesian prior weight"),
    prior_wr: float = Query(0.50, ge=0.0, le=1.0, description="Bayesian prior mean (0..1)"),
):
    """
    Base lane-vs-opponent stats (materialized view: lane_matchup_stats)
    """
    where = ["1=1"]
    params: list = []
    if lane:
        where.append("team_position = %s"); params.append(lane)
    if champ:
        where.append(eq_ci("champ")); params.append(champ)
    if opponent:
        where.append(eq_ci("opponent")); params.append(opponent)
    if patch:
        where.append("patch = %s"); params.append(patch)

    # ✅ enforce min_n
    where.append("n >= %s"); params.append(min_n)

    # Smoothing: (winrate*n + prior_wr*alpha) / (n + alpha)
    sql = f"""
      select patch, team_position, champ, opponent, n,
             winrate, avg_gd10, avg_xpd10, avg_cs10,
             ((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr
      from lane_matchup_stats
      where {' and '.join(where)}
      {order_clause(sort)}
      limit %s
    """
    params = [prior_wr, alpha, alpha] + params  # prepend smoothing params
    params.append(limit)
    return fetch_rows(sql, tuple(params))


@app.get("/ctx/lane_jg", response_model=list[MatchupRow])
def ctx_lane_vs_enemyjg(
    lane: Optional[str] = None,
    champ: Optional[str] = None,
    opponent: Optional[str] = None,
    enemy_jungler: Optional[str] = None,
    ally_jungler: Optional[str] = None,
    patch: Optional[str] = None,
    min_n: int = 10,
    limit: int = 50,
    sort: Optional[str] = Query(None, description="winrate|n|gd10|smoothed_wr"),
    alpha: int = Query(20, ge=0),
    prior_wr: float = Query(0.50, ge=0.0, le=1.0),
    include_builds: bool = True,
):
    table = "ctx_lane_vs_enemyjg_builds" if include_builds else "ctx_lane_vs_enemyjg"

    select_cols = (
        "patch, team_position, champ, opponent, enemy_jungler, ally_jungler, n, "
        "winrate, avg_gd10, avg_xpd10, avg_cs10, "
        "((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr"
    )
    if include_builds:
        select_cols += ", mythic_id, boots_id, primary_keystone, secondary_style"

    where, params = ["1=1"], []
    if lane:
        where.append("team_position = %s"); params.append(lane)
    if champ:
        where.append(eq_ci("champ")); params.append(champ)
    if opponent:
        where.append(eq_ci("opponent")); params.append(opponent)
    if enemy_jungler:
        where.append(eq_ci("enemy_jungler")); params.append(enemy_jungler)
    if ally_jungler:
        where.append(eq_ci("ally_jungler")); params.append(ally_jungler)
    if patch:
        where.append("patch = %s"); params.append(patch)
    where.append("n >= %s"); params.append(min_n)

    sql = f"""
      select {select_cols}
      from {table}
      where {' and '.join(where)}
      {order_clause(sort)}
      limit %s
    """
    # prepend smoothing args, append limit
    params = [prior_wr, alpha, alpha, *params, limit]
    return fetch_rows(sql, tuple(params))


@app.get("/ctx/bot2v2", response_model=list[MatchupRow])
def ctx_bot_2v2(
    champ: Optional[str] = None,
    my_duo: Optional[str] = None,
    opponent: Optional[str] = None,
    opp_duo: Optional[str] = None,
    patch: Optional[str] = None,
    min_n: int = 10,
    limit: int = 50,
    sort: Optional[str] = Query(None, description="winrate|n|gd10|smoothed_wr"),
    alpha: int = Query(20, ge=0),
    prior_wr: float = Query(0.50, ge=0.0, le=1.0),
    include_builds: bool = True,
):
    table = "ctx_bot_2v2_builds" if include_builds else "ctx_bot_2v2"

    select_cols = (
        "patch, team_position, champ, my_duo, opponent, opp_duo, n, "
        "winrate, avg_gd10, avg_xpd10, avg_cs10, "
        "((winrate * n) + (%s * %s)) / (n + %s) as smoothed_wr"
    )
    if include_builds:
        select_cols += ", mythic_id, boots_id, primary_keystone, secondary_style"

    where, params = ["1=1"], []
    if champ:
        where.append(eq_ci("champ")); params.append(champ)
    if my_duo:
        where.append(eq_ci("my_duo")); params.append(my_duo)
    if opponent:
        where.append(eq_ci("opponent")); params.append(opponent)
    if opp_duo:
        where.append(eq_ci("opp_duo")); params.append(opp_duo)
    if patch:
        where.append("patch = %s"); params.append(patch)
    where.append("n >= %s"); params.append(min_n)

    sql = f"""
      select {select_cols}
      from {table}
      where {' and '.join(where)}
      {order_clause(sort)}
      limit %s
    """
    params = [prior_wr, alpha, alpha, *params, limit]
    return fetch_rows(sql, tuple(params))


# mount the admin router at the end (order doesn’t really matter)
from . import admin_refresh
app.include_router(admin_refresh.router)
