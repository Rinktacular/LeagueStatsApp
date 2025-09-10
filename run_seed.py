# run_seed.py
import os, argparse
from typing import Iterable
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone

from riot.client import match_ids_by_puuid, get_match, get_timeline, ddragon_latest_version, ddragon_champions, ddragon_items
from riot.normalize import derive_patch, derive_lane_role

from util.logging import setup_logger
log = setup_logger(__name__)


load_dotenv()

PG_DSN = os.getenv("PG_DSN", "dbname=league user=postgres host=localhost")
DEFAULT_ROUTING = os.getenv("DEFAULT_ROUTING", "AMERICAS")
DEFAULT_QUEUE = int(os.getenv("DEFAULT_QUEUE", "420"))
ITEM_NAME_CACHE: dict[int, str] | None = None

def get_item_name(item_id: int) -> str:
    global ITEM_NAME_CACHE
    if ITEM_NAME_CACHE is None:
        try:
            ver = ddragon_latest_version()
            ITEM_NAME_CACHE = ddragon_items(ver)
        except Exception:
            ITEM_NAME_CACHE = {}
    return ITEM_NAME_CACHE.get(item_id, f"Unknown {item_id}")

def ensure_item_exists(cur, item_id: int):
    cur.execute(
        "INSERT INTO lol.items (item_id, item_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (item_id, get_item_name(item_id)),
    )

def upsert_champions_items(conn: psycopg.Connection):
    ver = ddragon_latest_version()
    champs = ddragon_champions(ver)
    items = ddragon_items(ver)
    with conn.cursor() as cur:
        for cid, name in champs.items():
            cur.execute("INSERT INTO lol.champions (champ_id, champ_name) VALUES (%s,%s) ON CONFLICT (champ_id) DO UPDATE SET champ_name=EXCLUDED.champ_name",
                        (cid, name))
        for iid, name in items.items():
            cur.execute("INSERT INTO lol.items (item_id, item_name) VALUES (%s,%s) ON CONFLICT (item_id) DO UPDATE SET item_name=EXCLUDED.item_name",
                        (iid, name))
            
            
def _ts_ms_to_dt(ts_ms: int):
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

def insert_match(conn: psycopg.Connection, info: dict):
    match_id = info["gameId"]
    region = info.get("platformId","NA1")
    queue_id = info["queueId"]
    patch = derive_patch(info["gameVersion"])
    game_version = info["gameVersion"]
    game_start_ts = psycopg.TimestampFromTicks(info["gameStartTimestamp"]/1000.0)
    duration_s = info["gameDuration"]
    blue_win = any(p["win"] for p in info["participants"] if p["teamId"] == 100)
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO lol.matches (match_id, region, queue_id, patch, game_version, game_start_ts, duration_s, skill_tier, blue_win)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (match_id) DO NOTHING
        """, (match_id, region, queue_id, patch, game_version, game_start_ts, duration_s, None, blue_win))

def insert_match_from_payload(conn: psycopg.Connection, match_payload: dict):
    mid = match_payload["metadata"]["matchId"]       # <-- canonical, e.g. "NA1_5365324203"
    info = match_payload["info"]
    region = info.get("platformId","NA1")
    queue_id = info["queueId"]
    patch = derive_patch(info["gameVersion"])
    game_version = info["gameVersion"]
    game_start_ts = _ts_ms_to_dt(info["gameStartTimestamp"])
    duration_s = info["gameDuration"]
    blue_win = any(p["win"] for p in info["participants"] if p["teamId"] == 100)

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO lol.matches (match_id, region, queue_id, patch, game_version, game_start_ts, duration_s, skill_tier, blue_win)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (match_id) DO NOTHING
        """, (mid, region, queue_id, patch, game_version, game_start_ts, duration_s, None, blue_win))


def insert_participants_from_payload(conn: psycopg.Connection, match_payload: dict):
    mid = match_payload["metadata"]["matchId"]       # <-- canonical
    info = match_payload["info"]

    with conn.cursor() as cur:
        for p in info["participants"]:
            lane_d, role_d = derive_lane_role(p)
            cs = p.get("totalMinionsKilled",0) + p.get("neutralMinionsKilled",0)
            cur.execute("""
            INSERT INTO lol.participants
              (match_id, puuid, team_id, champ_id, lane_raw, role_raw, lane_derived, role_derived,
               win, kills, deaths, assists, cs, gold_earned, damage_dealt,
               item0,item1,item2,item3,item4,item5,item6)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (match_id, puuid) DO NOTHING
            """, (
                mid, p["puuid"], p["teamId"], p["championId"],
                p.get("lane"), p.get("role"), lane_d, role_d,
                p["win"], p["kills"], p["deaths"], p["assists"],
                cs, p.get("goldEarned",0), p.get("totalDamageDealtToChampions"),
                p.get("item0"), p.get("item1"), p.get("item2"),
                p.get("item3"), p.get("item4"), p.get("item5"), p.get("item6")
            ))

def insert_participants(conn: psycopg.Connection, info: dict):
    mid = info["gameId"]
    with conn.cursor() as cur:
        for p in info["participants"]:
            lane_d, role_d = derive_lane_role(p)
            cs = p.get("totalMinionsKilled",0) + p.get("neutralMinionsKilled",0)
            cur.execute("""
            INSERT INTO lol.participants
              (match_id, puuid, team_id, champ_id, lane_raw, role_raw, lane_derived, role_derived,
               win, kills, deaths, assists, cs, gold_earned, damage_dealt,
               item0,item1,item2,item3,item4,item5,item6)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (match_id, puuid) DO NOTHING
            """, (
                mid, p["puuid"], p["teamId"], p["championId"],
                p.get("lane"), p.get("role"), lane_d, role_d,
                p["win"], p["kills"], p["deaths"], p["assists"],
                cs, p.get("goldEarned",0), p.get("totalDamageDealtToChampions"),
                p.get("item0"), p.get("item1"), p.get("item2"),
                p.get("item3"), p.get("item4"), p.get("item5"), p.get("item6")
            ))

"""inserts timeline data, including participant frames and item events"""
def insert_timeline(conn: psycopg.Connection, timeline: dict):
    mid = timeline["metadata"]["matchId"]
    # participantId (1..10) -> puuid
    id_to_puuid = {str(i+1): pu for i, pu in enumerate(timeline["metadata"]["participants"])}

    with conn.cursor() as cur:
        # Per-minute frames
        for minute, fr in enumerate(timeline["info"]["frames"]):
            pf = fr.get("participantFrames", {})
            for pid, snap in pf.items():
                pu = id_to_puuid.get(pid)
                if not pu:
                    continue
                gold = snap.get("totalGold") or snap.get("gold") or 0
                cs = snap.get("minionsKilled", 0) + snap.get("jungleMinionsKilled", 0)
                xp = snap.get("xp", 0)
                cur.execute("""
                    INSERT INTO lol.participant_frames (match_id, puuid, minute, gold, xp, cs)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (match_id, puuid, minute) DO NOTHING
                """, (mid, pu, minute, int(gold), int(xp), int(cs)))

        # Item-oriented events
                # Item-oriented events
        for fr in timeline["info"]["frames"]:
            for ev in fr.get("events", []):
                ev_type = ev.get("type")
                if ev_type not in ("ITEM_PURCHASED", "ITEM_SOLD", "ITEM_UNDO", "ITEM_DESTROYED", "ITEM_PICKUP"):
                    continue

                pid = ev.get("participantId")
                pu = id_to_puuid.get(str(pid)) if pid is not None else None
                if not pu:
                    continue  # non-participant event

                ts_ms = int(ev.get("timestamp", 0))
                rows = []

                if ev_type == "ITEM_PURCHASED":
                    item_id = ev.get("itemId")
                    if item_id:
                        rows.append(("PURCHASE", int(item_id)))

                elif ev_type == "ITEM_SOLD":
                    item_id = ev.get("itemId")
                    if item_id:
                        rows.append(("SELL", int(item_id)))

                elif ev_type == "ITEM_DESTROYED":
                    item_id = ev.get("itemId")
                    if item_id:
                        rows.append(("DESTROY", int(item_id)))

                elif ev_type == "ITEM_PICKUP":
                    item_id = ev.get("itemId")
                    if item_id:
                        rows.append(("PICKUP", int(item_id)))

                elif ev_type == "ITEM_UNDO":
                    b = ev.get("beforeId")  # may be 0/None
                    a = ev.get("afterId")
                    if b and b != 0:
                        rows.append(("UNDO_BEFORE", int(b)))
                    if a and a != 0 and a != b:
                        rows.append(("UNDO_AFTER", int(a)))

                for r_type, r_item in rows:
                    try:
                        ensure_item_exists(cur, r_item)
                        cur.execute("""
                            INSERT INTO lol.item_events (match_id, puuid, ts_ms, event_type, item_id)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                        """, (mid, pu, ts_ms, r_type, r_item))
                    except Exception as ex:
                        log.warning(
                            f"Skipping item event insert mid={mid} pu={pu} ts={ts_ms} type={r_type} item={r_item}: {ex}"
                        )

def seed_for_puuid(conn: psycopg.Connection, routing: str, puuid: str, queue: int, start: int, count: int):
    mids = match_ids_by_puuid(routing, puuid, start=start, count=count, queue=queue)
    for mid in mids:
        m = get_match(routing, mid)
        info = m["info"]
        insert_match(conn, info)
        insert_participants(conn, info)
        tl = get_timeline(routing, mid)
        insert_timeline(conn, tl)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puuid", action="append", help="One or more PUUIDs", required=True)
    ap.add_argument("--routing", default=DEFAULT_ROUTING, help="AMERICAS|EUROPE|ASIA")
    ap.add_argument("--queue", type=int, default=DEFAULT_QUEUE, help="Queue id (e.g., 420=Ranked Solo)")
    ap.add_argument("--batch", type=int, default=100, help="How many match ids per PUUID fetch")
    args = ap.parse_args()

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        upsert_champions_items(conn)
        for pu in args.puuid:
            seed_for_puuid(conn, args.routing, pu, args.queue, start=0, count=args.batch)

if __name__ == "__main__":
    main()
