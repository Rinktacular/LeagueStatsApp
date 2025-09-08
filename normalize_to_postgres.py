import os, json, re
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.exceptions import NotFound

# --- DB import: support psycopg3 or psycopg2 transparently ---
try:
    import psycopg  # psycopg3
    _PSYCOPG3 = True
except ImportError:
    import psycopg2 as psycopg  # psycopg2 fallback
    _PSYCOPG3 = False

load_dotenv()

BUCKET = os.environ["BUCKET_NAME"].replace("gs://", "")
PATCH  = os.environ.get("PATCH_TAG", "25.17")
REGION = os.environ["RIOT_REGION"]
PG_DSN = os.environ["PG_DSN"]

gcs = storage.Client()
bucket = gcs.bucket(BUCKET)

MATCH_KEY_PREFIX = f"raw/{PATCH}/{REGION}/matches/"
ID_RE = re.compile(r'.*/(NA1_\d+)\.json$')  # adjust for other regions later

def list_match_ids_from_gcs(limit: int = 200):
    ids = []
    for blob in gcs.list_blobs(BUCKET, prefix=MATCH_KEY_PREFIX):
        m = ID_RE.match(blob.name)
        if m:
            ids.append(m.group(1))
            if len(ids) >= limit:
                break
    return ids

def load_match_json(match_id: str):
    key = f"{MATCH_KEY_PREFIX}{match_id}.json"
    data = bucket.blob(key).download_as_bytes()
    return json.loads(data)

def get_lane_key(p):
    pos = (p.get("teamPosition") or "").upper()
    return pos if pos else "UNKNOWN"

def map_opponents(rows):
    by_pos_team = {}
    for r in rows:
        by_pos_team.setdefault((r["team_position"], r["team_id"]), []).append(r)
    pairs = []
    for pos in ["TOP","JUNGLE","MIDDLE","BOTTOM","UTILITY","UNKNOWN"]:
        a = by_pos_team.get((pos, 100), [])
        b = by_pos_team.get((pos, 200), [])
        if not a or not b:
            continue
        a_sorted = sorted(a, key=lambda x: x["participant_id"])
        b_sorted = sorted(b, key=lambda x: x["participant_id"])
        for i in range(min(len(a_sorted), len(b_sorted))):
            pairs.append((a_sorted[i]["participant_id"], b_sorted[i]["participant_id"], pos))
    return [{"your_pid": y, "opp_pid": o, "pos": pos} for y, o, pos in pairs]

def upsert_match(con, m):
    info = m["info"]
    meta = m.get("metadata", {})
    match_id = meta["matchId"]
    queue = info.get("queueId")
    duration_s = int(info.get("gameDuration", 0))
    game_start_ms = info.get("gameStartTimestamp") or info.get("gameCreation")
    # Force patch from .env file, ignore gameVersion
    # TODO: later we may want to parse gameVersion for historical data properly and/or map api "patches" to live game patch values.
    patch = PATCH


    with con.cursor() as cur:
        cur.execute("""
          insert into matches(match_id, patch, region, queue, game_duration_s, game_start)
          values (%s,%s,%s,%s,%s, to_timestamp(%s/1000.0))
          on conflict (match_id) do nothing
        """, (match_id, patch, REGION, queue, duration_s, game_start_ms))

        part_rows = []
        for p in info.get("participants", []):
            team_pos = get_lane_key(p)
            part_rows.append((
                match_id,
                p["participantId"],
                p.get("puuid"),
                p.get("summonerId"),
                p.get("teamId"),
                p.get("championId"),
                p.get("championName"),
                team_pos,
                (p.get("lane") or "").upper(),
                (p.get("role") or "").upper(),
                bool(p.get("win")),
                p.get("kills",0), p.get("deaths",0), p.get("assists",0),
                p.get("goldEarned",0),
                p.get("challenges", {}).get("laneMinionsFirst10Minutes", 0),
                p.get("challenges", {}).get("xpPerMinuteDeltas", {}).get("0-10", 0.0),
                p.get("challenges", {}).get("goldPerMinute", 0.0)
            ))

        if part_rows:
            cur.executemany("""
              insert into participants(
                match_id, participant_id, puuid, summoner_id, team_id,
                champ_id, champ_name, team_position, lane, role, win,
                kills, deaths, assists, gold_earned, cs10, xpd10, gd10
              ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              on conflict (match_id, participant_id) do nothing
            """, part_rows)

        cur.execute("""
          select match_id, participant_id, team_id, team_position
          from participants where match_id=%s
        """, (match_id,))
        prs = [{"match_id": r[0], "participant_id": r[1], "team_id": r[2], "team_position": r[3]}
               for r in cur.fetchall()]
        pairs = map_opponents(prs)
        if pairs:
            cur.executemany("""
              insert into opponents(match_id, your_participant_id, opp_participant_id, team_position)
              values (%s,%s,%s,%s)
              on conflict (match_id, your_participant_id) do nothing
            """, [(match_id, p["your_pid"], p["opp_pid"], p["pos"]) for p in pairs])

def main(limit: int = 1000):
    ids = list_match_ids_from_gcs(limit)
    print(f"[normalize] found {len(ids)} match files in GCS under {MATCH_KEY_PREFIX}")
    if not ids:
        print("[normalize] No match files found. Check your bucket path or run the worker.")
        return

    with psycopg.connect(PG_DSN) as con:
        # psycopg3 autocommit defaults to False; psycopg2 as well.
        con.autocommit = True
        for i, mid in enumerate(ids, 1):
            try:
                m = load_match_json(mid)
                upsert_match(con, m)
                if i % 20 == 0:
                    print(f"[normalize] {i}/{len(ids)} inserted")
            except NotFound:
                print(f"[normalize] missing object for {mid} (unexpected)")
            except Exception as ex:
                print(f"[normalize] failed {mid}: {ex}")

if __name__ == "__main__":
    main(100)
