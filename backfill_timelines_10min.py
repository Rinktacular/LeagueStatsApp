import os, json, re
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.exceptions import NotFound

# psycopg2/3 compatibility
try:
    import psycopg
    _PG3 = True
except ImportError:
    import psycopg2 as psycopg
    _PG3 = False

load_dotenv()

BUCKET = os.environ["BUCKET_NAME"].replace("gs://", "")
PATCH  = os.environ.get("PATCH_TAG", "25.17")
REGION = os.environ["RIOT_REGION"]
PG_DSN = os.environ["PG_DSN"]

gcs = storage.Client()
bucket = gcs.bucket(BUCKET)

TL_PREFIX = f"raw/{PATCH}/{REGION}/timelines/"
MID_RE = re.compile(r'.*/(NA1_\d+)\.json$')  # adjust region tag as needed

def list_timeline_ids(limit=300):
    ids = []
    for blob in gcs.list_blobs(BUCKET, prefix=TL_PREFIX):
        m = MID_RE.match(blob.name)
        if m:
            ids.append(m.group(1))
            if len(ids) >= limit:
                break
    return ids

def load_timeline(mid):
    key = f"{TL_PREFIX}{mid}.json"
    data = bucket.blob(key).download_as_bytes()
    return json.loads(data)

def pick_frame_index(frames, target_ms=600000):
    # choose first frame with timestamp >= target_ms; else last frame
    idx = None
    for i, fr in enumerate(frames):
        if fr.get("timestamp", 0) >= target_ms:
            idx = i
            break
    if idx is None:
        idx = len(frames) - 1
    return idx

def backfill_for_match(con, mid, tl):
    info = tl.get("info", {})
    frames = info.get("frames", [])
    if not frames:
        return

    idx = pick_frame_index(frames, 600000)
    pf = frames[idx].get("participantFrames", {})
    # Map pid -> (gold, xp, cs)
    stats = {}
    for k, v in pf.items():
        # keys can be '1'..'10' or 'participantId_1' style; handle both
        pid = int(k.split("_")[-1]) if "_" in k else int(k)
        total_gold = int(v.get("totalGold", 0))
        xp = int(v.get("xp", 0))
        cs = int(v.get("minionsKilled", 0)) + int(v.get("jungleMinionsKilled", 0))
        stats[pid] = (total_gold, xp, cs)

    with con.cursor() as cur:
        # Fetch opponent pairs for this match
        cur.execute("""
          select your_participant_id, opp_participant_id
          from opponents where match_id=%s
        """, (mid,))
        pairs = cur.fetchall()
        if not pairs:
            return

        # Prepare updates: set cs10/xpd10/gd10 for "your" participants
        updates = []
        for yp, op in pairs:
            y = stats.get(yp)
            o = stats.get(op)
            if not y:
                continue
            y_gold, y_xp, y_cs = y
            # opponent may be missing if remake; handle gracefully
            gd10 = (y_gold - o[0]) if o else None
            xpd10 = y_xp
            cs10 = y_cs
            updates.append((cs10, xpd10, gd10, mid, yp))

        if updates:
            cur.executemany("""
              update participants
                 set cs10 = %s,
                     xpd10 = %s,
                     gd10  = %s
               where match_id = %s
                 and participant_id = %s
            """, updates)

def main(limit=200):
    ids = list_timeline_ids(limit)
    print(f"[timeline] found {len(ids)} timelines under {TL_PREFIX}")

    with psycopg.connect(PG_DSN) as con:
        if _PG3:
            con.autocommit = True
        for i, mid in enumerate(ids, 1):
            try:
                tl = load_timeline(mid)
                backfill_for_match(con, mid, tl)
                if i % 20 == 0:
                    print(f"[timeline] updated {i}/{len(ids)}")
            except NotFound:
                print(f"[timeline] missing timeline {mid} (skipping)")
            except Exception as ex:
                print(f"[timeline] failed {mid}: {ex}")

if __name__ == "__main__":
    main(200)
