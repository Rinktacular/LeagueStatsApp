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

MATCH_PREFIX = f"raw/{PATCH}/{REGION}/matches/"
TL_PREFIX    = f"raw/{PATCH}/{REGION}/timelines/"
MID_RE = re.compile(r'.*/(NA1_\d+)\.json$')  # adjust if/when you add more regions

def list_ids(prefix, limit=300):
    ids = []
    for blob in gcs.list_blobs(BUCKET, prefix=prefix):
        m = MID_RE.match(blob.name)
        if m:
            ids.append(m.group(1))
            if len(ids) >= limit:
                break
    return ids

def load_json(prefix, mid):
    key = f"{prefix}{mid}.json"
    data = bucket.blob(key).download_as_bytes()
    return json.loads(data)

MYTHICS = {
    # keep this list small to start; expand as needed (Riot item IDs)
    6653,6655,4636,2065,4005,6610,6671,6672,6673,6691,6692,6693,4633,3190,6617,3194,6642,6632
}
BOOTS = {3006,3111,3117,3158,3020,3047}

def upsert_runes_and_items(con, mid, match):
    info = match.get("info", {})
    parts = info.get("participants", [])
    with con.cursor() as cur:
        # items final snapshot
        items_rows = []
        # runes
        runes_rows = []
        for p in parts:
            pid = p["participantId"]

            # final items (0..6)
            slots = [None]*7
            for s in range(7):
                slots[s] = p.get(f"item{s}", None)
            items_rows.append((mid, pid, *slots))

            # ----- FIXED RUNES PARSING -----
            primary_style = primary_keystone = None
            primary_minors = None
            secondary_style = None
            secondary_minors = None
            shards = None

            perks = p.get("perks") or {}
            styles = perks.get("styles") or []

            # styles is a list of dicts; find primary/sub by 'description'
            prim = next((s for s in styles if isinstance(s, dict) and s.get("description") == "primaryStyle"), None)
            sec  = next((s for s in styles if isinstance(s, dict) and s.get("description") == "subStyle"), None)

            if prim:
                primary_style = prim.get("style")
                sels = prim.get("selections") or []
                if sels:
                    # first selection is the keystone
                    ks = sels[0].get("perk") if isinstance(sels[0], dict) else None
                    primary_keystone = ks
                    # next up to 3 minors
                    pm = [x.get("perk") for x in sels[1:4] if isinstance(x, dict) and x.get("perk") is not None]
                    primary_minors = pm if pm else None

            if sec:
                secondary_style = sec.get("style")
                sels = sec.get("selections") or []
                sm = [x.get("perk") for x in sels[:2] if isinstance(x, dict) and x.get("perk") is not None]
                secondary_minors = sm if sm else None

            # statPerks is a dict of plain ints; keep deterministic order: offense, flex, defense
            sp = perks.get("statPerks")
            if isinstance(sp, dict):
                shards = [sp.get("offense"), sp.get("flex"), sp.get("defense")]
                # drop None if any key missing
                shards = [v for v in shards if v is not None] or None
            else:
                shards = None

            runes_rows.append((
                mid, pid, primary_style, primary_keystone,
                primary_minors, secondary_style, secondary_minors, shards
            ))

       
        if items_rows:
            cur.executemany("""
              insert into participant_items_final(match_id, participant_id, slot0,slot1,slot2,slot3,slot4,slot5,slot6)
              values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
              on conflict (match_id, participant_id) do update set
                slot0=excluded.slot0, slot1=excluded.slot1, slot2=excluded.slot2, slot3=excluded.slot3,
                slot4=excluded.slot4, slot5=excluded.slot5, slot6=excluded.slot6
            """, items_rows)

        if runes_rows:
            cur.executemany("""
              insert into participant_runes(match_id, participant_id, primary_style, primary_keystone,
                primary_minors, secondary_style, secondary_minors, shards)
              values (%s,%s,%s,%s,%s,%s,%s,%s)
              on conflict (match_id, participant_id) do update set
                primary_style=excluded.primary_style,
                primary_keystone=excluded.primary_keystone,
                primary_minors=excluded.primary_minors,
                secondary_style=excluded.secondary_style,
                secondary_minors=excluded.secondary_minors,
                shards=excluded.shards
            """, runes_rows)

def upsert_item_events_and_core(con, mid, timeline):
    info = timeline.get("info", {})
    frames = info.get("frames", [])
    if not frames:
        return
    events = []
    # Build per-participant inventory to detect completions
    # (simple heuristic; we mainly want mythic + boots + first legendary)
    inventory = {i: set() for i in range(1,11)}
    core_rows = {}  # pid -> dict

    for fr in frames:
        ts = fr.get("timestamp", 0)
        for ev in fr.get("events", []):
            et = ev.get("type")
            pid = ev.get("participantId") or ev.get("creatorId")
            if pid is None or not (1 <= pid <= 10):  # ignore team/objective events
                continue

            if et in ("ITEM_PURCHASED","ITEM_SOLD","ITEM_UNDO","ITEM_DESTROYED"):
                item_id = ev.get("itemId")
                if not item_id:
                    continue
                events.append((mid, pid, ts, item_id, et, None))
                # very coarse inventory tracking
                if et == "ITEM_PURCHASED":
                    inventory[pid].add(item_id)
                elif et in ("ITEM_SOLD","ITEM_UNDO","ITEM_DESTROYED"):
                    inventory[pid].discard(item_id)

                # detect core pieces roughly
                c = core_rows.setdefault(pid, {"mythic_id": None, "boots_id": None, "first_item_id": None, "first_back_gold": None})
                if c["boots_id"] is None and item_id in BOOTS and et == "ITEM_PURCHASED":
                    c["boots_id"] = item_id
                if c["mythic_id"] is None and item_id in MYTHICS and et == "ITEM_PURCHASED":
                    c["mythic_id"] = item_id
                if c["first_item_id"] is None and et == "ITEM_PURCHASED":
                    # ignore pots/wards; keep first meaningful purchase
                    if item_id not in (2003,2055,3340,3363,3364):
                        c["first_item_id"] = item_id

    with con.cursor() as cur:
        if events:
            cur.executemany("""
              insert into participant_item_events(match_id, participant_id, ts_ms, item_id, event, gold_at_ts)
              values (%s,%s,%s,%s,%s,%s)
              on conflict do nothing
            """, events)

        if core_rows:
            rows = [(mid, pid, v["mythic_id"], v["boots_id"], v["first_item_id"], v["first_back_gold"])
                    for pid, v in core_rows.items()]
            cur.executemany("""
              insert into participant_core_build(match_id, participant_id, mythic_id, boots_id, first_item_id, first_back_gold)
              values (%s,%s,%s,%s,%s,%s)
              on conflict (match_id, participant_id) do update set
                mythic_id=coalesce(excluded.mythic_id, participant_core_build.mythic_id),
                boots_id=coalesce(excluded.boots_id, participant_core_build.boots_id),
                first_item_id=coalesce(excluded.first_item_id, participant_core_build.first_item_id),
                first_back_gold=coalesce(excluded.first_back_gold, participant_core_build.first_back_gold)
            """, rows)

def main(limit=200):
    match_ids = list_ids(MATCH_PREFIX, limit)
    print(f"[builds] processing {len(match_ids)} matches from GCS...")
    with psycopg.connect(PG_DSN) as con:
        if _PG3:
            con.autocommit = True
        for i, mid in enumerate(match_ids, 1):
            try:
                m = load_json(MATCH_PREFIX, mid)
                upsert_runes_and_items(con, mid, m)
                tl = load_json(TL_PREFIX, mid)
                upsert_item_events_and_core(con, mid, tl)
                if i % 20 == 0:
                    print(f"[builds] {i}/{len(match_ids)}")
            except NotFound:
                print(f"[builds] missing objects for {mid}")
            except Exception as ex:
                print(f"[builds] failed {mid}: {ex}")

if __name__ == "__main__":
    main(200)
