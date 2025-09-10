# run_worker.py
import os
import time
import traceback
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from util.logging import setup_logger

from riot.client import match_ids_by_puuid, get_match, get_timeline, ddragon_latest_version, ddragon_champions, ddragon_items
from riot.normalize import derive_patch, derive_lane_role
from run_seed import upsert_champions_items, insert_match_from_payload, insert_participants_from_payload, insert_timeline  # re-use existing inserts

load_dotenv()
log = setup_logger("worker")

PG_DSN = os.getenv("PG_DSN", "dbname=league user=postgres host=localhost")
DEFAULT_QUEUE = int(os.getenv("DEFAULT_QUEUE", "420"))
POLL_S = int(os.getenv("WORKER_POLL_SECONDS", "5"))

def claim_next(conn: psycopg.Connection):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
        WITH nextjob AS (
          SELECT puuid FROM lol.seed_queue
          WHERE status='PENDING'
          ORDER BY updated_at
          LIMIT 1
        )
        UPDATE lol.seed_queue sq
        SET status='RUNNING', updated_at=now()
        FROM nextjob
        WHERE sq.puuid = nextjob.puuid
        RETURNING sq.puuid, sq.region_routing
        """)
        return cur.fetchone()

def complete(conn: psycopg.Connection, puuid: str):
    with conn.cursor() as cur:
        cur.execute("UPDATE lol.seed_queue SET status='DONE', updated_at=now(), last_error=NULL WHERE puuid=%s", (puuid,))

def fail(conn: psycopg.Connection, puuid: str, err: str):
    with conn.cursor() as cur:
        cur.execute("UPDATE lol.seed_queue SET status='ERROR', updated_at=now(), last_error=%s WHERE puuid=%s", (err, puuid))

def enqueue_new_puuids(conn: psycopg.Connection, regional: str, puuids: list[str]):
    if not puuids:
        return
    with conn.cursor() as cur:
        for pu in puuids:
            cur.execute("""
              INSERT INTO lol.seed_queue(puuid, region_routing, status)
              VALUES (%s,%s,'PENDING')
              ON CONFLICT (puuid) DO NOTHING
            """, (pu, regional))

def work_one(conn: psycopg.Connection, puuid: str, routing: str):
    log.info(f"Working puuid={puuid} routing={routing}")
    upsert_champions_items(conn)  # idempotent helper
    start = 0
    total_matches = 0
    while True:
        mids = match_ids_by_puuid(routing, puuid, start=start, count=100, queue=DEFAULT_QUEUE)
        if not mids:
            log.info(f"No more matches at start={start} for {puuid}")
            break
        log.info(f"Fetched {len(mids)} match ids (start={start}) for {puuid}")
        for mid in mids:
            try:
                log.info(f"Processing match {mid}")
                m = get_match(routing, mid)   # full payload with metadata+info
                # Snowball: enqueue every participant’s puuid
                metadata = m.get("metadata", {})
                others = metadata.get("participants", [])
                enqueue_new_puuids(conn, routing, others)

                insert_match_from_payload(conn, m)          # <-- uses metadata.matchId
                insert_participants_from_payload(conn, m)   # <-- uses metadata.matchId

                tl = get_timeline(routing, mid)
                insert_timeline(conn, tl)                   # already uses metadata.matchId

                total_matches += 1
            except Exception as e:
                log.error(f"Failed match {mid} for {puuid}: {e}", exc_info=True)
        start += 100
    log.info(f"Finished {puuid}; total_matches_ingested={total_matches}")

def main():
    log.info("Worker starting...")
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        while True:
            try:
                job = claim_next(conn)
                if not job:
                    time.sleep(POLL_S)
                    continue
                puuid = job["puuid"]
                routing = job["region_routing"]
                log.info(f"Claimed job puuid={puuid} routing={routing}")
                try:
                    work_one(conn, puuid, routing)
                    complete(conn, puuid)
                    log.info(f"Completed job puuid={puuid}")
                except Exception as e:
                    msg = traceback.format_exc()
                    fail(conn, puuid, msg)
                    log.error(f"Job failed puuid={puuid}: {e}", exc_info=True)
            except KeyboardInterrupt:
                log.info("Worker interrupted, exiting.")
                break
            except Exception as loop_ex:
                log.error(f"Worker loop error: {loop_ex}", exc_info=True)
                time.sleep(2)

if __name__ == "__main__":
    main()
