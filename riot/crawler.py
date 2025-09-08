import os, time
from datetime import datetime, timedelta, timezone
from .riot_api import RiotClient
from .storage import Storage
from .ledger import Ledger

def unix_seconds(dt) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

class Crawler:
    def __init__(self):
        self.region = os.environ["RIOT_REGION"]
        self.platform = os.environ["RIOT_PLATFORM"]
        self.api = RiotClient(os.environ["RIOT_API_KEY"], self.region, self.platform)
        self.patch = os.environ.get("PATCH_TAG", "dev")
        self.queue = int(os.environ.get("QUEUE", "420"))

        backend = os.environ.get("OBJECT_BACKEND", "gcs")
        bucket = os.environ["BUCKET_NAME"]
        if backend == "gcs":
            self.storage = Storage("gcs", bucket)
        else:
            self.storage = Storage("s3", bucket,
                                   endpoint_url=os.getenv("S3_ENDPOINT_URL"),
                                   aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                   aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                   region_name=os.getenv("S3_REGION"))
        self.ledger = Ledger(os.environ["PG_DSN"])

    def seed_from_challenger(self, limit=50) -> int:
        entries = self.api.get_challenger_entries() or []
        print(f"[seed] challenger entries: {len(entries)}")
        if not entries:
            return 0

        entries = entries[:limit]
        total_enqueued = 0
        players_no_ids = 0

        for idx, e in enumerate(entries, 1):
            # Prefer PUUID directly if present
            puuid = e.get("puuid")
            if not puuid:
                # Fallbacks for older payloads
                sid = e.get("summonerId")
                sname = e.get("summonerName")
                try:
                    if sid:
                        summ = self.api.get_summoner_by_id(sid)
                    elif sname:
                        summ = self.api.get_summoner_by_name(sname)
                    else:
                        print(f"[seed] entry missing puuid/summonerId/summonerName, skipping")
                        continue
                    puuid = summ.get("puuid")
                except Exception as ex:
                    print(f"[seed] summoner lookup failed: {ex}")
                    continue

            if not puuid:
                print(f"[seed] no puuid resolved, skipping")
                continue

            try:
                match_ids = self.api.get_match_ids_by_puuid(
                    puuid, queue=self.queue, start=0, count=50
                )
            except Exception as ex:
                print(f"[seed] match ids failed: {ex}")
                continue

            if not match_ids:
                players_no_ids += 1
                # Not fatal—some players might not have recent solo queue games
                continue

            added = self.ledger.enqueue_matches(self.region, match_ids)
            total_enqueued += added

            if idx % 10 == 0:
                print(f"[seed] processed {idx}/{len(entries)}, enqueued so far: {total_enqueued}")

            time.sleep(0.1)  # polite pacing

        print(f"[seed] Done. Enqueued={total_enqueued}, players_with_no_ids={players_no_ids}")
        return total_enqueued


    def process_one(self) -> bool:
        item = self.ledger.pop_next_match()
        if not item:
            print("[worker] queue empty")
            return False

        match_id, region, qid = item
        if self.ledger.seen(match_id):
            self.ledger.mark_done(qid)
            print(f"[worker] already seen {match_id}, marked done")
            return True

        try:
            match = self.api.get_match(match_id)
            self.storage.write_json(self.patch, region, match_id, "match", match)
            timeline = self.api.get_timeline(match_id)
            self.storage.write_json(self.patch, region, match_id, "timeline", timeline)

            self.ledger.mark_seen(match_id, region)
            self.ledger.mark_done(qid)
            print(f"[worker] saved {match_id}")
            # be extra polite with a personal key
            time.sleep(0.15)
            return True
        except Exception as e:
            print(f"[worker] error on {match_id}: {e!r}")
            time.sleep(1.0)
            return True


    def drain(self, max_items=200):
        processed = 0
        while processed < max_items:
            had = self.process_one()
            if not had:
                break
            processed += 1
        return processed
