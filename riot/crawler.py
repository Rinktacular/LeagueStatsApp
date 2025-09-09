import os, time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from .riot_api import RiotClient
from .storage import Storage
from .ledger import Ledger

def unix_seconds(dt) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

# platform host -> routing region
PLATFORM_TO_ROUTING = {
    # AMERICAS
    "na1": "americas", "br1": "americas", "la1": "americas", "la2": "americas",
    # EUROPE
    "euw1": "europe", "eun1": "europe", "tr1": "europe", "ru": "europe",
    # ASIA
    "kr": "asia", "jp1": "asia",
    # SEA
    "oc1": "sea", "ph2": "sea", "sg2": "sea", "th2": "sea", "tw2": "sea", "vn2": "sea",
}
ROUTING_VALUES = {"americas", "europe", "asia", "sea"}

def _resolve_routing(value: str) -> str:
    v = (value or "").lower()
    if v in ROUTING_VALUES:
        return v
    return PLATFORM_TO_ROUTING.get(v, "americas")

class Crawler:
    def __init__(self):
        # NOTE: env naming: RIOT_REGION should be the platform host (na1/euw1/kr)
        #       RIOT_PLATFORM should be the routing region (americas/europe/asia/sea)
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
            self.storage = Storage(
                "s3",
                bucket,
                endpoint_url=os.getenv("S3_ENDPOINT_URL"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("S3_REGION"),
            )
        self.ledger = Ledger(os.environ["PG_DSN"])

    def seed_from_challenger(self, limit: int = 50) -> int:
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
                    puuid, queue=self.queue, start=0, count=50, type_="ranked"
                )
            except Exception as ex:
                print(f"[seed] match ids failed: {ex}")
                continue

            if not match_ids:
                players_no_ids += 1
                continue

            # enqueue with routing region (self.platform) so the worker can fetch properly
            added = self.ledger.enqueue_matches(self.platform, match_ids)
            total_enqueued += added

            if idx % 10 == 0:
                print(f"[seed] processed {idx}/{len(entries)}, enqueued so far: {total_enqueued}")

            time.sleep(0.1)  # polite pacing

        print(f"[seed] Done. Enqueued={total_enqueued}, players_with_no_ids={players_no_ids}")
        return total_enqueued
    
   # ---------- NEW: Multi-platform & multi-tier seeding ----------
    def seed_from_leagues(
        self,
        platforms: Iterable[str],
        tiers: Iterable[str] = ("CHALLENGER", "GRANDMASTER", "MASTER"),
        divisions: Iterable[str] = ("I",),     # used for sub-Master only (e.g., DIAMOND)
        queue: Optional[int] = None,
        since: Optional[int] = None,           # unix seconds
        until: Optional[int] = None,           # unix seconds
        per_puuid: int = 100,                  # 0..100 per request
        limit_puuids: Optional[int] = None,    # cap #summoners per league/division
    ) -> int:
        api_key = os.environ["RIOT_API_KEY"]
        total_enqueued = 0

        for platform_host in platforms:
            p = platform_host.lower()
            routing = PLATFORM_TO_ROUTING.get(p, "americas")
            temp_api = RiotClient(api_key, region=platform_host, platform=routing)

            for tier in (t.upper() for t in tiers):
                print(f"[seed] {platform_host} {tier}")
                entries: list[dict] = []

                # 1) pull league entries
                try:
                    if tier == "CHALLENGER":
                        entries = temp_api.get_challenger_entries() or []
                    elif tier == "GRANDMASTER":
                        entries = temp_api.get_grandmaster_entries() or []
                    elif tier == "MASTER":
                        entries = temp_api.get_master_entries() or []
                    else:
                        # e.g., DIAMOND paging
                        for div in divisions:
                            page = 1
                            while True:
                                batch = temp_api.get_entries_paginated(
                                    queue="RANKED_SOLO_5x5", tier=tier, division=div, page=page
                                ) or []
                                if not batch:
                                    break
                                entries.extend(batch)
                                page += 1
                                if limit_puuids and len(entries) >= limit_puuids:
                                    break
                            if limit_puuids and len(entries) >= limit_puuids:
                                break
                except Exception as ex:
                    print(f"[seed] error fetching league list: {platform_host} {tier} -> {ex}")
                    continue

                if limit_puuids:
                    entries = entries[:limit_puuids]
                if not entries:
                    print(f"[seed] no entries for {platform_host} {tier}")
                    continue

                # 2) resolve summoner -> puuid
                puuids: list[str] = []
                for e in entries:
                    puuid = e.get("puuid")
                    if not puuid:
                        sid = e.get("summonerId")
                        sname = e.get("summonerName")
                        try:
                            if sid:
                                summ = temp_api.get_summoner_by_id(sid)
                            elif sname:
                                summ = temp_api.get_summoner_by_name(sname)
                            else:
                                continue
                            puuid = (summ or {}).get("puuid")
                        except Exception as ex:
                            print(f"[seed] summoner lookup failed: {ex}")
                            continue
                    if puuid:
                        puuids.append(puuid)
                    if limit_puuids and len(puuids) >= limit_puuids:
                        break

                # 3) puuid -> match IDs (optional time window)
                for puuid in puuids:
                    params = dict(
                        queue=(queue or self.queue),
                        start=0,
                        count=min(max(per_puuid, 0), 100),
                        type_="ranked",
                    )
                    if since is not None:
                        params["start_time"] = since
                    if until is not None:
                        params["end_time"] = until

                    try:
                        match_ids = temp_api.get_match_ids_by_puuid(puuid, **params)
                    except Exception as ex:
                        print(f"[seed] match ids failed puuid={puuid[:8]}…: {ex}")
                        continue

                    if not match_ids:
                        continue

                    # IMPORTANT: enqueue with routing region so worker knows where to fetch
                    added = self.ledger.enqueue_matches(routing, match_ids)
                    total_enqueued += added
                    time.sleep(0.05)  # be polite

                print(f"[seed] {platform_host} {tier}: enqueued so far {total_enqueued}")

        print(f"[seed] Done. Total enqueued={total_enqueued}")
        return total_enqueued
    
   # ---------- Worker ----------
    def process_one(self) -> bool:
        item = self.ledger.pop_next_match()
        if not item:
            print("[worker] queue empty")
            return False

        match_id, queued_region, qid = item  # 'queued_region' may be routing or platform host (legacy)
        routing = _resolve_routing(queued_region)

        # Build a client that points match-v5 to the proper routing
        api_key = os.environ["RIOT_API_KEY"]
        api = RiotClient(api_key, region=self.region, platform=routing)

        if self.ledger.seen(match_id):
            self.ledger.mark_done(qid)
            print(f"[worker] already seen {match_id}, marked done")
            return True

        try:
            match = api.get_match(match_id)
            self.storage.write_json(self.patch, routing, match_id, "match", match)

            timeline = api.get_timeline(match_id)
            self.storage.write_json(self.patch, routing, match_id, "timeline", timeline)

            self.ledger.mark_seen(match_id, routing)
            self.ledger.mark_done(qid)
            print(f"[worker] saved {match_id}")
            time.sleep(0.15)  # polite pacing
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
