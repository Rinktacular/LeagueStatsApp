import os, time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
import psycopg

from .riot_api import RiotClient
from .rate_limit import MultiLimiter
from .storage import Storage
from .ledger import Ledger
from .metrics import Metrics

def unix_seconds(dt) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())



def upsert_rank(conn, puuid: str, platform: str, tier: str | None, division: str | None):
    if not puuid:
        return
    tier = (tier or "UNRANKED").upper()
    division = division or None
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO puuid_cohort_current (puuid, platform, tier, division, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (puuid) DO UPDATE
              SET platform = EXCLUDED.platform,
                  tier     = EXCLUDED.tier,
                  division = EXCLUDED.division,
                  updated_at = now();
        """, (puuid, platform, tier, division))

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
        # Rate limiter for Riot API since we are limited until we obtain a production-grade API key (shared by all clients)
        self.limiter = MultiLimiter(per_sec=20, per_2min=100)
        self.metrics = Metrics()
        self.metrics.start_reporter(interval=10.0)

    def seed_from_challenger(self, limit: int = 50) -> int:
        self.limiter.acquire(MultiLimiter.key_for_platform(self.platform))
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
                        self.limiter.acquire(MultiLimiter.key_for_platform(self.platform))
                        summ = self.api.get_summoner_by_id(sid)
                    elif sname:
                        self.limiter.acquire(MultiLimiter.key_for_platform(self.platform))
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
                self.limiter.acquire(MultiLimiter.key_for_platform(self.platform))
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
        divisions: Iterable[str] = ("I",),
        queue: Optional[int] = None,
        since: Optional[int] = None,
        until: Optional[int] = None,
        per_puuid: int = 100,
        limit_puuids: Optional[int] = None,
    ) -> int:
        """
        Seed match IDs across platforms/tiers/divisions and upsert rank snapshots
        into puuid_cohort_current on the fly. Rate limits respected per platform (league/summoner)
        and per routing (match-v5).
        """
        import psycopg

        api_key = os.environ["RIOT_API_KEY"]
        pg_dsn  = os.environ["PG_DSN"]
        total_enqueued = 0

        def _upsert_rank(conn, puuid: str, platform_host: str, tier_val: Optional[str], div_val: Optional[str]) -> None:
            if not puuid:
                return
            tier_val = (tier_val or "UNRANKED").upper()
            div_val  = (div_val or None)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO puuid_cohort_current (puuid, platform, tier, division, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (puuid) DO UPDATE
                    SET platform = EXCLUDED.platform,
                        tier     = EXCLUDED.tier,
                        division = EXCLUDED.division,
                        updated_at = now();
                    """,
                    (puuid, platform_host.lower(), tier_val, div_val),
                )

        with psycopg.connect(pg_dsn) as conn:
            conn.autocommit = True

            for platform_host in platforms:
                p = platform_host.lower()
                routing = PLATFORM_TO_ROUTING.get(p, "americas")
                temp_api = RiotClient(api_key, region=platform_host, platform=routing)

                for tier in (t.upper() for t in tiers):
                    print(f"[seed] {platform_host} {tier}")
                    entries: list[dict] = []

                    # 1) League entries (platform-scoped rate limit)
                    try:
                        if tier == "CHALLENGER":
                            self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
                            entries = temp_api.get_challenger_entries() or []
                            self.metrics.record_request("platform", platform_host, "league")
                        elif tier == "GRANDMASTER":
                            self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
                            entries = temp_api.get_grandmaster_entries() or []
                            self.metrics.record_request("platform", platform_host, "league")
                        elif tier == "MASTER":
                            self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
                            entries = temp_api.get_master_entries() or []
                            self.metrics.record_request("platform", platform_host, "league")
                        else:
                            for div in divisions:
                                page = 1
                                while True:
                                    self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
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

                    # 2) Summoner -> PUUID (platform-scoped rate limit) + upsert rank
                    puuids: list[str] = []
                    seen_puuids: set[str] = set()

                    for e in entries:
                        entry_tier = (e.get("tier") or tier or "UNRANKED")
                        entry_div  = e.get("rank")  # "I".."IV" or None

                        puuid = e.get("puuid")
                        if not puuid:
                            sid = e.get("summonerId")
                            sname = e.get("summonerName")
                            try:
                                if sid:
                                    self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
                                    summ = temp_api.get_summoner_by_id(sid)
                                    self.metrics.record_request("platform", platform_host, "league")
                                elif sname:
                                    self.limiter.acquire(MultiLimiter.key_for_platform(platform_host))
                                    summ = temp_api.get_summoner_by_name(sname)
                                    self.metrics.record_request("platform", platform_host, "league")
                                else:
                                    continue
                                puuid = (summ or {}).get("puuid")
                            except Exception as ex:
                                print(f"[seed] summoner lookup failed: {ex}")
                                continue

                        if not puuid or puuid in seen_puuids:
                            continue

                        try:
                            _upsert_rank(conn, puuid, platform_host, entry_tier, entry_div)
                        except Exception as ex:
                            print(f"[seed] rank upsert failed puuid={puuid[:8]}…: {ex}")

                        puuids.append(puuid)
                        seen_puuids.add(puuid)
                        if limit_puuids and len(puuids) >= limit_puuids:
                            break

                    # 3) PUUID -> match IDs (routing-scoped rate limit)
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
                            self.limiter.acquire(MultiLimiter.key_for_routing(routing))
                            match_ids = temp_api.get_match_ids_by_puuid(puuid, **params)
                            self.metrics.record_request("platform", platform_host, "league")
                        except Exception as ex:
                            msg = str(ex)
                            if "429" in msg:
                                print(f"[seed] rate limited on matchlist ({routing}); sleeping 1.2s")
                                self.metrics.record_429("routing", routing, "matchlist")
                                time.sleep(1.2)
                            else:
                                print(f"[seed] match ids failed puuid={puuid[:8]}…: {ex}")
                                self.metrics.record_error("platform", platform_host, "summoner")
                            continue

                        if not match_ids:
                            continue

                        try:
                            added = self.ledger.enqueue_matches(routing, match_ids)
                            total_enqueued += added
                        except Exception as ex:
                            print(f"[seed] enqueue failed ({platform_host} {tier}) puuid={puuid[:8]}…: {ex}")

                        # optional tiny jitter; limiter is primary throttle
                        # time.sleep(0.02)

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
            self.limiter.acquire(MultiLimiter.key_for_routing(routing))
            match = api.get_match(match_id)
            self.metrics.record_request("routing", routing, "match")
            self.storage.write_json(self.patch, routing, match_id, "match", match)

            self.limiter.acquire(MultiLimiter.key_for_routing(routing))
            timeline = api.get_timeline(match_id)
            self.metrics.record_request("routing", routing, "timeline")
            self.storage.write_json(self.patch, routing, match_id, "timeline", timeline)

            self.ledger.mark_seen(match_id, routing)
            self.ledger.mark_done(qid)
            print(f"[worker] saved {match_id}")
            self.metrics.record_processed(routing, 1)
            time.sleep(0.05)  # polite pacing between calls
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
