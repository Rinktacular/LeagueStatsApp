from dotenv import load_dotenv
import argparse
import datetime as dt
from riot.crawler import Crawler

def to_unix(d: str | None):
    if not d:
        return None
    return int(dt.datetime.fromisoformat(d).replace(tzinfo=dt.timezone.utc).timestamp())

if __name__ == "__main__":
    load_dotenv()
    c = Crawler()

    ap = argparse.ArgumentParser(description="Seed ranked match IDs from Riot leagues")
    ap.add_argument("--platforms", default="na1", help="Comma list of platform hosts, e.g. na1,euw1,kr")
    ap.add_argument("--tiers", default="CHALLENGER,GRANDMASTER,MASTER",
                    help="Comma list, e.g. CHALLENGER,GRANDMASTER,MASTER,DIAMOND")
    ap.add_argument("--divisions", default="I", help="For sub-Master tiers (e.g., DIAMOND): I,II,III,IV")
    ap.add_argument("--queue", type=int, default=420, help="420=Solo, 440=Flex, 430=Normals, 450=ARAM")
    ap.add_argument("--since", default=None, help="ISO date UTC, e.g. 2025-08-10")
    ap.add_argument("--until", default=None, help="ISO date UTC, e.g. 2025-09-09")
    ap.add_argument("--per-puuid", type=int, default=100, help="Max matches per PUUID (0..100)")
    ap.add_argument("--limit-puuids", type=int, default=0, help="Cap PUUIDs per league/division (0=no cap)")
    ap.add_argument("--challenger-only", action="store_true", help="Use legacy Challenger-only seeding")
    args = ap.parse_args()

    if args.challenger_only:
        print("[seed] Challenger-only mode")
        enq = c.seed_from_challenger(limit=args.limit_puuids or 50)
        print(f"[seed] Enqueued {enq} matches")
    else:
        platforms = [s.strip() for s in args.platforms.split(",") if s.strip()]
        tiers = [s.strip() for s in args.tiers.split(",") if s.strip()]
        divisions = [s.strip() for s in args.divisions.split(",") if s.strip()]
        since = to_unix(args.since)
        until = to_unix(args.until)

        total = c.seed_from_leagues(
            platforms=platforms,
            tiers=tiers,
            divisions=divisions,
            queue=args.queue,
            since=since,
            until=until,
            per_puuid=args.per-puuid if hasattr(args, "per-puuid") else args.per_puuid,  # handle shells that mangle hyphen
            limit_puuids=(args.limit_puuids or None),
        )
        print(f"[seed] Enqueued {total} matches")
