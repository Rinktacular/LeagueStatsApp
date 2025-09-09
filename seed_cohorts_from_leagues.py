# scripts/seed_cohorts_from_leagues.py
import os, time
import psycopg
from dotenv import load_dotenv
from riot.riot_api import RiotClient

load_dotenv()
PG_DSN   = os.environ["PG_DSN"]
API_KEY  = os.environ["RIOT_API_KEY"]

PLATFORMS = ["na1","euw1","kr"]  # add more as you like
QUEUE = "RANKED_SOLO_5x5"

def upsert(puuid, platform, tier, division):
    with psycopg.connect(PG_DSN, autocommit=True) as con, con.cursor() as cur:
        cur.execute("""
          insert into puuid_cohort_current (puuid, platform, tier, division, updated_at)
          values (%s, %s, %s, %s, now())
          on conflict (puuid) do update
            set platform=excluded.platform, tier=excluded.tier, division=excluded.division, updated_at=now()
        """, (puuid, platform, tier, division))

def process_apex(rc: RiotClient, platform: str, tier_name: str):
    # tier_name in {"MASTER","GRANDMASTER","CHALLENGER"}
    if tier_name == "MASTER":
        data = rc._get(f"https://{platform}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/{QUEUE}")
    elif tier_name == "GRANDMASTER":
        data = rc._get(f"https://{platform}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{QUEUE}")
    else:
        data = rc._get(f"https://{platform}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{QUEUE}")

    # Some payloads include puuid per entry; if not, resolve via summonerId -> Summoner-v4
    entries = data.get("entries", [])
    for e in entries:
        puuid = e.get("puuid")
        division = e.get("rank")  # "I" for apex
        if not puuid:
            sid = e.get("summonerId")
            if not sid:
                continue
            summ = rc._get(f"https://{platform}.api.riotgames.com/lol/summoner/v4/summoners/{sid}")
            puuid = summ.get("puuid")
        if puuid:
            upsert(puuid, platform, tier_name, division)
            time.sleep(0.01)  # tiny pacing

def main():
    for platform in PLATFORMS:
        rc = RiotClient(API_KEY, region=platform, platform={"na1":"americas","euw1":"europe","kr":"asia"}[platform])
        for tier_name in ("CHALLENGER","GRANDMASTER","MASTER"):
            print(f"[seed] {platform} {tier_name}")
            process_apex(rc, platform, tier_name)
            time.sleep(0.2)

if __name__ == "__main__":
    main()
