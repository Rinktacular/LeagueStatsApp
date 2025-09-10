# bootstrap_players.py
import os
import time
import argparse
import requests
import psycopg
from dotenv import load_dotenv
from util.logging import setup_logger

load_dotenv()
log = setup_logger("bootstrap")

RIOT_API_KEY = os.environ["RIOT_API_KEY"]
PG_DSN = os.getenv("PG_DSN", "dbname=league user=postgres host=localhost")

PLATFORM_TO_REGIONAL = {
    "na1":"AMERICAS","br1":"AMERICAS","la1":"AMERICAS","la2":"AMERICAS","oc1":"AMERICAS",
    "euw1":"EUROPE","eun1":"EUROPE","tr1":"EUROPE","ru":"EUROPE",
    "kr":"ASIA","jp1":"ASIA",
    "ph2":"SEA","sg2":"SEA","th2":"SEA","tw2":"SEA","vn2":"SEA",
}

MASTER_PLUS = {"MASTER","GRANDMASTER","CHALLENGER"}

def platform_base(platform: str) -> str:
    return f"https://{platform}.api.riotgames.com"

def _get(url: str) -> requests.Response:
    while True:
        r = requests.get(url, headers={"X-Riot-Token": RIOT_API_KEY}, timeout=30)
        # Retry on 429/503
        if r.status_code in (429, 503):
            retry = int(r.headers.get("Retry-After", "2"))
            log.warning(f"Rate-limited {r.status_code}. Sleeping {retry}s: {url}")
            time.sleep(retry)
            continue
        return r

def _json_or_none(r: requests.Response):
    try:
        return r.json()
    except Exception:
        return None

def league_entries_paged(platform: str, queue: str, tier: str, division: str, page: int) -> list[dict]:
    """IRON..DIAMOND tiers use paged entries endpoint (list)."""
    url = f"{platform_base(platform)}/lol/league/v4/entries/{queue}/{tier}/{division}?page={page}"
    r = _get(url)
    if not r.ok:
        log.error(f"/entries error {r.status_code}: {r.text[:200]}")
        r.raise_for_status()
    data = _json_or_none(r)
    if not isinstance(data, list):
        log.error(f"/entries unexpected payload type={type(data).__name__}: {str(data)[:200]}")
        return []
    return data

def league_entries_master_plus(platform: str, queue: str, tier: str) -> list[dict]:
    """MASTER/GM/CHAL use different endpoints; response is object with 'entries' array."""
    endpoint = {
        "MASTER": "masterleagues",
        "GRANDMASTER": "grandmasterleagues",
        "CHALLENGER": "challengerleagues",
    }[tier]
    url = f"{platform_base(platform)}/lol/league/v4/{endpoint}/by-queue/{queue}"
    r = _get(url)
    if not r.ok:
        log.error(f"/{endpoint} error {r.status_code}: {r.text[:200]}")
        r.raise_for_status()
    data = _json_or_none(r)
    if not isinstance(data, dict) or "entries" not in data or not isinstance(data["entries"], list):
        log.error(f"/{endpoint} unexpected payload: {str(data)[:200]}")
        return []
    return data["entries"]

def summoner_by_id(platform: str, encrypted_summoner_id: str) -> dict | None:
    url = f"{platform_base(platform)}/lol/summoner/v4/summoners/{encrypted_summoner_id}"
    r = _get(url)
    if not r.ok:
        log.error(f"/summoners error {r.status_code}: {r.text[:200]}")
        return None
    data = _json_or_none(r)
    if not isinstance(data, dict) or "puuid" not in data:
        log.error(f"/summoners unexpected payload: {str(data)[:200]}")
        return None
    return data

def enqueue_puuid(conn: psycopg.Connection, puuid: str, platform: str):
    regional = PLATFORM_TO_REGIONAL.get(platform.lower())
    if not regional:
        raise ValueError(f"No regional mapping for platform {platform}")
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO lol.seed_queue(puuid, region_routing, status)
          VALUES (%s,%s,'PENDING')
          ON CONFLICT (puuid) DO UPDATE
            SET region_routing = EXCLUDED.region_routing,
                status = CASE WHEN lol.seed_queue.status='ERROR' THEN 'PENDING' ELSE lol.seed_queue.status END,
                updated_at = now()
        """, (puuid, regional))

def main():
    ap = argparse.ArgumentParser(description="Bootstrap seed_queue from ladder")
    ap.add_argument("--platform", required=True, help="na1|euw1|kr|...")
    ap.add_argument("--tier", required=True, help="IRON|BRONZE|SILVER|GOLD|PLATINUM|EMERALD|DIAMOND|MASTER|GRANDMASTER|CHALLENGER")
    ap.add_argument("--division", default="I", help="I|II|III|IV (ignored for MASTER+)")
    ap.add_argument("--queue", default="RANKED_SOLO_5x5")
    ap.add_argument("--pages", type=int, default=2, help="ladder pages to sample for non-MASTER+ tiers")
    args = ap.parse_args()

    tier = args.tier.upper()
    log.info(f"Bootstrapping: platform={args.platform} tier={tier} div={args.division} queue={args.queue} pages={args.pages}")

    total_enqueued = 0
    with psycopg.connect(PG_DSN, autocommit=True) as conn:

        if tier in MASTER_PLUS:
            # Single call; large list
            entries = league_entries_master_plus(args.platform, args.queue, tier)
            log.info(f"{tier} entries returned: {len(entries)}")
            for e in entries:
                puuid = e.get("puuid")
                if puuid:
                    enqueue_puuid(conn, puuid, args.platform)
                    total_enqueued += 1
                    if total_enqueued % 100 == 0:
                        log.info(f"Enqueued so far: {total_enqueued}")
                    continue

                sid = e.get("summonerId")
                if not sid:
                    log.warning(f"Master+ entry missing both puuid and summonerId: {str(e)[:160]}")
                    continue
                summ = summoner_by_id(args.platform, sid)
                if not summ:
                    continue
                enqueue_puuid(conn, summ["puuid"], args.platform)
                total_enqueued += 1
        else:
            # Paged flow
            for page in range(1, args.pages + 1):
                entries = league_entries_paged(args.platform, args.queue, tier, args.division, page)
                log.info(f"Page {page}: got {len(entries)} entries")
                if not entries:
                    break
                for e in entries:
                    if not isinstance(e, dict):
                        log.warning(f"Non-dict entry on page {page}: {repr(e)[:160]}")
                        continue

                    puuid = e.get("puuid")
                    if puuid:
                        enqueue_puuid(conn, puuid, args.platform)
                        total_enqueued += 1
                        continue

                    sid = e.get("summonerId")
                    if sid:
                        summ = summoner_by_id(args.platform, sid)
                        if not summ:
                            continue
                        enqueue_puuid(conn, summ["puuid"], args.platform)
                        total_enqueued += 1
                        continue

                    log.warning(f"Entry missing both puuid and summonerId on page {page}: {str(e)[:160]}")

    log.info(f"Bootstrap complete. Total enqueued: {total_enqueued}")

if __name__ == "__main__":
    main()
