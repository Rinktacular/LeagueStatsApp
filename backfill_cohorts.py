# backfill_cohorts.py
import os, time, argparse
from dotenv import load_dotenv
import psycopg
from riot.riot_api import RiotClient
import httpx

load_dotenv()
API_KEY = os.environ["RIOT_API_KEY"]
PG_DSN  = os.environ["PG_DSN"]

PLATFORM_TO_ROUTING = {
    "na1":"americas","br1":"americas","la1":"americas","la2":"americas",
    "euw1":"europe","eun1":"europe","tr1":"europe","ru":"europe",
    "kr":"asia","jp1":"asia",
    "oc1":"sea","ph2":"sea","sg2":"sea","th2":"sea","tw2":"sea","vn2":"sea",
}
HOSTS = list(PLATFORM_TO_ROUTING.keys())
TIERS_ORDER = {"CHALLENGER":6,"GRANDMASTER":5,"MASTER":4,"DIAMOND":3,"EMERALD":2,"PLATINUM":1,"GOLD":0,"SILVER":-1,"BRONZE":-2,"IRON":-3}

def pick_soloq(entries):
    solo = [e for e in entries if e.get("queueType") == "RANKED_SOLO_5x5"]
    pool = solo or entries
    if not pool: return None
    return max(pool, key=lambda e: TIERS_ORDER.get(e.get("tier","").upper(), -99))

def count_pending():
    with psycopg.connect(PG_DSN) as con, con.cursor() as cur:
        cur.execute("""
          select count(distinct p.puuid)
          from participants p
          left join puuid_cohort_current c on c.puuid=p.puuid
          where p.puuid is not null and c.puuid is null
        """)
        return cur.fetchone()[0]

def fetch_batch(limit=50):
    # Try to also pull summoner_id if you store it
    sql = """
    with ranked_host as (
      select p.puuid,
             lower(split_part(m.match_id,'_',1)) as host,
             count(*) as cnt,
             row_number() over (partition by p.puuid order by count(*) desc) as rn,
             max(p.summoner_id) filter (where p.summoner_id is not null) as summoner_id
      from participants p
      join matches m on m.match_id=p.match_id
      left join puuid_cohort_current c on c.puuid=p.puuid
      where p.puuid is not null and c.puuid is null
      group by 1,2
    )
    select puuid, host, summoner_id
    from ranked_host
    where rn=1
    limit %s
    """
    with psycopg.connect(PG_DSN) as con, con.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def upsert_cohort(puuid, platform_host, tier, division):
    with psycopg.connect(PG_DSN, autocommit=True) as con, con.cursor() as cur:
        cur.execute("""
          insert into puuid_cohort_current (puuid, platform, tier, division, updated_at)
          values (%s, %s, %s, %s, now())
          on conflict (puuid) do update
            set platform  = excluded.platform,
                tier      = excluded.tier,
                division  = excluded.division,
                updated_at= now()
        """, (puuid, platform_host, tier, division))

def resolve_summoner_id(puuid, preferred_host) -> tuple[str|None, str|None]:
    """
    Return (summoner_id, host_used). Try preferred host; if 404, try other hosts.
    """
    tried = []
    hosts = [preferred_host] + [h for h in HOSTS if h != preferred_host]
    for host in hosts:
        tried.append(host)
        rc = RiotClient(API_KEY, region=host, platform=PLATFORM_TO_ROUTING.get(host,"americas"), timeout=10.0)
        try:
            summ = rc.get_summoner_by_puuid(puuid)
            sid = summ.get("id")
            if sid:
                return sid, host
            else:
                print(f"[backfill]    host {host}: 200 but no 'id' in body -> {summ}", flush=True)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 404:
                print(f"[backfill]    host {host}: 404 not found", flush=True)
                continue
            if code in (429,503,504):
                ra = e.response.headers.get("Retry-After","1")
                print(f"[backfill]    host {host}: {code}, sleeping {ra}s", flush=True)
                time.sleep(float(ra))
                continue
            print(f"[backfill]    host {host}: HTTP {code}", flush=True)
        except Exception as e:
            print(f"[backfill]    host {host}: ERROR {e}", flush=True)
    return None, None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    total = count_pending()
    print(f"[backfill] pending distinct puuids: {total}", flush=True)
    if total == 0: return

    processed = 0
    while True:
        if args.limit and processed >= args.limit:
            break
        rows = fetch_batch(limit=min(args.batch_size, (args.limit-processed) if args.limit else args.batch_size))
        if not rows:
            break
        print(f"[backfill] batch: {len(rows)} puuids", flush=True)

        for puuid, host, summoner_id in rows:
            print(f"[backfill] -> {host}:{puuid[:8]}…", flush=True)
            try:
                sid = summoner_id
                used_host = host
                if not sid:
                    sid, used_host = resolve_summoner_id(puuid, host)
                if not sid:
                    print(f"[backfill]    could not resolve summoner id (skipping for now)", flush=True)
                    # DO NOT mark UNRANKED here; we’ll retry later with a different key/host
                    continue

                # Now get ranked entries on the platform host we used to resolve SID
                rc = RiotClient(API_KEY, region=used_host, platform=PLATFORM_TO_ROUTING.get(used_host,"americas"), timeout=10.0)
                try:
                    entries = rc.get_ranked_entries_by_summoner(sid)
                except httpx.HTTPStatusError as e:
                    code = e.response.status_code
                    print(f"[backfill]    entries HTTP {code}", flush=True)
                    if code in (429,503,504):
                        ra = e.response.headers.get("Retry-After","1")
                        time.sleep(float(ra)); continue
                    continue

                pick = pick_soloq(entries)
                if not pick:
                    print(f"[backfill]    no ranked entries; marking UNRANKED", flush=True)
                    upsert_cohort(puuid, used_host, "UNRANKED", None)
                else:
                    tier = pick.get("tier","UNRANKED")
                    div  = pick.get("rank")
                    print(f"[backfill]    -> {tier} {div or ''}".rstrip(), flush=True)
                    upsert_cohort(puuid, used_host, tier, div)

                processed += 1
                time.sleep(args.sleep)

            except Exception as e:
                print(f"[backfill]    ERROR {e}", flush=True)
                time.sleep(0.2)

    print(f"[backfill] done. processed ~{processed}", flush=True)

if __name__ == "__main__":
    # run unbuffered: python -u backfill_cohorts.py
    main()
