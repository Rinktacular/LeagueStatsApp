import os, io, json, requests
from dotenv import load_dotenv

# psycopg2/3 compatibility
try:
    import psycopg
    _PG3=True
except ImportError:
    import psycopg2 as psycopg
    _PG3=False

load_dotenv()
PG_DSN = os.environ["PG_DSN"]

def get_latest_ddragon_patch():
    r = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=20)
    r.raise_for_status()
    return r.json()[0]  # latest, e.g. "14.20.1"

def fetch_champion_names(patch):
    url = f"https://ddragon.leagueoflegends.com/cdn/{patch}/data/en_US/champion.json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]
    # e.g. {"Aatrox":{"id":"Aatrox","name":"Aatrox",...}, ...}
    return sorted({v["name"] for v in data.values()})

# optional: opinionated overrides you can expand anytime
OVERRIDES = {
    # name: (dmg, has_engage, is_tank)
    "Alistar": ("AP", True, True),
    "Annie": ("AP", True, False),
    "Jarvan IV": ("AD", True, True),
    "Sejuani": ("AP", True, True),
    "Maokai": ("AP", True, True),
    "Poppy": ("AD", True, True),
    "Rell": ("AP", True, True),
    "Lux": ("AP", True, False),
    "Kai'Sa": ("MIXED", False, False),
    "Jayce": ("MIXED", False, False),
    "Akali": ("MIXED", True, False),
    "Yasuo": ("AD", True, False),
    "Zed": ("AD", False, False),
    "Orianna": ("AP", False, False),
    "Syndra": ("AP", False, False),
    "Ezreal": ("AD", False, False),
    "Gwen": ("AP", False, False),
    "Ambessa": ("AD", True, True),   # adjust if needed
    "Aurora": ("AP", True, False),   # adjust if needed
    "Yunara": ("AD", True, False),   # adjust if needed
}

def main():
    patch = get_latest_ddragon_patch()
    names = fetch_champion_names(patch)

    # upsert all champs with neutral defaults
    upsert_neutral = """
      insert into champ_tags (champ_name, dmg, has_engage, is_tank)
      values (%s, 'MIXED', false, false)
      on conflict (champ_name) do nothing;
    """

    # upsert overrides
    upsert_override = """
      insert into champ_tags (champ_name, dmg, has_engage, is_tank)
      values (%s, %s, %s, %s)
      on conflict (champ_name) do update
        set dmg=excluded.dmg, has_engage=excluded.has_engage, is_tank=excluded.is_tank;
    """

    with psycopg.connect(PG_DSN) as con:
        if _PG3: con.autocommit = True
        with con.cursor() as cur:
            for n in names:
                cur.execute(upsert_neutral, (n,))
            for n, (dmg, eng, tank) in OVERRIDES.items():
                cur.execute(upsert_override, (n, dmg, eng, tank))

    print(f"Seeded {len(names)} champs from DDragon; applied {len(OVERRIDES)} overrides.")

if __name__ == "__main__":
    main()
