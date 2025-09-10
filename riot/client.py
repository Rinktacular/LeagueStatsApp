# riot/client.py
import os, time, random, requests
from typing import Any

from dotenv import load_dotenv

load_dotenv()

RIOT_KEY = os.environ["RIOT_API_KEY"]
HEAD = {"X-Riot-Token": RIOT_KEY}

def _sleep_backoff(resp: requests.Response) -> bool:
    if resp.status_code in (429, 503):
        retry = int(resp.headers.get("Retry-After", "2"))
        time.sleep(retry)
        return True
    return False

def request_json(url: str) -> Any:
    while True:
        r = requests.get(url, headers=HEAD, timeout=30)
        if _sleep_backoff(r): 
            continue
        r.raise_for_status()
        return r.json()

def match_ids_by_puuid(routing: str, puuid: str, start=0, count=100, queue=None, start_time=None, end_time=None):
    base = f"https://{routing}.api.riotgames.com"
    params = [f"start={start}", f"count={count}"]
    if queue is not None: params.append(f"queue={queue}")
    if start_time: params.append(f"startTime={start_time}")
    if end_time: params.append(f"endTime={end_time}")
    url = f"{base}/lol/match/v5/matches/by-puuid/{puuid}/ids?{'&'.join(params)}"
    return request_json(url)

def get_match(routing: str, match_id: str):
    url = f"https://{routing}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return request_json(url)

def get_timeline(routing: str, match_id: str):
    url = f"https://{routing}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline"
    return request_json(url)

# Data Dragon (names)
def ddragon_versions():
    return requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=30).json()

def ddragon_latest_version():
    return ddragon_versions()[0]

def ddragon_champions(version: str):
    j = requests.get(f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json", timeout=30).json()
    out = {}
    for _, c in j["data"].items():
        out[int(c["key"])] = c["name"]
    return out

def ddragon_items(version: str):
    j = requests.get(f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json", timeout=30).json()
    out = {}
    for iid, c in j["data"].items():
        try:
            out[int(iid)] = c["name"]
        except:
            pass
    return out
