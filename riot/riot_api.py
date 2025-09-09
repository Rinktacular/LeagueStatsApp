import time
from typing import Any, Dict, List, Optional
import httpx

class RiotClient:
    """
    region  = platform host (e.g., na1, euw1, kr)
    platform = routing region (e.g., americas, europe, asia, sea)

    League/Summoner endpoints use 'region' (platform host).
    Match v5 endpoints use 'platform' (routing region).
    """
    def __init__(self, api_key: str, region: str, platform: str, timeout: float = 15.0):
        self.api_key = api_key
        self.region = region
        self.platform = platform
        self.client = httpx.Client(timeout=timeout)

    def _get(self, url, params=None):
        headers = {"X-Riot-Token": self.api_key, "User-Agent": "league-context/1.0"}
        backoff = 1.0
        while True:
            r = self.client.get(url, params=params, headers=headers)
            if r.status_code in (401, 403):
                # show the reason and raise immediately
                try:
                    print(f"[riot] {r.status_code} url={url} body={r.text}", flush=True)
                finally:
                    r.raise_for_status()
            if r.status_code in (429, 503, 504):
                ra = r.headers.get("Retry-After")
                delay = float(ra) if ra and ra.isdigit() else backoff
                print(f"[riot] {r.status_code} sleeping {delay}s url={url}", flush=True)
                time.sleep(delay); backoff = min(backoff * 2, 16.0); continue
            r.raise_for_status()
            return r.json()


    # --- League lists (Master+) ---
    def get_challenger_entries(self, queue: str = "RANKED_SOLO_5x5") -> List[Dict[str, Any]]:
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{queue}"
        data = self._get(url)
        return data.get("entries", [])  # [{summonerId, summonerName, ...}]

    def get_grandmaster_entries(self, queue: str = "RANKED_SOLO_5x5") -> List[Dict[str, Any]]:
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{queue}"
        data = self._get(url)
        return data.get("entries", [])

    def get_master_entries(self, queue: str = "RANKED_SOLO_5x5") -> List[Dict[str, Any]]:
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/masterleagues/by-queue/{queue}"
        data = self._get(url)
        return data.get("entries", [])

    def get_entries_paginated(
        self,
        queue: str = "RANKED_SOLO_5x5",
        tier: str = "DIAMOND",
        division: str = "I",
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        league-v4 entries for sub-Master tiers with pagination:
        GET /lol/league/v4/entries/{queue}/{tier}/{division}?page={page}
        """
        tier = tier.upper()
        division = division.upper()
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/entries/{queue}/{tier}/{division}"
        return self._get(url, params={"page": page})

    # --- Summoner (platform host) ---
    def get_summoner_by_id(self, summoner_id: str) -> Dict[str, Any]:
        url = f"https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
        return self._get(url)

    def get_summoner_by_name(self, name: str) -> Dict[str, Any]:
        url = f"https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{name}"
        return self._get(url)
    
    def get_summoner_by_puuid(self, puuid: str) -> dict:
        url = f"https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
        return self._get(url)

    def get_ranked_entries_by_summoner(self, summoner_id: str) -> list[dict]:
        # GET /lol/league/v4/entries/by-summoner/{summonerId}
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/entries/by-summoner/{summoner_id}"
        return self._get(url)

    # --- Match v5 (routing region) ---
    def get_match_ids_by_puuid(
        self,
        puuid: str,
        queue: int,
        start: int = 0,
        count: int = 20,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        type_: Optional[str] = "ranked",
    ) -> List[str]:
        """
        GET /lol/match/v5/matches/by-puuid/{puuid}/ids?queue=&start=&count=&startTime=&endTime=&type=
        """
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params: Dict[str, Any] = {"queue": queue, "start": start, "count": count}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if type_:
            params["type"] = type_
        return self._get(url, params=params)

    def get_match(self, match_id: str) -> Dict[str, Any]:
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        return self._get(url)

    def get_timeline(self, match_id: str) -> Dict[str, Any]:
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline"
        return self._get(url)
