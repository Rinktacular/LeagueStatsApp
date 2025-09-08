import time
from typing import Any, Dict, List, Optional
import httpx

class RiotClient:
    def __init__(self, api_key: str, region: str, platform: str, timeout=15.0):
        self.api_key = api_key
        self.region = region       # na1/euw1/kr
        self.platform = platform   # americas/europe/asia/sea
        self.client = httpx.Client(timeout=timeout)

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        headers = {"X-Riot-Token": self.api_key}
        while True:
            r = self.client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "2"))
                time.sleep(retry_after)
                continue
            r.raise_for_status()
            return r.json()

    def get_challenger_entries(self, queue="RANKED_SOLO_5x5") -> List[Dict[str, Any]]:
        url = f"https://{self.region}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{queue}"
        data = self._get(url)
        return data.get("entries", [])

    def get_summoner_by_id(self, summoner_id: str) -> Dict[str, Any]:
        url = f"https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
        return self._get(url)
    
    def get_summoner_by_name(self, name: str) -> Dict[str, Any]:
        url = f"https://{self.region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{name}"
        return self._get(url)


    def get_match_ids_by_puuid(self, puuid: str, queue: int, start: int = 0, count: int = 20,
                               start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[str]:
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"queue": queue, "start": start, "count": count}
        if start_time: params["startTime"] = start_time
        if end_time: params["endTime"] = end_time
        return self._get(url, params=params)

    def get_match(self, match_id: str) -> Dict[str, Any]:
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        return self._get(url)

    def get_timeline(self, match_id: str) -> Dict[str, Any]:
        url = f"https://{self.platform}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline"
        return self._get(url)
