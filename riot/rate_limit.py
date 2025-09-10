# riot/rate_limit.py
import time
from collections import deque
from typing import Dict

class MultiLimiter:
    """
    Enforces both: ≤per_sec in any 1s window AND ≤per_2min in any 120s window,
    keyed by 'routing' like 'na1','euw1','americas'.
    """
    def __init__(self, per_sec: int = 20, per_2min: int = 100):
        self.per_sec = per_sec
        self.per_2min = per_2min
        self._sec: Dict[str, deque] = {}
        self._long: Dict[str, deque] = {}

    def acquire(self, key: str):
        now = time.monotonic()
        dq1 = self._sec.setdefault(key, deque())
        dq2 = self._long.setdefault(key, deque())

        # prune old entries
        one = now - 1.0
        two = now - 120.0
        while dq1 and dq1[0] < one:
            dq1.popleft()
        while dq2 and dq2[0] < two:
            dq2.popleft()

        # if violating either window, sleep until the earliest becomes free
        if len(dq1) >= self.per_sec or len(dq2) >= self.per_2min:
            wait1 = max(0.0, (dq1[0] + 1.0) - now) if dq1 else 0.0
            wait2 = max(0.0, (dq2[0] + 120.0) - now) if dq2 else 0.0
            time.sleep(max(wait1, wait2))
            return self.acquire(key)

        # record this request
        dq1.append(now)
        dq2.append(now)

    @staticmethod
    def key_for_platform(platform_host: str) -> str:
        # league-v4 / summoner-v4 limits are per platform host (na1/euw1/kr/...)
        return platform_host.lower()

    @staticmethod
    def key_for_routing(routing: str) -> str:
        # match-v5 limits are per routing region (americas/europe/asia/sea)
        return routing.lower()
