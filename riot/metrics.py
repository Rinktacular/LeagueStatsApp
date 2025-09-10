# riot/metrics.py
from __future__ import annotations
import time
from collections import defaultdict, deque, Counter
from threading import Lock, Thread
from typing import Dict, Deque, Tuple, Optional

_WINDOW_1S = 1.0
_WINDOW_120S = 120.0

class Metrics:
    """
    Thread-safe sliding-window metrics.
    Tracks requests per (scope, key, endpoint) and prints a periodic summary.
    - scope: 'platform' (na1/euw1/kr/...) or 'routing' (americas/europe/asia/sea)
    - key:   actual platform_host or routing string
    - endpoint: 'league', 'summoner', 'match', 'timeline', etc.
    Also tracks queue size, enqueued, processed, 429s, errors.
    """
    def __init__(self):
        self._lock = Lock()
        # timestamps for sliding windows
        self._events: Dict[Tuple[str, str, str], Deque[float]] = defaultdict(deque)
        # counters (total since start)
        self._totals = Counter()
        # gauges
        self._queue_size: Optional[int] = None
        self._last_print = 0.0
        self._reporter: Optional[Thread] = None
        self._running = False

    def start_reporter(self, interval: float = 10.0):
        if self._reporter and self._reporter.is_alive():
            return
        self._running = True
        self._reporter = Thread(target=self._loop, args=(interval,), daemon=True)
        self._reporter.start()

    def stop_reporter(self):
        self._running = False

    def _loop(self, interval: float):
        while self._running:
            time.sleep(interval)
            try:
                self.print_summary()
            except Exception:
                pass

    def record_request(self, scope: str, key: str, endpoint: str):
        now = time.monotonic()
        k = (scope, key.lower(), endpoint)
        with self._lock:
            dq = self._events[k]
            dq.append(now)
            self._totals[f"req_total::{scope}::{key.lower()}::{endpoint}"] += 1

    def record_429(self, scope: str, key: str, endpoint: str):
        with self._lock:
            self._totals[f"429::{scope}::{key.lower()}::{endpoint}"] += 1

    def record_error(self, scope: str, key: str, endpoint: str):
        with self._lock:
            self._totals[f"err::{scope}::{key.lower()}::{endpoint}"] += 1

    def record_enqueued(self, routing: str, n: int):
        with self._lock:
            self._totals[f"enqueued::{routing.lower()}"] += n

    def record_processed(self, routing: str, n: int = 1):
        with self._lock:
            self._totals[f"processed::{routing.lower()}"] += n

    def set_queue_size(self, n: int):
        with self._lock:
            self._queue_size = n

    def _prune(self):
        now = time.monotonic()
        with self._lock:
            for dq in self._events.values():
                # prune > 120s; we compute both 1s and 120s rates from same deque
                cutoff = now - _WINDOW_120S
                while dq and dq[0] < cutoff:
                    dq.popleft()

    def _rates(self):
        now = time.monotonic()
        one = now - _WINDOW_1S
        two = now - _WINDOW_120S
        rates_1s: Dict[Tuple[str, str, str], int] = {}
        rates_120s: Dict[Tuple[str, str, str], int] = {}
        with self._lock:
            for k, dq in self._events.items():
                # dq already pruned to 120s
                # 1s window count
                cnt_1s = 0
                for t in reversed(dq):
                    if t < one:
                        break
                    cnt_1s += 1
                rates_1s[k] = cnt_1s
                rates_120s[k] = len(dq)  # 120s window size
        return rates_1s, rates_120s

    def print_summary(self):
        self._prune()
        rates1, rates2 = self._rates()

        # Aggregate nicely for log line
        by_scope_key = defaultdict(lambda: {"1s": Counter(), "120s": Counter(), "endpoints": defaultdict(lambda: {"1s": 0, "120s": 0})})
        for (scope, key, ep), v1 in rates1.items():
            v2 = rates2[(scope, key, ep)]
            d = by_scope_key[(scope, key)]
            d["1s"][ep] += v1
            d["120s"][ep] += v2
            d["endpoints"][ep]["1s"] += v1
            d["endpoints"][ep]["120s"] += v2

        lines = []
        with self._lock:
            qsz = self._queue_size
            totals = dict(self._totals)

        lines.append("=== pacing ===")
        if qsz is not None:
            lines.append(f"queue_size={qsz}")

        # per scope/key summary
        for (scope, key), d in sorted(by_scope_key.items()):
            tot1 = sum(d["1s"].values())
            tot2 = sum(d["120s"].values())
            # convert 120s to per-second average for readability
            avg2 = tot2 / 120.0 if tot2 else 0.0
            lines.append(f"{scope}:{key} -> {tot1:2d}/s, {avg2:.2f}/s (120s window)")
            # endpoint breakdown
            for ep in sorted(d["endpoints"].keys()):
                v1 = d["endpoints"][ep]["1s"]
                v2 = d["endpoints"][ep]["120s"]
                lines.append(f"  - {ep:<8} {v1:2d}/s, {v2/120.0:.2f}/s (120s)")

        # totals
        # enqueued/processed per routing
        for k, v in sorted(totals.items()):
            if k.startswith("enqueued::") or k.startswith("processed::") or k.startswith("429::") or k.startswith("err::"):
                lines.append(f"{k}={v}")

        print("\n".join(lines))
