import math
from typing import Tuple

def wilson_ci(p: float, n: int, z: float = 1.96) -> Tuple[float, float]:
    """
    Wilson score interval for a binomial proportion.
    Returns (low, high). If n == 0, returns (0.0, 1.0).
    """
    if n <= 0:
        return 0.0, 1.0
    denom = 1 + (z**2) / n
    center = p + (z**2) / (2 * n)
    adj = z * math.sqrt((p * (1 - p) + (z**2) / (4 * n)) / n)
    low = (center - adj) / denom
    high = (center + adj) / denom
    # numeric safety
    return max(0.0, low), min(1.0, high)
