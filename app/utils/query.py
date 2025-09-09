from typing import Optional, Tuple

# Allowed sort keys and their default directions
_ALLOWED_SORT_KEYS = {"winrate", "n", "gd10", "smoothed_wr"}

def parse_sort(sort: Optional[str]) -> Tuple[str, str]:
    """
    Returns (key, dir) where key ∈ _ALLOWED_SORT_KEYS and dir ∈ {'asc','desc'}.
    Supports '-key' for descending.
    Defaults to ('n','desc').
    """
    if not sort or not isinstance(sort, str):
        return ("n", "desc")
    s = sort.strip().lower()
    direction = "asc"
    if s.startswith("-"):
        direction = "desc"
        s = s[1:]
    if s not in _ALLOWED_SORT_KEYS:
        return ("n", "desc")
    return (s, direction)

def order_clause(sort: Optional[str]) -> str:
    key, direction = parse_sort(sort)

    # primary key
    if key == "winrate":
        primary = f"winrate {direction}"
    elif key == "gd10":
        primary = f"avg_gd10 {direction}"
    elif key == "smoothed_wr":
        primary = f"smoothed_wr {direction}"
    else:  # "n"
        primary = f"n {direction}"

    # stable tie-breakers (keep deterministic!)
    # - Keep n DESC as a strong secondary
    # - Then champ/opponent asc, then patch asc for consistency
    return f"order by {primary}, n desc, champ asc, opponent asc, patch asc"
