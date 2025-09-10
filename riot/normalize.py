# riot/normalize.py
def derive_patch(game_version: str) -> str:
    # "25.17.456.1234" -> "25.17"
    parts = (game_version or "").split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else game_version

def derive_lane_role(part: dict) -> tuple[str, str]:
    # Use Riot fields with your normalization
    lane = (part.get("teamPosition") or part.get("lane") or "UNKNOWN").upper()
    if lane in ("MIDDLE", "MID"): lane = "MID"
    if lane in ("BOTTOM", "BOT"): lane = "ADC" if part.get("teamPosition","").upper() in ("BOTTOM","BOT") else lane
    role_d = "UNKNOWN"
    if lane == "JUNGLE": role_d = "JUNGLE"
    elif lane == "TOP": role_d = "TOP"
    elif lane == "MID": role_d = "MID"
    elif lane == "ADC": role_d = "BOT_CARRY"
    elif lane == "SUPPORT": role_d = "SUPPORT"
    return lane, role_d
