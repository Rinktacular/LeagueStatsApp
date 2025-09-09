# app/schemas/params.py
from typing import Optional, Literal
from fastapi import Query, HTTPException
from pydantic import BaseModel, field_validator
import re
from difflib import get_close_matches

# normalize a string for alias lookup (lowercase, strip spaces/punct)
def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.strip().lower())


CHAMP_ALIASES = {
    # apostrophe names... do we need these?
    _key("Kai'Sa"): "Kai'Sa",
    _key("Vel'Koz"): "Vel'Koz",
    _key("Kog'Maw"): "Kog'Maw",
    _key("Rek'Sai"): "Rek'Sai",
    _key("K'Sante"): "K'Sante",
    _key("Bel'Veth"): "Bel'Veth",
    _key("Cho'Gath"): "Cho'Gath",

    # common typed forms → canonical
    _key("kaisa"): "Kai'Sa",
    _key("velkoz"): "Vel'Koz",
    _key("kogmaw"): "Kog'Maw",
    _key("reksai"): "Rek'Sai",
    _key("ksante"): "K'Sante",
    _key("belveth"): "Bel'Veth",
    _key("cho"): "Cho'Gath",
    _key("chogath"): "Cho'Gath",
    _key("drmundo"): "Dr. Mundo",
    _key("missfortune"): "Miss Fortune",
    _key("masteryi"): "Master Yi",
    _key("xinzhao"): "Xin Zhao",
    _key("jarvaniv"): "Jarvan IV",
    _key("jarvan"): "Jarvan IV",
    _key("j4"): "Jarvan IV",
    _key("monkeyking"): "Wukong",
    _key("wukong"): "Wukong",
    _key("nunuandwillump"): "Nunu & Willump",
    _key("nunu & willump"): "Nunu & Willump",
}

# Canonical enums
Lane = Literal["TOP", "JUNGLE", "MID", "BOT", "SUPPORT"]

LANE_ALIASES = {
    "top": "TOP", "toplane": "TOP",
    "jg": "JUNGLE", "jungle": "JUNGLE",
    "mid": "MID", "middle": "MID", "midlane": "MID",
    "bot": "BOT", "adc": "BOT", "bottom": "BOT", "carry": "BOT",
    "sup": "SUPPORT", "support": "SUPPORT", "supp": "SUPPORT",
}

CANONICAL_CHAMPS: set[str] = set()  # empty means “don’t enforce strict set”    

def canonicalize_display(name: str) -> str:
    # prefer the canonical name if we know it; otherwise return the DB value
    return CHAMP_ALIASES.get(_key(name), name)

def _err(param: str, value: str, allowed: list[str], suggestions: list[str] = []):
    raise HTTPException(
        status_code=400,
        detail={
            "error": "invalid_param",
            "param": param,
            "value": value,
            "allowed": allowed,
            "suggestions": suggestions,
        },
    )

def _suggest(value: str, universe: list[str], n=3):
    return get_close_matches(value, universe, n=n, cutoff=0.6)

PATCH_RE = re.compile(r"^\d+\.\d+$")  # e.g., 14.18

def normalize_lane(raw: Optional[str]) -> Optional[Lane]:
    if raw is None or raw == "":
        return None
    key = raw.strip().lower()
    if key in LANE_ALIASES:
        return LANE_ALIASES[key]  # type: ignore[return-value]
    allowed = ["TOP", "JUNGLE", "MID", "BOT", "SUPPORT"]
    _err("lane", raw, allowed, _suggest(raw.upper(), allowed))

def normalize_patch(raw: Optional[str]) -> Optional[str]:
    if raw is None or raw == "":
        return None
    if PATCH_RE.match(raw.strip()):
        return raw.strip()
    _err("patch", raw, ["<major.minor> like 14.18"])

# If you have a roster cached, set it here (optional improvement below)
CANONICAL_CHAMPS: set[str] = set()  # empty means “don’t enforce strict set”

def normalize_champ(raw: Optional[str], param_name: str) -> Optional[str]:
    if raw is None or raw == "":
        return None
    key = _key(raw)
    # alias hit
    if key in CHAMP_ALIASES:
        return CHAMP_ALIASES[key]

    # if you have a canonical roster, enforce it; otherwise pass-through
    if CANONICAL_CHAMPS:
        # try case-insensitive match against roster
        for c in CANONICAL_CHAMPS:
            if _key(c) == key:
                return c
        # strict mode with suggestions if still unknown
        universe = sorted(CANONICAL_CHAMPS | set(CHAMP_ALIASES.values()))
        suggestions = get_close_matches(raw, universe, n=3, cutoff=0.6)
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_param",
                "param": param_name,
                "value": raw,
                "allowed": ["known champion name"],
                "suggestions": suggestions,
            },
        )

    # no strict roster → pass through and let DB matching (eq_ci) handle it
    return raw.strip()


class CommonQueryParams(BaseModel):
    lane: Optional[str] = None
    champ: Optional[str] = None
    opponent: Optional[str] = None
    enemy_jungler: Optional[str] = None
    ally_jungler: Optional[str] = None
    patch: Optional[str] = None
    min_n: int = 10
    limit: int = 50
    offset: int = 0
    sort: Optional[str] = None
    alpha: int = 20
    prior_wr: float = 0.50
    include_builds: bool = True
    warn_n: int = Query(25, ge=0, description="Rows with n < warn_n get low_sample=true") ## boolean flag to mark low sample size in response to user

    @field_validator("lane", mode="before")
    @classmethod
    def _v_lane(cls, v):
        return normalize_lane(v)

    @field_validator("patch", mode="before")
    @classmethod
    def _v_patch(cls, v):
        return normalize_patch(v)

    @field_validator("champ", mode="before")
    @classmethod
    def _v_champ(cls, v):
        return normalize_champ(v, "champ")

    @field_validator("opponent", mode="before")
    @classmethod
    def _v_opp(cls, v):
        return normalize_champ(v, "opponent")

    @field_validator("enemy_jungler", mode="before")
    @classmethod
    def _v_ejg(cls, v):
        return normalize_champ(v, "enemy_jungler")

    @field_validator("ally_jungler", mode="before")
    @classmethod
    def _v_ajg(cls, v):
        return normalize_champ(v, "ally_jungler")