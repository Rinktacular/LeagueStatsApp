from fastapi import APIRouter, Query
from .ddragon import DD

router = APIRouter(prefix="/meta", tags=["Meta"])

@router.get("/refresh")
async def meta_refresh(patch: str | None = Query(None), lang: str | None = Query(None)):
    """Force-refresh Data Dragon caches. If patch is provided (e.g., '14.15'), try to use it."""
    await DD.refresh(patch_hint=patch, lang=lang)
    return {"ok": True, "version": DD.version, "lang": DD.lang}

@router.get("/items")
def meta_items():
    """Map of itemId -> {name, icon}."""
    return {"version": DD.version, "items": DD.items}

@router.get("/runes")
def meta_runes():
    """Keystones and styles maps for runes reforged."""
    return {"version": DD.version, "keystones": DD.keystones, "styles": DD.styles}
