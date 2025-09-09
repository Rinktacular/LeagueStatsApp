# app/meta/ddragon.py
import httpx
from typing import Dict, Optional

CDN = "https://ddragon.leagueoflegends.com"
VERSIONS_URL = f"{CDN}/api/versions.json"

class DDragonCache:
    def __init__(self):
        self.version: Optional[str] = None
        self.lang: str = "en_US"
        self.items: Dict[int, Dict[str, str]] = {}
        self.keystones: Dict[int, Dict[str, str]] = {}
        self.styles: Dict[int, Dict[str, str]] = {}

    async def refresh(self, patch_hint: Optional[str] = None, lang: Optional[str] = None):
        if lang:
            self.lang = lang

        async with httpx.AsyncClient(timeout=10) as client:
            versions = (await client.get(VERSIONS_URL)).json()
            # If user provided "14.15", find first version starting with that; else use latest
            version = next((v for v in versions if patch_hint and v.startswith(patch_hint)), None) or versions[0]
            self.version = version

            # Items
            items_resp = await client.get(f"{CDN}/cdn/{version}/data/{self.lang}/item.json")
            items_data = items_resp.json()["data"]
            self.items = {
                int(item_id): {
                    "name": meta.get("name", str(item_id)),
                    "icon": f"{CDN}/cdn/{version}/img/item/{item_id}.png",
                }
                for item_id, meta in items_data.items()
            }

            # Runes (runesReforged)
            runes_resp = await client.get(f"{CDN}/cdn/{version}/data/{self.lang}/runesReforged.json")
            runes = runes_resp.json()

            ks, styles = {}, {}
            for tree in runes:
                style_id = int(tree["id"])
                styles[style_id] = {
                    "name": tree["name"],
                    "icon": f"{CDN}/cdn/img/{tree['icon']}",
                }
                for slot in tree["slots"]:
                    for perk in slot["runes"]:
                        ks[int(perk["id"])] = {
                            "name": perk["name"],
                            "icon": f"{CDN}/cdn/img/{perk['icon']}",
                            "style_id": style_id,
                        }
            self.keystones, self.styles = ks, styles

DD = DDragonCache()
