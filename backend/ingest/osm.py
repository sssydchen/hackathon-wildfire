from __future__ import annotations

from typing import Dict, List

import requests

from .cache import load_cache, save_cache

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
CACHE_TTL_SECONDS = 60 * 60

OVERPASS_QUERY_TEMPLATE = """
[out:json][timeout:25];
(
  node["power"="substation"]({south},{west},{north},{east});
  way["power"="substation"]({south},{west},{north},{east});

  way["power"="line"]({south},{west},{north},{east});
  way["power"="minor_line"]({south},{west},{north},{east});

  node["amenity"="hospital"]({south},{west},{north},{east});
  way["amenity"="hospital"]({south},{west},{north},{east});

  node["man_made"="water_works"]({south},{west},{north},{east});
  way["man_made"="water_works"]({south},{west},{north},{east});
  node["utility"="water"]({south},{west},{north},{east});

  way["highway"~"motorway|trunk|primary|secondary"]({south},{west},{north},{east});
);
out center tags;
""".strip()


def fetch_assets(bbox: str) -> List[Dict]:
    """Fetch selected infrastructure assets from OSM Overpass for bbox west,south,east,north."""
    cache_key = f"osm_{bbox.replace(',', '_')}"
    cached = load_cache(cache_key, CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    west, south, east, north = [float(v) for v in bbox.split(",")]
    query = OVERPASS_QUERY_TEMPLATE.format(
        south=south,
        west=west,
        north=north,
        east=east,
    )

    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    assets: List[Dict] = []
    for elem in data.get("elements", []):
        lat, lon = _extract_lat_lon(elem)
        if lat is None or lon is None:
            continue

        tags = elem.get("tags", {})
        asset_type = _classify_asset(tags)
        if asset_type is None:
            continue

        assets.append(
            {
                "id": f"osm_{elem.get('type')}_{elem.get('id')}",
                "lat": lat,
                "lon": lon,
                "asset_type": asset_type,
                "name": tags.get("name") or asset_type,
                "tags": tags,
            }
        )

    save_cache(cache_key, assets)
    return assets


def _extract_lat_lon(elem: Dict):
    if "lat" in elem and "lon" in elem:
        return float(elem["lat"]), float(elem["lon"])
    center = elem.get("center")
    if center and "lat" in center and "lon" in center:
        return float(center["lat"]), float(center["lon"])
    return None, None


def _classify_asset(tags: Dict) -> str | None:
    power = tags.get("power")
    amenity = tags.get("amenity")
    man_made = tags.get("man_made")
    utility = tags.get("utility")
    highway = tags.get("highway")

    if power == "substation":
        return "substation"
    if power in {"line", "minor_line"}:
        return "power_line"
    if amenity == "hospital":
        return "hospital"
    if man_made == "water_works" or utility == "water":
        return "water_facility"
    if highway in {"motorway", "trunk", "primary", "secondary"}:
        return "major_road"
    return None


def get_overpass_query(bbox: str) -> str:
    west, south, east, north = [float(v) for v in bbox.split(",")]
    return OVERPASS_QUERY_TEMPLATE.format(
        south=south,
        west=west,
        north=north,
        east=east,
    )
