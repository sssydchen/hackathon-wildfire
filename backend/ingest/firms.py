from __future__ import annotations

import csv
import io
import os
from typing import Dict, List

import requests

from .cache import load_cache, save_cache

FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
DEFAULT_SOURCE = "VIIRS_NOAA20_NRT"
CACHE_TTL_SECONDS = 15 * 60


def _cache_key(bbox: str, days: int, source: str) -> str:
    safe_bbox = bbox.replace(",", "_")
    return f"firms_{source}_{days}_{safe_bbox}"


def fetch_fires(bbox: str, days: int = 1, source: str = DEFAULT_SOURCE) -> List[Dict]:
    """
    Fetch FIRMS active fire points for bbox='west,south,east,north'.
    Requires NASA FIRMS API key in FIRMS_API_KEY env var.
    """
    cache_key = _cache_key(bbox, days, source)
    cached = load_cache(cache_key, CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    api_key = os.getenv("FIRMS_API_KEY")
    if not api_key:
        return []

    url = f"{FIRMS_BASE}/{api_key}/{source}/{bbox}/{days}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    points: List[Dict] = []
    reader = csv.DictReader(io.StringIO(resp.text))
    for row in reader:
        lat = row.get("latitude")
        lon = row.get("longitude")
        if lat is None or lon is None:
            continue
        points.append(
            {
                "id": row.get("track") or row.get("acq_time") or f"fire_{len(points)}",
                "lat": float(lat),
                "lon": float(lon),
                "brightness": _float_or_none(row.get("bright_ti4") or row.get("brightness")),
                "confidence": row.get("confidence"),
                "acq_date": row.get("acq_date"),
                "acq_time": row.get("acq_time"),
                "raw": row,
            }
        )

    save_cache(cache_key, points)
    return points


def _float_or_none(value: str | None):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None
