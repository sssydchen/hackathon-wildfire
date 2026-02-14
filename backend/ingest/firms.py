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


def fetch_fires(
    bbox: str,
    days: int = 1,
    source: str = DEFAULT_SOURCE,
    min_confidence: float | None = None,
) -> List[Dict]:
    """
    Fetch FIRMS active fire points for bbox='west,south,east,north'.
    Requires NASA FIRMS API key in FIRMS_API_KEY env var.
    """
    cache_key = _cache_key(bbox, days, source)
    cached = load_cache(cache_key, CACHE_TTL_SECONDS)
    if cached is not None:
        return _filter_by_confidence(cached, min_confidence)

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

        confidence_raw = row.get("confidence")
        confidence_score = _confidence_score(confidence_raw)

        points.append(
            {
                "id": row.get("track") or row.get("acq_time") or f"fire_{len(points)}",
                "lat": float(lat),
                "lon": float(lon),
                "brightness": _float_or_none(row.get("bright_ti4") or row.get("brightness")),
                "confidence": confidence_raw,
                "confidence_score": confidence_score,
                "acq_date": row.get("acq_date"),
                "acq_time": row.get("acq_time"),
                "raw": row,
            }
        )

    save_cache(cache_key, points)
    return _filter_by_confidence(points, min_confidence)


def _filter_by_confidence(points: List[Dict], min_confidence: float | None) -> List[Dict]:
    if min_confidence is None:
        return points

    filtered = []
    for p in points:
        score = p.get("confidence_score")
        if score is None:
            continue
        if score >= min_confidence:
            filtered.append(p)
    return filtered


def _confidence_score(value: str | None) -> float | None:
    if value is None:
        return None

    raw = value.strip().lower()
    if raw == "":
        return None

    # FIRMS may emit categorical confidence labels in some products.
    label_map = {
        "l": 30.0,
        "low": 30.0,
        "n": 60.0,
        "nominal": 60.0,
        "h": 90.0,
        "high": 90.0,
    }
    if raw in label_map:
        return label_map[raw]

    try:
        return max(0.0, min(100.0, float(raw)))
    except ValueError:
        return None


def _float_or_none(value: str | None):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None
