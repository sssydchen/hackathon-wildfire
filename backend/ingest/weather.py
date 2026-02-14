from __future__ import annotations

from statistics import mean
from typing import Dict

import requests

from .cache import load_cache, save_cache

CACHE_TTL_SECONDS = 30 * 60


def _cache_key(lat: float, lon: float, hours: int) -> str:
    return f"weather_{lat:.3f}_{lon:.3f}_{hours}"


def fetch_weather_summary(lat: float, lon: float, hours: int = 24) -> Dict:
    """Use Open-Meteo for no-auth forecast summary used by risk scoring."""
    cache_key = _cache_key(lat, lon, hours)
    cached = load_cache(cache_key, CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m"
        "&forecast_days=2"
    )
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json().get("hourly", {})

    take = max(1, min(hours, len(data.get("temperature_2m", []))))

    temps = data.get("temperature_2m", [])[:take]
    humidity = data.get("relative_humidity_2m", [])[:take]
    wind_speed = data.get("wind_speed_10m", [])[:take]
    wind_dir = data.get("wind_direction_10m", [])[:take]

    summary = {
        "temperature_c": float(mean(temps)) if temps else 25.0,
        "humidity_pct": float(mean(humidity)) if humidity else 35.0,
        "wind_speed_kmh": float(mean(wind_speed)) if wind_speed else 15.0,
        "wind_direction_deg": float(mean(wind_dir)) if wind_dir else 180.0,
    }

    save_cache(cache_key, summary)
    return summary
