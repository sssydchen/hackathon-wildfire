from __future__ import annotations

import math
from functools import lru_cache
from statistics import mean
from typing import Dict, Iterable, List

import requests

from .cache import load_cache, save_cache

CACHE_TTL_SECONDS = 30 * 60

GRIDMET_URLS = {
    "vs": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_vs_1979_CurrentYear_CONUS.nc",
    "th": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_th_1979_CurrentYear_CONUS.nc",
    "tmmn": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_tmmn_1979_CurrentYear_CONUS.nc",
    "tmmx": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_tmmx_1979_CurrentYear_CONUS.nc",
    "rmin": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_rmin_1979_CurrentYear_CONUS.nc",
    "rmax": "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_rmax_1979_CurrentYear_CONUS.nc",
}


def _cache_key(lat: float, lon: float, hours: int, source: str) -> str:
    return f"weather_{source}_{lat:.3f}_{lon:.3f}_{hours}"


def fetch_weather_summary(lat: float, lon: float, hours: int = 24, source: str = "gridmet") -> Dict:
    """
    Fetch weather summary for risk scoring.
    Supported sources: gridmet, openmeteo.
    """
    normalized_source = source.strip().lower()
    cache_key = _cache_key(lat, lon, hours, normalized_source)
    cached = load_cache(cache_key, CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    if normalized_source == "gridmet":
        try:
            summary = fetch_gridmet_summary(lat=lat, lon=lon, hours=hours)
            save_cache(cache_key, summary)
            return summary
        except Exception:
            # Fallback keeps API responsive during THREDDS outages.
            fallback = fetch_openmeteo_summary(lat=lat, lon=lon, hours=hours)
            fallback["weather_source"] = "openmeteo_fallback"
            save_cache(cache_key, fallback)
            return fallback

    if normalized_source == "openmeteo":
        summary = fetch_openmeteo_summary(lat=lat, lon=lon, hours=hours)
        summary["weather_source"] = "openmeteo"
        save_cache(cache_key, summary)
        return summary

    raise ValueError(f"Unsupported weather source: {source}")


def fetch_openmeteo_summary(lat: float, lon: float, hours: int = 24) -> Dict:
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

    return {
        "temperature_c": float(mean(temps)) if temps else 25.0,
        "humidity_pct": float(mean(humidity)) if humidity else 35.0,
        "wind_speed_kmh": float(mean(wind_speed)) if wind_speed else 15.0,
        "wind_direction_deg": _circular_mean_deg(wind_dir) if wind_dir else 180.0,
    }


def fetch_gridmet_summary(lat: float, lon: float, hours: int = 24) -> Dict:
    try:
        import xarray as xr
    except ImportError as exc:
        raise RuntimeError("xarray is required for GridMET weather source") from exc

    n_days = max(1, math.ceil(hours / 24.0))
    lon_360 = lon if lon >= 0 else lon + 360.0

    vs_vals = _gridmet_values(xr, "vs", lat, lon_360, n_days)
    th_vals = _gridmet_values(xr, "th", lat, lon_360, n_days)
    tmmn_vals = _gridmet_values(xr, "tmmn", lat, lon_360, n_days)
    tmmx_vals = _gridmet_values(xr, "tmmx", lat, lon_360, n_days)
    rmin_vals = _gridmet_values(xr, "rmin", lat, lon_360, n_days)
    rmax_vals = _gridmet_values(xr, "rmax", lat, lon_360, n_days)

    # GridMET vs is m/s. Convert to km/h.
    wind_speed_kmh = (_mean_or_default(vs_vals, 4.2)) * 3.6
    wind_direction = _circular_mean_deg(th_vals) if th_vals else 180.0

    # GridMET temperature usually Kelvin for tmmn/tmmx.
    avg_temp_raw = _mean_or_default(tmmn_vals + tmmx_vals, 298.15)
    temperature_c = avg_temp_raw - 273.15 if avg_temp_raw > 150 else avg_temp_raw

    # Humidity proxy from daily min/max relative humidity.
    humidity_pct = _mean_or_default(rmin_vals + rmax_vals, 35.0)

    return {
        "temperature_c": float(temperature_c),
        "humidity_pct": float(humidity_pct),
        "wind_speed_kmh": float(wind_speed_kmh),
        "wind_direction_deg": float(wind_direction),
        "weather_source": "gridmet",
    }


@lru_cache(maxsize=16)
def _open_gridmet_dataset(url: str):
    import xarray as xr

    return xr.open_dataset(url)


def _gridmet_values(xr_module, var_name: str, lat: float, lon_360: float, n_days: int) -> List[float]:
    ds = _open_gridmet_dataset(GRIDMET_URLS[var_name])
    if var_name not in ds:
        return []

    time_coord = "day" if "day" in ds.coords else ("time" if "time" in ds.coords else None)
    if time_coord is None:
        return []

    series = ds[var_name].sel(lat=lat, lon=lon_360, method="nearest")
    recent = series.isel({time_coord: slice(-n_days, None)})
    values = recent.values

    out: List[float] = []
    for v in values:
        try:
            fv = float(v)
            if not math.isnan(fv):
                out.append(fv)
        except Exception:
            continue
    return out


def _mean_or_default(values: Iterable[float], default: float) -> float:
    vals = list(values)
    if not vals:
        return default
    return float(mean(vals))


def _circular_mean_deg(values: Iterable[float]) -> float:
    vals = list(values)
    if not vals:
        return 180.0

    sin_sum = 0.0
    cos_sum = 0.0
    for v in vals:
        r = math.radians(v)
        sin_sum += math.sin(r)
        cos_sum += math.cos(r)

    if abs(sin_sum) < 1e-9 and abs(cos_sum) < 1e-9:
        return 180.0

    angle = math.degrees(math.atan2(sin_sum, cos_sum))
    return (angle + 360.0) % 360.0
