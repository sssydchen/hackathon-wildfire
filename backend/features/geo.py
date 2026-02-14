from __future__ import annotations

import math
from typing import Iterable, Optional, Tuple

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two coordinates in kilometers."""
    lat1_r = math.radians(lat1)
    lon1_r = math.radians(lon1)
    lat2_r = math.radians(lat2)
    lon2_r = math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from point 1 to point 2 in degrees [0, 360)."""
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    dlon_r = math.radians(lon2 - lon1)

    y = math.sin(dlon_r) * math.cos(lat2_r)
    x = math.cos(lat1_r) * math.sin(lat2_r) - (
        math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon_r)
    )

    bearing = math.degrees(math.atan2(y, x))
    return (bearing + 360.0) % 360.0


def wind_alignment_cos(fire_to_asset_bearing_deg: float, wind_to_bearing_deg: float) -> float:
    """Cosine alignment where 1 means wind pushes from fire to asset."""
    delta = math.radians(fire_to_asset_bearing_deg - wind_to_bearing_deg)
    return math.cos(delta)


def nearest_point(
    lat: float, lon: float, points: Iterable[Tuple[float, float]]
) -> Optional[Tuple[float, float, float]]:
    """Return (distance_km, nearest_lat, nearest_lon) for nearest point."""
    best: Optional[Tuple[float, float, float]] = None

    for p_lat, p_lon in points:
        d = haversine_km(lat, lon, p_lat, p_lon)
        if best is None or d < best[0]:
            best = (d, p_lat, p_lon)

    return best
