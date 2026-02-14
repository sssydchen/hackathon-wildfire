from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List

from backend.features.geo import bearing_deg, nearest_point, wind_alignment_cos


@dataclass
class RiskConfig:
    alpha_dist: float = 1.1
    alpha_wind: float = 0.08
    alpha_humidity: float = 0.03
    base_bias: float = -1.2


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def compute_asset_risk(
    asset: Dict,
    fires: List[Dict],
    weather: Dict,
    config: RiskConfig | None = None,
) -> Dict:
    cfg = config or RiskConfig()

    if not fires:
        return {
            "asset_id": asset["id"],
            "risk_score": 0.0,
            "risk_bucket": "low",
            "features": {
                "min_dist_to_fire_km": None,
                "wind_alignment": None,
                "effective_dist": None,
                "wind_speed_kmh": weather.get("wind_speed_kmh"),
                "humidity_pct": weather.get("humidity_pct"),
            },
        }

    nearest = nearest_point(asset["lat"], asset["lon"], [(f["lat"], f["lon"]) for f in fires])
    if nearest is None:
        return {
            "asset_id": asset["id"],
            "risk_score": 0.0,
            "risk_bucket": "low",
            "features": {},
        }

    dist_km, fire_lat, fire_lon = nearest
    fire_to_asset = bearing_deg(fire_lat, fire_lon, asset["lat"], asset["lon"])
    wind_to = float(weather.get("wind_direction_deg", 180.0))
    alignment = wind_alignment_cos(fire_to_asset, wind_to)
    wind_speed = float(weather.get("wind_speed_kmh", 15.0))
    humidity = float(weather.get("humidity_pct", 35.0))

    effective_dist = dist_km / max(0.2, wind_speed * max(0.0, alignment) + 0.3)

    linear = (
        cfg.base_bias
        + cfg.alpha_dist * (5.0 - dist_km)
        + cfg.alpha_wind * wind_speed * alignment
        - cfg.alpha_humidity * humidity
    )
    risk = sigmoid(linear)

    return {
        "asset_id": asset["id"],
        "risk_score": round(risk, 4),
        "risk_bucket": _bucket(risk),
        "features": {
            "min_dist_to_fire_km": round(dist_km, 3),
            "wind_alignment": round(alignment, 3),
            "effective_dist": round(effective_dist, 3),
            "wind_speed_kmh": round(wind_speed, 2),
            "humidity_pct": round(humidity, 2),
        },
    }


def _bucket(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"
