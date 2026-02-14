from __future__ import annotations

from typing import Dict, List

from backend.features.geo import haversine_km


def compute_cascade_impacts(
    assets: List[Dict],
    risks_by_asset_id: Dict[str, Dict],
    fires: List[Dict],
    substation_threshold: float = 0.7,
    outage_radius_km: float = 8.0,
) -> Dict:
    by_type: Dict[str, List[Dict]] = {}
    for asset in assets:
        by_type.setdefault(asset["asset_type"], []).append(asset)

    hospitals = by_type.get("hospital", [])
    water = by_type.get("water_facility", [])
    roads = by_type.get("major_road", [])

    cards = []

    for sub in by_type.get("substation", []):
        risk = risks_by_asset_id.get(sub["id"], {}).get("risk_score", 0.0)
        if risk < substation_threshold:
            continue

        impacted_hospitals = _assets_within_radius(sub, hospitals, outage_radius_km)
        impacted_water = _assets_within_radius(sub, water, outage_radius_km)

        cards.append(
            {
                "type": "substation_outage",
                "trigger_asset_id": sub["id"],
                "trigger_name": sub.get("name", "substation"),
                "trigger_risk": risk,
                "impacted_hospitals": impacted_hospitals,
                "impacted_water_facilities": impacted_water,
            }
        )

    compromised_roads = []
    for road in roads:
        nearest_fire_dist = _nearest_fire_dist_km(road, fires)
        if nearest_fire_dist is not None and nearest_fire_dist <= 2.0:
            compromised_roads.append(
                {
                    "asset_id": road["id"],
                    "name": road.get("name", "road"),
                    "distance_to_fire_km": round(nearest_fire_dist, 3),
                    "status": "compromised",
                }
            )

    return {
        "cascade_cards": cards,
        "compromised_roads": compromised_roads,
    }


def _assets_within_radius(source: Dict, candidates: List[Dict], radius_km: float) -> List[Dict]:
    impacted = []
    for c in candidates:
        d = haversine_km(source["lat"], source["lon"], c["lat"], c["lon"])
        if d <= radius_km:
            impacted.append(
                {
                    "asset_id": c["id"],
                    "name": c.get("name", c["asset_type"]),
                    "distance_km": round(d, 3),
                }
            )
    return impacted


def _nearest_fire_dist_km(asset: Dict, fires: List[Dict]):
    if not fires:
        return None
    return min(
        haversine_km(asset["lat"], asset["lon"], f["lat"], f["lon"]) for f in fires
    )
