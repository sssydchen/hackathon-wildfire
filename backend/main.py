from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from backend.cascade.impact import compute_cascade_impacts
from backend.ingest.firms import fetch_fires
from backend.ingest.osm import fetch_assets, get_overpass_query
from backend.ingest.weather import fetch_weather_summary
from backend.model.risk_model import compute_asset_risk

app = FastAPI(title="Wildfire Cascade API", version="0.1.0")

SCENARIO_DIR = Path(__file__).resolve().parent / "data_cache" / "scenarios"


class RiskRequest(BaseModel):
    bbox: str = Field(..., description="west,south,east,north")
    horizon_hours: int = Field(default=24, ge=1, le=48)
    firms_days: int = Field(default=1, ge=1, le=10)
    fire_source: str = Field(default="VIIRS_NOAA20_NRT")


@app.get("/health")
def health() -> Dict:
    return {"ok": True}


@app.get("/fires")
def fires(
    bbox: str = Query(..., description="west,south,east,north"),
    days: int = 1,
    source: str = "VIIRS_NOAA20_NRT",
) -> Dict:
    try:
        points = fetch_fires(bbox=bbox, days=days, source=source)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch fires: {exc}")
    return {"bbox": bbox, "count": len(points), "fires": points}


@app.get("/assets")
def assets(bbox: str = Query(..., description="west,south,east,north")) -> Dict:
    try:
        results = fetch_assets(bbox=bbox)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch assets: {exc}")
    return {"bbox": bbox, "count": len(results), "assets": results}


@app.get("/assets/overpass_query")
def assets_overpass_query(bbox: str = Query(..., description="west,south,east,north")) -> Dict:
    return {"bbox": bbox, "query": get_overpass_query(bbox)}


@app.post("/risk")
def risk(req: RiskRequest) -> Dict:
    try:
        fires = fetch_fires(bbox=req.bbox, days=req.firms_days, source=req.fire_source)
        assets = fetch_assets(bbox=req.bbox)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Data ingestion error: {exc}")

    west, south, east, north = [float(v) for v in req.bbox.split(",")]
    center_lat = (south + north) / 2.0
    center_lon = (west + east) / 2.0

    try:
        weather = fetch_weather_summary(lat=center_lat, lon=center_lon, hours=req.horizon_hours)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Weather fetch error: {exc}")

    scored_assets: List[Dict] = []
    risks_by_asset_id: Dict[str, Dict] = {}

    for asset in assets:
        risk_result = compute_asset_risk(asset=asset, fires=fires, weather=weather)
        merged = {**asset, **risk_result}
        scored_assets.append(merged)
        risks_by_asset_id[asset["id"]] = risk_result

    cascade = compute_cascade_impacts(
        assets=assets,
        risks_by_asset_id=risks_by_asset_id,
        fires=fires,
    )

    return {
        "bbox": req.bbox,
        "horizon_hours": req.horizon_hours,
        "weather": weather,
        "fire_count": len(fires),
        "asset_count": len(assets),
        "assets": scored_assets,
        "cascade": cascade,
    }


@app.get("/scenario/camp-fire-2018")
def scenario_camp_fire() -> Dict:
    path = SCENARIO_DIR / "camp-fire-2018.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Scenario file not found")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
