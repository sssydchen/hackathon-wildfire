from __future__ import annotations

from typing import Dict, List, Tuple

import folium
import pandas as pd
import requests
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium

USA_BBOX = "-125.0,24.0,-66.5,49.5"


st.set_page_config(page_title="Wildfire Cascade", layout="wide")
st.title("US Wildfire Infrastructure Risk")
st.caption("Live FIRMS fire detections + wind-aware infrastructure risk + cascade impacts")

api_base = st.sidebar.text_input("API Base", "http://127.0.0.1:8000")
fire_source = st.sidebar.selectbox("FIRMS Source", ["VIIRS_NOAA20_NRT", "VIIRS_SNPP_NRT"], index=0)
firms_days = st.sidebar.selectbox("Fire lookback days", [1, 2, 3], index=0)
horizon = st.sidebar.selectbox("Risk horizon (hours)", [24, 48], index=0)
weather_source = st.sidebar.selectbox("Weather Source", ["gridmet", "openmeteo"], index=0)
min_fire_confidence = st.sidebar.slider("Fire confidence threshold", 0, 100, 60)
min_asset_risk = st.sidebar.slider("Asset risk threshold", 0.0, 1.0, 0.4, 0.05)
auto_analyze = st.sidebar.checkbox("Auto analyze on zoom", value=True)


if "risk_result" not in st.session_state:
    st.session_state["risk_result"] = None
if "risk_bbox" not in st.session_state:
    st.session_state["risk_bbox"] = None


def _bbox_from_bounds(bounds: Dict) -> str:
    sw = bounds.get("_southWest", {})
    ne = bounds.get("_northEast", {})
    west = sw.get("lng")
    south = sw.get("lat")
    east = ne.get("lng")
    north = ne.get("lat")
    return f"{west},{south},{east},{north}"


def _fetch_fires(bbox: str) -> List[Dict]:
    resp = requests.get(
        f"{api_base}/fires",
        params={
            "bbox": bbox,
            "days": firms_days,
            "source": fire_source,
            "min_confidence": min_fire_confidence,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("fires", [])


def _run_risk_analysis(bbox: str) -> Dict:
    payload = {
        "bbox": bbox,
        "horizon_hours": horizon,
        "firms_days": firms_days,
        "fire_source": fire_source,
        "fire_confidence_threshold": float(min_fire_confidence),
        "weather_source": weather_source,
    }
    resp = requests.post(f"{api_base}/risk", json=payload, timeout=90)
    resp.raise_for_status()
    return resp.json()


def _risk_color(score: float) -> str:
    if score >= 0.75:
        return "red"
    if score >= 0.4:
        return "orange"
    return "green"


def _build_national_map(fires: List[Dict]) -> folium.Map:
    fmap = folium.Map(location=[39.5, -98.35], zoom_start=4, tiles="CartoDB positron")

    cluster = MarkerCluster(name="Wildfires").add_to(fmap)
    for fire in fires:
        confidence = fire.get("confidence_score")
        confidence_text = "n/a" if confidence is None else f"{confidence:.1f}"
        folium.CircleMarker(
            location=[fire["lat"], fire["lon"]],
            radius=4,
            color="crimson",
            weight=1,
            fill=True,
            fill_opacity=0.75,
            popup=(
                f"Fire ID: {fire.get('id')}<br>"
                f"Confidence: {confidence_text}<br>"
                f"Date: {fire.get('acq_date')} {fire.get('acq_time')}"
            ),
        ).add_to(cluster)

    folium.LayerControl().add_to(fmap)
    return fmap


def _build_detail_map(result: Dict) -> folium.Map:
    west, south, east, north = [float(v) for v in result["bbox"].split(",")]
    center = [(south + north) / 2.0, (west + east) / 2.0]
    fmap = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")

    for fire in _safe_list(result.get("fires")):
        folium.CircleMarker(
            location=[fire["lat"], fire["lon"]],
            radius=5,
            color="crimson",
            fill=True,
            fill_opacity=0.8,
        ).add_to(fmap)

    for asset in result.get("assets", []):
        score = float(asset.get("risk_score", 0.0))
        folium.CircleMarker(
            location=[asset["lat"], asset["lon"]],
            radius=5,
            color=_risk_color(score),
            fill=True,
            fill_opacity=0.85,
            popup=(
                f"{asset.get('name')} ({asset.get('asset_type')})<br>"
                f"Risk: {score:.3f} ({asset.get('risk_bucket')})"
            ),
        ).add_to(fmap)

    return fmap


def _safe_list(value) -> List[Dict]:
    if isinstance(value, list):
        return value
    return []


# Nationwide fires map
try:
    usa_fires = _fetch_fires(USA_BBOX)
except Exception as exc:
    st.error(f"Failed to load nationwide fire data: {exc}")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("US fire detections", len(usa_fires))
col2.metric("Min fire confidence", min_fire_confidence)
col3.metric("Risk horizon (hours)", horizon)
col4.metric("Weather source", weather_source)

st.subheader("Nationwide Wildfire Map")
fires_map = _build_national_map(usa_fires)
map_state = st_folium(
    fires_map,
    use_container_width=True,
    height=620,
    returned_objects=["bounds", "zoom"],
    key="national_fires_map",
)

current_zoom = map_state.get("zoom") if map_state else None
current_bounds = map_state.get("bounds") if map_state else None

analysis_bbox = None
if current_bounds:
    analysis_bbox = _bbox_from_bounds(current_bounds)

st.caption("Zoom to a fire region (>=7) to analyze nearby infrastructure risk.")

run_analysis = False
if analysis_bbox and current_zoom and current_zoom >= 7:
    run_analysis = st.button("Analyze Current Map View")
    if auto_analyze:
        run_analysis = True
else:
    st.info("Zoom in further (level 7 or more) to analyze local infrastructure impacts.")

if run_analysis and analysis_bbox:
    try:
        risk_result = _run_risk_analysis(analysis_bbox)
        risk_result["fires"] = _fetch_fires(analysis_bbox)
        st.session_state["risk_result"] = risk_result
        st.session_state["risk_bbox"] = analysis_bbox
    except Exception as exc:
        st.error(f"Risk analysis failed: {exc}")

result = st.session_state.get("risk_result")
if result:
    st.subheader("Zoomed-In Risk Analysis")

    summary_cols = st.columns(4)
    summary_cols[0].metric("Fires in view", result.get("fire_count", 0))
    summary_cols[1].metric("Assets in view", result.get("asset_count", 0))
    summary_cols[2].metric("Weather wind km/h", round(result.get("weather", {}).get("wind_speed_kmh", 0), 2))
    summary_cols[3].metric("Humidity %", round(result.get("weather", {}).get("humidity_pct", 0), 2))

    filtered_assets = [
        a for a in result.get("assets", []) if float(a.get("risk_score", 0.0)) >= min_asset_risk
    ]
    top_assets = sorted(filtered_assets, key=lambda a: float(a.get("risk_score", 0.0)), reverse=True)[:25]

    st.markdown("### Top Risk Assets")
    if top_assets:
        df = pd.DataFrame(
            [
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "asset_type": a.get("asset_type"),
                    "risk_score": round(float(a.get("risk_score", 0.0)), 4),
                    "risk_bucket": a.get("risk_bucket"),
                    "min_dist_to_fire_km": a.get("features", {}).get("min_dist_to_fire_km"),
                    "wind_alignment": a.get("features", {}).get("wind_alignment"),
                }
                for a in top_assets
            ]
        )
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No assets above selected risk threshold in current view.")

    st.markdown("### Cascade Impact Cards")
    st.json(result.get("cascade", {}))

    st.markdown("### Infrastructure + Fire Overlay (Current View)")
    detail_map = _build_detail_map(result)
    st_folium(detail_map, use_container_width=True, height=520, key="detail_map")
