from __future__ import annotations

import requests
import streamlit as st

st.set_page_config(page_title="Wildfire Cascade", layout="wide")
st.title("Wildfire Cascade MVP")

api_base = st.sidebar.text_input("API Base", "http://127.0.0.1:8000")
bbox = st.sidebar.text_input("BBox", "-122.6,37.0,-121.8,38.0")
horizon = st.sidebar.selectbox("Horizon (hours)", [24, 48], index=0)

if st.sidebar.button("Run Risk"):
    payload = {"bbox": bbox, "horizon_hours": horizon, "firms_days": 1}
    resp = requests.post(f"{api_base}/risk", json=payload, timeout=45)
    if resp.ok:
        result = resp.json()
        st.subheader("Summary")
        st.json(
            {
                "fire_count": result.get("fire_count"),
                "asset_count": result.get("asset_count"),
                "weather": result.get("weather"),
            }
        )
        st.subheader("Top Risk Assets")
        top = sorted(result.get("assets", []), key=lambda a: a.get("risk_score", 0), reverse=True)[:20]
        st.dataframe(
            [
                {
                    "id": a.get("id"),
                    "type": a.get("asset_type"),
                    "name": a.get("name"),
                    "risk_score": a.get("risk_score"),
                    "risk_bucket": a.get("risk_bucket"),
                }
                for a in top
            ],
            use_container_width=True,
        )
        st.subheader("Cascade")
        st.json(result.get("cascade", {}))
    else:
        st.error(f"Request failed: {resp.status_code} - {resp.text}")
