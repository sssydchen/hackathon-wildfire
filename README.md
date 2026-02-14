# wildfire-cascade

FastAPI + Streamlit MVP for wildfire-to-infrastructure risk and cascade impact estimation.

## Implemented

- `GET /fires?bbox=...&min_confidence=...` (NASA FIRMS, requires `FIRMS_API_KEY`)
- `GET /assets?bbox=...` (OSM Overpass assets)
- `GET /assets/overpass_query?bbox=...` (ready-to-run Overpass query)
- `POST /risk` (distance + wind alignment + sigmoid risk model + cascade cards)
- `GET /scenario/camp-fire-2018` (cached replay payload)
- `app/streamlit_app.py` (nationwide map + zoom-in analysis + top risk asset table)

## Quick start

```bash
cd /Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export FIRMS_API_KEY="your_actual_key"
uvicorn backend.main:app --reload
```

In a second terminal:

```bash
cd /Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire
source .venv/bin/activate
streamlit run app/streamlit_app.py
```

Then open:
- API docs: http://127.0.0.1:8000/docs
- App UI: http://localhost:8501

## Notes

- If `FIRMS_API_KEY` is missing, `/fires` and `/risk` return zero fire points.
- `backend/data_cache/` caches upstream API responses to improve demo reliability.
