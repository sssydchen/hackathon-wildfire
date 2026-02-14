# wildfire-cascade

FastAPI MVP for wildfire-to-infrastructure risk and cascade impact estimation.

## Implemented

- `GET /fires?bbox=...` (NASA FIRMS, requires `FIRMS_API_KEY`)
- `GET /assets?bbox=...` (OSM Overpass assets)
- `GET /assets/overpass_query?bbox=...` (ready-to-run Overpass query)
- `POST /risk` (distance + wind alignment + sigmoid risk model + cascade cards)
- `GET /scenario/camp-fire-2018` (cached replay payload)

## Repo structure

- `/Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire/backend/main.py`
- `/Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire/backend/ingest/`
- `/Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire/backend/features/`
- `/Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire/backend/model/`
- `/Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire/backend/cascade/`

## Quick start

```bash
cd /Users/yifanchen/Documents/DocumentsMB/NortheasternU/Hackathon/Hackathon_wildfire
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.main:app --reload
```

Then open http://127.0.0.1:8000/docs.

## Example request

```bash
curl -X POST http://127.0.0.1:8000/risk \
  -H 'Content-Type: application/json' \
  -d '{
    "bbox": "-122.6,37.0,-121.8,38.0",
    "horizon_hours": 24,
    "firms_days": 1,
    "fire_source": "VIIRS_NOAA20_NRT"
  }'
```

## Notes

- If `FIRMS_API_KEY` is missing, `/fires` and `/risk` return with zero fire points.
- `backend/data_cache/` caches upstream API responses to improve demo reliability.
