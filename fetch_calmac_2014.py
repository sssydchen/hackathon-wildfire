from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote, unquote
import html

import requests

WEATHER_PAGE_URL = "https://www.calmac.org/weather.asp"
CALMAC_API_URL = "https://www.calmac.org/api.asp"
START_YEAR = 2017
END_YEAR = 2025
OUTPUT_PATH = Path("data/calmac.csv")


def extract_station_wmos_for_year(html: str, year: int) -> list[str]:
    # Parse every api.asp?... URL and read query params regardless ordering.
    # This is resilient to pages that reorder year/type/wmo parameters.
    links = re.findall(r"api\.asp\?[^\"'<>\\s]+", html, flags=re.IGNORECASE)
    wmos: set[str] = set()
    for link in links:
        q = unquote(html.unescape(link))
        if "data=weather" not in q.lower():
            continue
        qs = parse_qs(q.split("?", 1)[1], keep_blank_values=True)
        years = qs.get("year", [])
        if str(year) not in years:
            continue
        wmo_vals = qs.get("wmo", [])
        for wmo in wmo_vals:
            if wmo:
                wmos.add(wmo)
    return sorted(wmos)


def extract_all_station_wmos(html_text: str) -> list[str]:
    # Fallback: collect any wmo=... in page.
    raw = html.unescape(html_text)
    matches = re.findall(r"(?:[?&]|&amp;)wmo=([A-Za-z0-9]+)", raw, flags=re.IGNORECASE)
    return sorted(set(matches))


def fetch_station_epw(wmo: str, year: int) -> str:
    params = {
        "data": "weather",
        "wmo": wmo,
        "year": str(year),
        "type": "epw",
    }
    # Build URL explicitly so logs show what is requested.
    query = "&".join(f"{k}={quote(v)}" for k, v in params.items())
    url = f"{CALMAC_API_URL}?{query}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def is_valid_epw_text(payload: str) -> bool:
    # EPW starts with LOCATION header and has at least 9 CSV lines.
    lines = payload.splitlines()
    if len(lines) < 9:
        return False
    return lines[0].upper().startswith("LOCATION,")


def epw_rows_to_records(wmo: str, epw_text: str) -> Iterable[dict]:
    reader = csv.reader(io.StringIO(epw_text))
    rows = list(reader)
    if len(rows) < 9:
        return []

    location = rows[0]
    station_name = location[1].strip() if len(location) > 1 else ""
    state = location[2].strip() if len(location) > 2 else ""
    country = location[3].strip() if len(location) > 3 else ""
    lat = _to_float(location[6]) if len(location) > 6 else None
    lon = _to_float(location[7]) if len(location) > 7 else None

    out = []
    # EPW data starts after 8 header lines.
    for row in rows[8:]:
        if len(row) < 22:
            continue
        yr = _to_int(row[0])
        month = _to_int(row[1])
        day = _to_int(row[2])
        hour = _to_int(row[3])
        dry_bulb_c = _to_float(row[6])
        rel_humidity = _to_float(row[8])
        wind_dir_deg = _to_float(row[20])
        wind_speed_ms = _to_float(row[21])
        wind_speed_kmh = wind_speed_ms * 3.6 if wind_speed_ms is not None else None

        out.append(
            {
                "wmo": wmo,
                "station_name": station_name,
                "state": state,
                "country": country,
                "lat": lat,
                "lon": lon,
                "year": yr,
                "month": month,
                "day": day,
                "hour_epw": hour,
                "dry_bulb_c": dry_bulb_c,
                "relative_humidity_pct": rel_humidity,
                "wind_direction_deg": wind_dir_deg,
                "wind_speed_ms": wind_speed_ms,
                "wind_speed_kmh": wind_speed_kmh,
            }
        )
    return out


def aggregate_records_6h(records: list[dict]) -> list[dict]:
    # Aggregate hourly EPW rows into 6-hour windows:
    # hour_epw starts at 1, windows begin at 1/7/13/19.
    buckets: dict[tuple, dict] = {}
    for r in records:
        hour = r.get("hour_epw")
        if hour is None:
            continue
        block = (int(hour) - 1) // 6
        block_start_hour = block * 6 + 1
        key = (
            r.get("wmo"),
            r.get("year"),
            r.get("month"),
            r.get("day"),
            block_start_hour,
        )

        b = buckets.get(key)
        if b is None:
            b = {
                "wmo": r.get("wmo"),
                "station_name": r.get("station_name"),
                "state": r.get("state"),
                "country": r.get("country"),
                "lat": r.get("lat"),
                "lon": r.get("lon"),
                "year": r.get("year"),
                "month": r.get("month"),
                "day": r.get("day"),
                "hour_epw": block_start_hour,
                "dry_bulb_c_sum": 0.0,
                "dry_bulb_c_n": 0,
                "relative_humidity_pct_sum": 0.0,
                "relative_humidity_pct_n": 0,
                "wind_direction_deg_sum": 0.0,
                "wind_direction_deg_n": 0,
                "wind_speed_ms_sum": 0.0,
                "wind_speed_ms_n": 0,
                "wind_speed_kmh_sum": 0.0,
                "wind_speed_kmh_n": 0,
            }
            buckets[key] = b

        for field in [
            "dry_bulb_c",
            "relative_humidity_pct",
            "wind_direction_deg",
            "wind_speed_ms",
            "wind_speed_kmh",
        ]:
            val = r.get(field)
            if val is None:
                continue
            b[f"{field}_sum"] += float(val)
            b[f"{field}_n"] += 1

    def _avg(b: dict, field: str):
        n = b[f"{field}_n"]
        if n == 0:
            return None
        return b[f"{field}_sum"] / n

    out: list[dict] = []
    for _, b in sorted(buckets.items(), key=lambda x: x[0]):
        out.append(
            {
                "wmo": b["wmo"],
                "station_name": b["station_name"],
                "state": b["state"],
                "country": b["country"],
                "lat": b["lat"],
                "lon": b["lon"],
                "year": b["year"],
                "month": b["month"],
                "day": b["day"],
                "hour_epw": b["hour_epw"],
                "dry_bulb_c": _avg(b, "dry_bulb_c"),
                "relative_humidity_pct": _avg(b, "relative_humidity_pct"),
                "wind_direction_deg": _avg(b, "wind_direction_deg"),
                "wind_speed_ms": _avg(b, "wind_speed_ms"),
                "wind_speed_kmh": _avg(b, "wind_speed_kmh"),
            }
        )
    return out


def _to_float(value: str):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: str):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    print("=" * 70)
    print("CALMAC WEATHER EXPORT")
    print("=" * 70)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    print(f"Fetching station list from {WEATHER_PAGE_URL} ...")
    page = requests.get(WEATHER_PAGE_URL, timeout=60)
    page.raise_for_status()
    years = list(range(START_YEAR, END_YEAR + 1))
    print(f"Year range: {START_YEAR}-{END_YEAR}")
    print(f"Output file: {OUTPUT_PATH}")
    print()

    fieldnames = [
        "wmo",
        "station_name",
        "state",
        "country",
        "lat",
        "lon",
        "year",
        "month",
        "day",
        "hour_epw",
        "dry_bulb_c",
        "relative_humidity_pct",
        "wind_direction_deg",
        "wind_speed_ms",
        "wind_speed_kmh",
    ]

    total_rows = 0
    failed = 0
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for year in years:
            print(f"\n--- Year {year} ---")
            wmos = extract_station_wmos_for_year(page.text, year)
            if not wmos:
                wmos = extract_all_station_wmos(page.text)
                if not wmos:
                    raise RuntimeError("No CALMAC station WMOs found on weather page.")
                print(
                    f"No explicit {year} links found. Falling back to {len(wmos)} discovered stations."
                )
            else:
                print(f"Stations found for {year}: {len(wmos)}")

            total = len(wmos)
            for idx, wmo in enumerate(wmos, start=1):
                print(f"[{idx}/{total}] Downloading WMO {wmo} for {year} ...")
                try:
                    epw_text = fetch_station_epw(wmo=wmo, year=year)
                    if not is_valid_epw_text(epw_text):
                        print("  Skipping: no valid EPW payload for this station/year.")
                        failed += 1
                        continue
                    records = list(epw_rows_to_records(wmo=wmo, epw_text=epw_text))
                    records_6h = aggregate_records_6h(records)
                    writer.writerows(records_6h)
                    total_rows += len(records_6h)
                    print(
                        f"  Wrote {len(records_6h):,} rows at 6-hour granularity "
                        f"(from {len(records):,} hourly rows; total {total_rows:,})"
                    )
                except Exception as exc:
                    failed += 1
                    print(f"  ERROR WMO {wmo}: {exc}")

    print()
    print("=" * 70)
    print("DONE")
    print(f"Years attempted: {START_YEAR}-{END_YEAR}")
    print(f"Stations failed: {failed}")
    print(f"Rows written: {total_rows:,}")
    print(f"Saved: {OUTPUT_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()
