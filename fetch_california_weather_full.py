"""
Download California weather grid from Open-Meteo archive.
Defaults:
- Years: 2017-2024
- Grid spacing: 0.5 degrees
- Output: data/california_weather_grid_2017_2024.csv
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download California weather grid.")
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--step-deg", type=float, default=0.5, help="Grid step in degrees.")
    parser.add_argument("--sleep-sec", type=float, default=0.5, help="Delay between API calls.")
    parser.add_argument(
        "--flush-every",
        type=int,
        default=100,
        help="Write buffered rows to disk every N requests.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/california_weather_grid_2017_2024.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--backup",
        type=str,
        default="data/weather_grid_BACKUP.csv",
        help="Backup CSV path.",
    )
    return parser.parse_args()


def build_grid(step_deg: float) -> list[dict[str, float]]:
    west, east = -124.5, -114.0
    south, north = 32.5, 42.0
    lats = np.arange(south, north + step_deg, step_deg)
    lons = np.arange(west, east + step_deg, step_deg)
    return [{"lat": round(lat, 4), "lon": round(lon, 4)} for lat in lats for lon in lons]


def fetch_weather(lat: float, lon: float, year: int) -> pd.DataFrame | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "wind_direction_10m",
        ],
        "timezone": "UTC",
    }
    try:
        response = requests.get(ARCHIVE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        hourly = data["hourly"]
        df = pd.DataFrame(
            {
                "datetime": pd.to_datetime(hourly["time"]),
                "temp_c": hourly["temperature_2m"],
                "humidity": hourly["relative_humidity_2m"],
                "wind_speed_kmh": hourly["wind_speed_10m"],
                "wind_direction": hourly["wind_direction_10m"],
            }
        )
        df = df.set_index("datetime").resample("12h").mean().reset_index()
        df["grid_lat"] = lat
        df["grid_lon"] = lon
        return df
    except requests.exceptions.RequestException as exc:
        print(f"  ERROR request at ({lat}, {lon}) year {year}: {exc}")
        return None
    except Exception as exc:
        print(f"  ERROR parse at ({lat}, {lon}) year {year}: {exc}")
        return None


def append_frames(frames: list[pd.DataFrame], path: Path, write_header: bool) -> bool:
    if not frames:
        return write_header
    chunk = pd.concat(frames, ignore_index=True)
    chunk.to_csv(path, mode="w" if write_header else "a", header=write_header, index=False)
    return False


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    backup_path = Path(args.backup)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    if args.end_year < args.start_year:
        raise ValueError("--end-year must be >= --start-year")
    if args.step_deg <= 0:
        raise ValueError("--step-deg must be > 0")

    years = list(range(args.start_year, args.end_year + 1))
    grid_points = build_grid(args.step_deg)
    total_requests = len(grid_points) * len(years)

    print("=" * 70)
    print("CALIFORNIA WEATHER GRID DOWNLOAD")
    print("=" * 70)
    print(f"Grid points: {len(grid_points)}")
    print(f"Years: {years[0]}-{years[-1]} ({len(years)} years)")
    print(f"Total API calls: {total_requests}")
    print(f"Estimated time: {(total_requests * max(args.sleep_sec, 0.0)) / 60:.0f} min + API time")
    print(f"Output: {output_path}")
    print()

    if output_path.exists():
        output_path.unlink()
    if backup_path.exists():
        backup_path.unlink()

    buffered: list[pd.DataFrame] = []
    output_header = True
    backup_header = True
    completed = 0
    failed = 0
    saved_rows = 0
    start_time = datetime.now()

    for point in grid_points:
        lat, lon = point["lat"], point["lon"]
        for year in years:
            completed += 1
            df = fetch_weather(lat, lon, year)
            if df is None:
                failed += 1
            else:
                buffered.append(df)

            if completed % 20 == 0:
                elapsed = max((datetime.now() - start_time).total_seconds(), 1e-6)
                rate = completed / elapsed
                remaining_min = (total_requests - completed) / max(rate, 1e-9) / 60.0
                print(
                    f"[{completed}/{total_requests}] {completed/total_requests*100:.1f}% | "
                    f"ETA: {remaining_min:.0f} min | Failed: {failed}"
                )

            if completed % args.flush_every == 0:
                if buffered:
                    output_header = append_frames(buffered, output_path, output_header)
                    saved_rows += sum(len(x) for x in buffered)
                    backup_header = append_frames(buffered, backup_path, backup_header)
                    buffered = []
                    print(f"  Saved progress rows: {saved_rows:,}")

            if args.sleep_sec > 0:
                time.sleep(args.sleep_sec)

    if buffered:
        output_header = append_frames(buffered, output_path, output_header)
        backup_header = append_frames(buffered, backup_path, backup_header)
        saved_rows += sum(len(x) for x in buffered)

    total_minutes = (datetime.now() - start_time).total_seconds() / 60.0
    print("\n" + "=" * 70)
    if saved_rows > 0:
        print("SUCCESS")
        print(f"Saved rows: {saved_rows:,}")
        print(f"Failed requests: {failed}/{total_requests}")
        print(f"Final output: {output_path}")
    else:
        print("NO DATA SAVED")
        print("Check network/API availability.")
    print(f"Total time: {total_minutes:.1f} minutes")
    print("=" * 70)


if __name__ == "__main__":
    main()
