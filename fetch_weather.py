from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import xarray as xr

VS_URL = "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_vs_1979_CurrentYear_CONUS.nc"
TH_URL = "https://thredds.northwestknowledge.net/thredds/dodsC/agg_met_th_1979_CurrentYear_CONUS.nc"

LAT_MIN, LAT_MAX = 32.5, 42.0
LON_MIN_NEG, LON_MAX_NEG = -124.5, -114.0
LON_MIN_360, LON_MAX_360 = LON_MIN_NEG + 360.0, LON_MAX_NEG + 360.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export CA GridMET wind data year-by-year.")
    p.add_argument("--start-year", type=int, default=2017)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--output", type=str, default="data/gridmet_ca_wind_2017_2024.csv")
    return p.parse_args()


def ordered_slice(first: float, last: float, low: float, high: float) -> slice:
    return slice(low, high) if first <= last else slice(high, low)


def open_and_prepare(url: str):
    ds = xr.open_dataset(url)
    time_coord = "day" if "day" in ds.coords else ("time" if "time" in ds.coords else None)
    if time_coord is None:
        raise RuntimeError(f"No day/time coordinate in dataset: {list(ds.coords)}")

    lon_vals = ds["lon"].values
    lat_vals = ds["lat"].values
    uses_360 = float(lon_vals.max()) > 180.0
    lon_min = LON_MIN_360 if uses_360 else LON_MIN_NEG
    lon_max = LON_MAX_360 if uses_360 else LON_MAX_NEG

    lat_slice = ordered_slice(float(lat_vals[0]), float(lat_vals[-1]), LAT_MIN, LAT_MAX)
    lon_slice = ordered_slice(float(lon_vals[0]), float(lon_vals[-1]), lon_min, lon_max)
    return ds, time_coord, lat_slice, lon_slice


def pick_var_name(ds, expected: str) -> str:
    vars_ = list(ds.data_vars)
    if expected in vars_:
        return expected
    if not vars_:
        raise RuntimeError("Dataset has no data variables.")
    return vars_[0]


def load_year_df(
    ds,
    time_coord: str,
    lat_slice: slice,
    lon_slice: slice,
    expected_name: str,
    year: int,
) -> pd.DataFrame:
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    subset = ds.sel(
        **{
            time_coord: slice(start_date, end_date),
            "lat": lat_slice,
            "lon": lon_slice,
        }
    )
    var_name = pick_var_name(subset, expected_name)
    if var_name != expected_name:
        print(f"  Using variable '{var_name}' (expected '{expected_name}') for {year}")

    df = subset[[var_name]].to_dataframe().reset_index()
    df = df.rename(columns={time_coord: "date", var_name: expected_name})
    if df["lon"].max() > 180:
        df["lon"] = df["lon"] - 360.0
    return df[["date", "lat", "lon", expected_name]]


def main() -> None:
    args = parse_args()
    if args.end_year < args.start_year:
        raise ValueError("--end-year must be >= --start-year")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    print("=" * 70)
    print("GRIDMET CA WIND EXPORT (YEAR-BY-YEAR)")
    print("=" * 70)
    print(f"Years: {args.start_year}-{args.end_year}")
    print(f"Output: {out_path}")
    print()

    print("Opening datasets...")
    vs_ds, vs_time, vs_lat_slice, vs_lon_slice = open_and_prepare(VS_URL)
    th_ds, th_time, th_lat_slice, th_lon_slice = open_and_prepare(TH_URL)
    print("Datasets ready.")
    print()

    wrote_header = False
    total_rows = 0
    total_years = args.end_year - args.start_year + 1

    for idx, year in enumerate(range(args.start_year, args.end_year + 1), start=1):
        print(f"[{idx}/{total_years}] Processing year {year}...")
        try:
            vs_df = load_year_df(vs_ds, vs_time, vs_lat_slice, vs_lon_slice, "vs", year)
            th_df = load_year_df(th_ds, th_time, th_lat_slice, th_lon_slice, "th", year)
            print(f"  Rows vs={len(vs_df):,}, th={len(th_df):,}")

            merged = vs_df.merge(th_df, on=["date", "lat", "lon"], how="inner")
            merged = merged.rename(columns={"vs": "wind_speed_ms", "th": "wind_direction_deg"})
            merged["wind_speed_kmh"] = merged["wind_speed_ms"] * 3.6
            merged["year"] = year

            mode = "w" if not wrote_header else "a"
            merged.to_csv(out_path, mode=mode, header=not wrote_header, index=False)
            wrote_header = True
            total_rows += len(merged)
            print(f"  Wrote {len(merged):,} rows (total {total_rows:,})")
        except Exception as exc:
            print(f"  ERROR year {year}: {exc}")

    print("\n" + "=" * 70)
    if total_rows > 0:
        print("SUCCESS")
        print(f"Total rows: {total_rows:,}")
        print(f"Saved file: {out_path}")
    else:
        print("NO DATA WRITTEN")
        print("Check dataset connectivity/coordinates.")
    print("=" * 70)


if __name__ == "__main__":
    main()
