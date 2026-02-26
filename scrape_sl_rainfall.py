#!/usr/bin/env python3
# ==============================================================================
# GitHub Actions Script: Sri Lanka Irrigation Rainfall Data Scraper (Station CSVs)
# ==============================================================================
import os
import re
import sys
import json
import shutil
import argparse
from datetime import timedelta

import requests
import pandas as pd

API_URL_DEFAULT = (
    "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/"
    "gauges_2_view/FeatureServer/0/query"
)

def safe_filename(name: str) -> str:
    name = str(name).strip()
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "unknown"

def fetch_all_records(url: str, record_count: int = 2000, timeout: int = 60) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "orderByFields": "OBJECTID ASC",
        "resultOffset": 0,
        "resultRecordCount": record_count,
    }

    all_data: list[dict] = []

    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP Error {r.status_code}: {r.text[:300]}")

        data = r.json()
        if "error" in data:
            raise RuntimeError(f"API Error: {json.dumps(data['error'], ensure_ascii=False)}")

        features = data.get("features", [])
        if not features:
            break

        all_data.extend([f.get("attributes", {}) for f in features])

        if data.get("exceededTransferLimit"):
            params["resultOffset"] += params["resultRecordCount"]
            print(f"⬇️ Downloaded {len(all_data)} records... fetching more...")
        else:
            break

    return all_data

def clean_dataframe(df: pd.DataFrame, tz_offset: timedelta) -> pd.DataFrame:
    # Rename
    if "CreationDate" in df.columns:
        df = df.rename(columns={"CreationDate": "Observation_Time"})

    # Drop noise columns (keep this conservative)
    cols_to_drop = ["globalid", "Creator", "EditDate", "Editor"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # Convert any date/time columns (epoch ms) and apply SL offset
    for col in df.columns:
        if "time" in col.lower() or "date" in col.lower():
            try:
                s = pd.to_datetime(df[col], unit="ms", errors="raise")
                s = s + tz_offset
                df[col] = s.dt.tz_localize(None)
                print(f"🕒 Converted column '{col}' to Sri Lanka Time (UTC+5:30)")
            except Exception:
                # If it's not epoch-ms, ignore silently
                pass

    return df

def write_station_csvs(df: pd.DataFrame, group_col: str, out_dir: str) -> tuple[int, str]:
    os.makedirs(out_dir, exist_ok=True)

    if group_col not in df.columns:
        raise KeyError(f"Column '{group_col}' not found. Available columns: {list(df.columns)}")

    unique_stations = df[group_col].dropna().unique().tolist()
    print(f"📂 Grouping into {len(unique_stations)} station CSVs by '{group_col}'...")

    for station in unique_stations:
        station_df = df[df[group_col] == station]
        fname = safe_filename(station) + ".csv"
        station_df.to_csv(os.path.join(out_dir, fname), index=False)

    zip_base = os.path.join(os.path.dirname(out_dir) or ".", "SL_Station_Rainfall_Data")
    zip_path = shutil.make_archive(zip_base, "zip", out_dir)
    return len(unique_stations), zip_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.environ.get("API_URL", API_URL_DEFAULT))
    ap.add_argument("--group-col", default=os.environ.get("GROUP_COL", "gauge"))
    ap.add_argument("--out-dir", default=os.environ.get("OUT_DIR", "out/Station_Rainfall_CSVs"))
    ap.add_argument("--record-count", type=int, default=int(os.environ.get("RECORD_COUNT", "2000")))
    ap.add_argument("--timeout", type=int, default=int(os.environ.get("HTTP_TIMEOUT", "60")))
    args = ap.parse_args()

    print("🚀 Connecting to SL Irrigation Database...")
    all_data = fetch_all_records(args.url, record_count=args.record_count, timeout=args.timeout)

    if not all_data:
        print("No data found.")
        return 0

    df = pd.DataFrame(all_data)
    df = clean_dataframe(df, tz_offset=timedelta(hours=5, minutes=30))

    print("\n📊 Cleaned Data Preview:")
    print(df.head(5).to_string(index=False))

    nstations, zip_path = write_station_csvs(df, args.group_col, args.out_dir)

    print(f"\n✅ Created {nstations} station CSV files in: {args.out_dir}")
    print(f"📦 Zipped output: {zip_path}")

    # Optional: GitHub Actions step summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("## Sri Lanka Rainfall Scrape\n")
            f.write(f"- Total records: {len(df)}\n")
            f.write(f"- Stations: {nstations}\n")
            f.write(f"- Output folder: `{args.out_dir}`\n")
            f.write(f"- Zip: `{zip_path}`\n")

    return 0

if __name__ == "__main__":
    sys.exit(main())
