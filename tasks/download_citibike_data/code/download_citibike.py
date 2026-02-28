"""Download and filter NYC Citi Bike trip data for January 2024.

Filters to weekday, peak-hour, annual-member trips with valid station coordinates.
Samples down to SAMPLE_SIZE to keep the downstream routing step manageable.
"""

import io
import os
import zipfile

import pandas as pd
import requests

OUTPUT_DIR = "../output"

CITIBIKE_URL = (
    "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip"
)

# Commute-hour windows (24h clock, inclusive lower / exclusive upper)
MORNING_START, MORNING_END = 7, 9    # 07:00–09:00
EVENING_START, EVENING_END = 17, 19  # 17:00–19:00

SAMPLE_SIZE = 15_000
RANDOM_SEED = 42

KEEP_COLS = [
    "ride_id", "started_at", "ended_at",
    "start_station_id", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng",
]


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Downloading {CITIBIKE_URL} ...")
    resp = requests.get(CITIBIKE_URL, timeout=300, stream=True)
    resp.raise_for_status()

    raw = b"".join(resp.iter_content(chunk_size=1 << 20))
    print(f"Downloaded {len(raw) / 1e6:.1f} MB")

    print("Extracting and parsing CSV in chunks...")
    chunks = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        for csv_name in csv_names:
            with zf.open(csv_name) as f:
                for chunk in pd.read_csv(f, chunksize=200_000, low_memory=False):
                    # Parse timestamps
                    chunk["started_at"] = pd.to_datetime(
                        chunk["started_at"], errors="coerce"
                    )
                    chunk["ended_at"] = pd.to_datetime(
                        chunk["ended_at"], errors="coerce"
                    )
                    chunk["duration_sec"] = (
                        chunk["ended_at"] - chunk["started_at"]
                    ).dt.total_seconds()

                    # Members only
                    chunk = chunk[chunk["member_casual"] == "member"]

                    # Valid coordinates and station IDs
                    chunk = chunk.dropna(
                        subset=[
                            "start_lat", "start_lng", "end_lat", "end_lng",
                            "start_station_id", "end_station_id",
                        ]
                    )

                    # Reasonable duration
                    chunk = chunk[chunk["duration_sec"].between(120, 3600)]

                    # Weekday peak hours
                    dow = chunk["started_at"].dt.dayofweek
                    hour = chunk["started_at"].dt.hour
                    peak = (
                        ((hour >= MORNING_START) & (hour < MORNING_END))
                        | ((hour >= EVENING_START) & (hour < EVENING_END))
                    )
                    chunk = chunk[(dow < 5) & peak]

                    # Drop same-station trips
                    chunk = chunk[
                        chunk["start_station_id"] != chunk["end_station_id"]
                    ]

                    if len(chunk) > 0:
                        chunks.append(chunk[KEEP_COLS + ["duration_sec"]])

    if not chunks:
        raise RuntimeError("No trips passed filters — check the data URL or format")

    df = pd.concat(chunks, ignore_index=True)
    print(f"After commute-hour filter: {len(df):,} trips")
    print(f"Unique station pairs: {df.groupby(['start_station_id','end_station_id']).ngroups:,}")

    # Coerce station IDs to string (some chunks may have float NaN mixed in)
    df["start_station_id"] = df["start_station_id"].astype(str)
    df["end_station_id"] = df["end_station_id"].astype(str)

    if len(df) > SAMPLE_SIZE:
        df = df.sample(n=SAMPLE_SIZE, random_state=RANDOM_SEED)
    df = df.reset_index(drop=True)

    out_path = f"{OUTPUT_DIR}/citibike_trips.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df):,} trips to {out_path}")


if __name__ == "__main__":
    main()
