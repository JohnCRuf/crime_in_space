"""Query the OSRM route API to get estimated driving durations for sampled HVFHV trips.

We deduplicate (PULocationID, DOLocationID) pairs from the sample and query the OSRM
/route endpoint once per unique pair, caching results to avoid redundant API calls.
The public server table endpoint is limited to ~10 coords; the route endpoint is reliable.
"""

import os
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import requests

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"

# Public OSRM API — free, no key required
OSRM_ROUTE_BASE = "http://router.project-osrm.org/route/v1/driving"

SAMPLE_SIZE = 10_000
RANDOM_SEED = 42


def load_zone_coords(geo_path: str) -> dict:
    """Return dict {locationid: (lon, lat)} from taxi zone polygon centroids."""
    gdf = gpd.read_file(geo_path).to_crs("EPSG:4326")
    id_col = next(
        c for c in gdf.columns
        if c.lower().replace("_", "") in ("locationid", "locationi")
    )
    gdf = gdf.rename(columns={id_col: "locationid"})
    gdf["locationid"] = pd.to_numeric(gdf["locationid"], errors="coerce").astype("Int64")
    centroids = gdf.to_crs("EPSG:32618").geometry.centroid.to_crs("EPSG:4326")
    gdf["lon"] = centroids.x
    gdf["lat"] = centroids.y
    gdf = gdf[["locationid", "lon", "lat"]].dropna()
    return {int(row.locationid): (row.lon, row.lat) for row in gdf.itertuples()}


def query_durations(pairs: list, zone_coords: dict) -> dict:
    """
    Query OSRM /route for each unique (PU, DO) pair.
    Returns dict {(pu_id, do_id): duration_seconds}.
    """
    unique_pairs = [
        (pu, do) for pu, do in set(pairs)
        if pu in zone_coords and do in zone_coords
    ]
    n = len(unique_pairs)
    print(f"  Querying {n} unique (PU, DO) pairs via OSRM route endpoint...")

    results = {}
    for i, (pu, do) in enumerate(unique_pairs):
        pu_lon, pu_lat = zone_coords[pu]
        do_lon, do_lat = zone_coords[do]
        url = (
            f"{OSRM_ROUTE_BASE}/{pu_lon:.6f},{pu_lat:.6f};{do_lon:.6f},{do_lat:.6f}"
            "?overview=false"
        )
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") == "Ok" and data.get("routes"):
                    results[(pu, do)] = data["routes"][0]["duration"]
                    break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  Failed ({pu},{do}): {exc}")

        if (i + 1) % 500 == 0:
            print(f"  {i + 1}/{n} pairs done")
        time.sleep(0.1)  # polite pause

    return results


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load and filter trips
    trips = pd.read_parquet(
        f"{INPUT_DIR}/hvfhv_trips.parquet",
        columns=["pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"],
    ).dropna()

    trips["actual_duration"] = (
        pd.to_datetime(trips["dropoff_datetime"]) - pd.to_datetime(trips["pickup_datetime"])
    ).dt.total_seconds()

    trips = trips[
        (trips["actual_duration"] > 60)
        & (trips["actual_duration"] < 10_800)
        & (trips["PULocationID"] != trips["DOLocationID"])
    ]
    trips = trips.sample(n=min(SAMPLE_SIZE, len(trips)), random_state=RANDOM_SEED).copy()
    trips["PULocationID"] = trips["PULocationID"].astype(int)
    trips["DOLocationID"] = trips["DOLocationID"].astype(int)

    print(f"Sampled {len(trips):,} trips")
    pairs = list(zip(trips["PULocationID"], trips["DOLocationID"]))
    n_unique = len(set(pairs))
    print(f"Unique (PU, DO) pairs: {n_unique}")

    print("Loading zone centroids...")
    zone_coords = load_zone_coords(f"{INPUT_DIR}/taxi_zones.geojson")

    duration_cache = query_durations(pairs, zone_coords)

    trips["estimated_duration"] = [
        duration_cache.get((row.PULocationID, row.DOLocationID), np.nan)
        for row in trips.itertuples()
    ]
    trips = trips.dropna(subset=["estimated_duration"]).copy()
    trips["time_diff"] = trips["actual_duration"] - trips["estimated_duration"]

    out_path = f"{OUTPUT_DIR}/trips_with_routes.parquet"
    trips.to_parquet(out_path, index=False)
    print(f"\nSaved {len(trips):,} trips with route estimates to {out_path}")


if __name__ == "__main__":
    main()
