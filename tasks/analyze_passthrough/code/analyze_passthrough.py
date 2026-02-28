"""Passthrough analysis: do drivers detour when the optimal route meaningfully crosses
a high-crime zone?

Sample selection: trips whose OSRM-optimal route spends an estimated ≥ MIN_HC_TIME_SEC
seconds in high-crime intermediate zones (time estimated as intersection-length fraction
of total route length × OSRM duration). Compare pct_time_increase for this group
against trips whose optimal route passes through no high-crime zones at all.

No endpoint crime filter is applied so all 9k+ sampled trips are eligible; the treatment
condition is defined purely by in-transit exposure.
"""

import os
import time

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from scipy.stats import ttest_ind
from shapely.geometry import shape

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"

OSRM_ROUTE_BASE = "http://router.project-osrm.org/route/v1/driving"

# Intermediate zone threshold: top 30% is "high crime"
HIGH_CRIME_MIN_DECILE = 7

# Minimum estimated seconds the optimal route must spend in high-crime intermediate
# zones for a trip to enter the treatment group
MIN_HC_TIME_SEC = 600  # 10 minutes


def build_zone_crime_lookup(crime_csv: str, zones_geojson: str) -> pd.DataFrame:
    """Compute crime rate per sq km and decile for every taxi zone."""
    crime = pd.read_csv(crime_csv)
    crime["latitude"] = pd.to_numeric(crime["latitude"], errors="coerce")
    crime["longitude"] = pd.to_numeric(crime["longitude"], errors="coerce")
    crime = crime.dropna(subset=["latitude", "longitude"])

    zones_gdf = gpd.read_file(zones_geojson).to_crs("EPSG:4326")
    id_col = next(
        c for c in zones_gdf.columns
        if c.lower().replace("_", "") in ("locationid", "locationi")
    )
    zones_gdf = zones_gdf.rename(columns={id_col: "LocationID"})
    zones_gdf["LocationID"] = (
        pd.to_numeric(zones_gdf["LocationID"], errors="coerce").astype("Int64")
    )

    zones_proj = zones_gdf.to_crs("EPSG:32618").copy()
    zones_gdf["area_km2"] = zones_proj.geometry.area / 1e6

    crime_gdf = gpd.GeoDataFrame(
        crime,
        geometry=gpd.points_from_xy(crime["longitude"], crime["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(
        crime_gdf, zones_gdf[["LocationID", "geometry"]],
        how="left", predicate="within",
    )
    crime_counts = joined.groupby("LocationID").size().reset_index(name="crime_count")
    crime_counts["LocationID"] = crime_counts["LocationID"].astype(int)

    zone_meta = zones_gdf[["LocationID", "area_km2"]].copy()
    zone_meta["LocationID"] = zone_meta["LocationID"].astype(int)
    zone_crime = zone_meta.merge(crime_counts, on="LocationID", how="left")
    zone_crime["crime_count"] = zone_crime["crime_count"].fillna(0)
    zone_crime["crime_rate_km2"] = zone_crime["crime_count"] / zone_crime["area_km2"]
    zone_crime["zone_decile"] = (
        pd.qcut(zone_crime["crime_rate_km2"], q=10, labels=False, duplicates="drop") + 1
    )
    return zone_crime[["LocationID", "crime_rate_km2", "zone_decile"]].copy()


def load_zone_centroids(zones_geojson: str) -> dict:
    gdf = gpd.read_file(zones_geojson).to_crs("EPSG:4326")
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
    return {int(r.locationid): (r.lon, r.lat) for r in gdf.itertuples()}


def fetch_geometries(pairs: list, zone_coords: dict, cache_path: str = None,
                     n_workers: int = 4) -> dict:
    """Fetch OSRM full route geometry for each unique (PU, DO) pair.

    Uses a thread pool. Results cached to cache_path (pickle) to survive reruns.
    Cache stores geometry directly (not wrapped in a dict entry).
    """
    import pickle
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cached: dict = {}
    cache_lock = threading.Lock()

    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            cached = pickle.load(f)
        print(f"  Loaded {len(cached)} cached geometries from {cache_path}")

    unique = [
        (pu, do) for pu, do in set(pairs)
        if pu in zone_coords and do in zone_coords and (pu, do) not in cached
    ]
    n = len(unique)
    print(f"  Fetching route geometries for {n} pairs (cache has {len(cached)}) "
          f"using {n_workers} threads...")

    results = dict(cached)
    done_count = [0]
    count_lock = threading.Lock()

    def _fetch_one(pu_do):
        pu, do = pu_do
        pu_lon, pu_lat = zone_coords[pu]
        do_lon, do_lat = zone_coords[do]
        url = (
            f"{OSRM_ROUTE_BASE}/{pu_lon:.6f},{pu_lat:.6f};{do_lon:.6f},{do_lat:.6f}"
            "?overview=full&geometries=geojson"
        )
        geom = None
        for attempt in range(5):
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if data.get("code") == "Ok" and data.get("routes"):
                    geom = shape(data["routes"][0]["geometry"])
                    break
            except requests.exceptions.ConnectTimeout:
                time.sleep(30 * (attempt + 1))
            except Exception as exc:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                else:
                    pass  # silently skip
        if geom is not None:
            with cache_lock:
                results[pu_do] = geom
        return pu_do

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_fetch_one, pair): pair for pair in unique}
        for fut in as_completed(futures):
            fut.result()
            with count_lock:
                done_count[0] += 1
                done = done_count[0]
            if done % 500 == 0:
                print(f"  {done}/{n} done  ({len(results)} with route)")
                if cache_path:
                    with cache_lock:
                        snap = dict(results)
                    with open(cache_path, "wb") as f:
                        pickle.dump(snap, f)

    if cache_path and n > 0:
        with open(cache_path, "wb") as f:
            pickle.dump(results, f)
        print(f"  Saved {len(results)} geometries to {cache_path}")

    return results


def compute_hc_time(routes_gdf: gpd.GeoDataFrame,
                    zones_gdf: gpd.GeoDataFrame,
                    high_crime_ids: set,
                    est_dur: pd.Series) -> pd.Series:
    """Estimate seconds each trip's optimal route spends in high-crime intermediate zones.

    Uses length fraction of the route within each high-crime zone as a proxy for
    time fraction:  time_in_zone = (segment_length / total_length) × estimated_duration

    Returns a Series indexed like routes_gdf, values in seconds.
    """
    # Ensure trip_idx is a regular column (it may be the index after set_index)
    if routes_gdf.index.name == "trip_idx":
        routes_gdf = routes_gdf.reset_index()

    # Project to metric CRS for accurate length computation
    routes_proj = routes_gdf.to_crs("EPSG:32618")
    zones_proj = zones_gdf.to_crs("EPSG:32618")[
        zones_gdf["LocationID"].isin(high_crime_ids)
    ].copy()

    # Precompute route lengths
    route_lengths = routes_proj.geometry.length  # indexed by trip_idx via GeoDataFrame index

    # Spatial join: find candidate (route, high-crime zone) pairs
    joined = gpd.sjoin(
        routes_proj[["trip_idx", "PULocationID", "DOLocationID", "geometry"]],
        zones_proj[["LocationID", "geometry"]],
        how="inner",
        predicate="intersects",
    )
    # Drop repeated route geometry to free memory before the intersection loop
    joined = pd.DataFrame(joined.drop(columns=["geometry"]))
    # Drop endpoint zones
    joined = joined[
        (joined["LocationID"] != joined["PULocationID"]) &
        (joined["LocationID"] != joined["DOLocationID"])
    ]

    if joined.empty:
        return pd.Series(0.0, index=routes_gdf.index)

    # Build geometry lookups (projected)
    route_geom_dict = dict(zip(routes_proj["trip_idx"], routes_proj.geometry))
    route_len_dict = dict(zip(routes_proj["trip_idx"], route_lengths))
    zone_geom_dict = {
        int(r.LocationID): r.geometry
        for r in zones_proj.itertuples()
    }

    hc_time: dict[int, float] = {}

    for row in joined.itertuples():
        trip_idx = int(row.trip_idx)
        zone_id = int(row.LocationID)

        r_geom = route_geom_dict.get(trip_idx)
        z_geom = zone_geom_dict.get(zone_id)
        if r_geom is None or z_geom is None:
            continue

        seg = r_geom.intersection(z_geom)
        if seg.is_empty:
            continue

        total_len = route_len_dict.get(trip_idx, 0)
        if total_len == 0:
            continue

        duration = est_dur.get(trip_idx, 0)
        time_frac = seg.length / total_len
        hc_time[trip_idx] = hc_time.get(trip_idx, 0.0) + time_frac * duration

    return pd.Series(hc_time).reindex(routes_gdf.index, fill_value=0.0)


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Zone-level crime data
    print("Building zone crime lookup...")
    zone_crime = build_zone_crime_lookup(
        f"{INPUT_DIR}/nyc_crime.csv",
        f"{INPUT_DIR}/taxi_zones.geojson",
    )
    zone_decile_map = dict(zip(zone_crime["LocationID"].astype(int), zone_crime["zone_decile"]))

    # 2. Load all trips (no endpoint crime filter)
    trips = pd.read_parquet(f"{INPUT_DIR}/analysis_data.parquet")
    trips["PULocationID"] = trips["PULocationID"].astype(int)
    trips["DOLocationID"] = trips["DOLocationID"].astype(int)
    trips = trips.reset_index(drop=True)
    print(f"Total trips: {len(trips):,}")

    # 3. Fetch OSRM route geometries for all unique pairs
    print("Loading zone centroids...")
    zone_coords = load_zone_centroids(f"{INPUT_DIR}/taxi_zones.geojson")

    pairs = list(zip(trips["PULocationID"], trips["DOLocationID"]))
    print("Fetching OSRM route geometries...")
    geom_cache = fetch_geometries(
        pairs, zone_coords,
        cache_path=f"{OUTPUT_DIR}/geom_cache.pkl",
    )

    # 4. Build GeoDataFrame of routes
    route_rows = []
    for row in trips.itertuples():
        geom = geom_cache.get((row.PULocationID, row.DOLocationID))
        if geom is not None:
            route_rows.append({
                "trip_idx": row.Index,
                "PULocationID": row.PULocationID,
                "DOLocationID": row.DOLocationID,
                "geometry": geom,
            })

    routes_gdf = gpd.GeoDataFrame(route_rows, geometry="geometry", crs="EPSG:4326")
    routes_gdf = routes_gdf.set_index("trip_idx")

    # 5. Load zones with crime data for spatial intersection
    zones_gdf = gpd.read_file(f"{INPUT_DIR}/taxi_zones.geojson").to_crs("EPSG:4326")
    id_col = next(
        c for c in zones_gdf.columns
        if c.lower().replace("_", "") in ("locationid", "locationi")
    )
    zones_gdf = zones_gdf.rename(columns={id_col: "LocationID"})
    zones_gdf["LocationID"] = pd.to_numeric(zones_gdf["LocationID"], errors="coerce").astype(int)
    zones_gdf = zones_gdf.merge(
        zone_crime[["LocationID", "zone_decile"]], on="LocationID", how="left"
    )
    high_crime_ids = set(
        zones_gdf[zones_gdf["zone_decile"] >= HIGH_CRIME_MIN_DECILE]["LocationID"]
    )

    # 6. Compute estimated time in high-crime intermediate zones
    print("Computing estimated time in high-crime intermediate zones...")
    est_dur = trips["estimated_duration"]
    routes_gdf["hc_time_sec"] = compute_hc_time(
        routes_gdf, zones_gdf, high_crime_ids, est_dur
    )
    routes_gdf["hc_time_min"] = routes_gdf["hc_time_sec"] / 60

    # Merge back onto trips
    trips = trips.join(routes_gdf[["hc_time_sec", "hc_time_min"]], how="left")
    trips["hc_time_sec"] = trips["hc_time_sec"].fillna(0)
    trips["hc_time_min"] = trips["hc_time_min"].fillna(0)

    # 7. Sample selection
    #    Control:   route passes through 0 seconds of high-crime zone
    #    Treatment: route passes through ≥ MIN_HC_TIME_SEC of high-crime zone
    g0 = trips[trips["hc_time_sec"] == 0]["pct_time_increase"]
    g1 = trips[trips["hc_time_sec"] >= MIN_HC_TIME_SEC]["pct_time_increase"]
    excluded = trips[
        (trips["hc_time_sec"] > 0) & (trips["hc_time_sec"] < MIN_HC_TIME_SEC)
    ]

    print(f"\nControl  (0 sec in high-crime zones):         n={len(g0):,}")
    print(f"Excluded (0 < t < {MIN_HC_TIME_SEC//60} min):                    n={len(excluded):,}")
    print(f"Treated  (≥{MIN_HC_TIME_SEC//60} min in high-crime zones):      n={len(g1):,}")

    if len(g1) < 10:
        print("WARNING: too few treated observations — consider lowering MIN_HC_TIME_SEC")
        return

    t_stat, p_val = ttest_ind(g1, g0, equal_var=False)
    diff = g1.mean() - g0.mean()

    print(f"\nControl  mean pct_time_increase: {g0.mean():.1f}%")
    print(f"Treated  mean pct_time_increase: {g1.mean():.1f}%  "
          f"(mean hc time = {trips[trips['hc_time_sec'] >= MIN_HC_TIME_SEC]['hc_time_min'].mean():.1f} min)")
    print(f"Difference: {diff:+.1f} pp")
    print(f"Welch t-test: t = {t_stat:.3f},  p = {p_val:.4f}")

    # Save
    trips.to_parquet(f"{OUTPUT_DIR}/passthrough_analysis.parquet", index=False)

    # 8. Plot
    fig, ax = plt.subplots(figsize=(7, 5))

    labels = [
        f"Route avoids\nhigh-crime zones\n(n={len(g0):,})",
        f"Route spends ≥{MIN_HC_TIME_SEC//60} min\nin high-crime zone\n(n={len(g1):,})",
    ]
    means = [g0.mean(), g1.mean()]
    cis = [1.96 * g0.sem(), 1.96 * g1.sem()]
    colors = ["steelblue", "tomato"]

    ax.bar(labels, means, color=colors, alpha=0.8, width=0.45, yerr=cis, capsize=8)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)

    ax.set_ylabel("% Trip Time Increase  (actual / OSRM estimate − 1) × 100", fontsize=11)
    ax.set_title(
        f"In-Transit Crime Exposure and Trip Duration Excess\n"
        f"Treatment: ≥{MIN_HC_TIME_SEC//60} min of OSRM route in high-crime zone  (NYC Jan 2024)",
        fontsize=11,
    )

    p_str = "p < 0.001" if p_val < 0.001 else f"p = {p_val:.3f}"
    ax.text(
        0.5, 0.97,
        f"Δ = {diff:+.1f} pp  ({p_str})",
        ha="center", va="top", fontsize=11, fontweight="bold",
        transform=ax.transAxes,
    )

    plt.tight_layout()
    out_path = f"{OUTPUT_DIR}/passthrough_comparison.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"\nPlot saved to {out_path}")


if __name__ == "__main__":
    main()
