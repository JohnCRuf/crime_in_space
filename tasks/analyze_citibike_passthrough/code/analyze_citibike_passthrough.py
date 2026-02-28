"""Citi Bike passthrough analysis: do riders take longer routes when the optimal
path passes through a high-crime zone?

Uses precise station coordinates for OSRM queries (no zone-centroid approximation).
Treatment: OSRM-optimal route spends ≥ MIN_HC_TIME_SEC seconds in high-crime
intermediate zones (estimated via route-length fraction).
Control: OSRM-optimal route passes through no high-crime zones at all.
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

HIGH_CRIME_MIN_DECILE = 7   # top 30% of zones by crime rate
MIN_HC_TIME_SEC = 300       # 5 minutes — treatment threshold for bike trips


# ---------------------------------------------------------------------------
# Crime lookup
# ---------------------------------------------------------------------------

def build_zone_crime_lookup(crime_csv: str, zones_geojson: str) -> pd.DataFrame:
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
    zones_proj = zones_gdf.to_crs("EPSG:32618")
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
    counts = joined.groupby("LocationID").size().reset_index(name="crime_count")
    counts["LocationID"] = counts["LocationID"].astype(int)

    meta = zones_gdf[["LocationID", "area_km2"]].copy()
    meta["LocationID"] = meta["LocationID"].astype(int)
    zc = meta.merge(counts, on="LocationID", how="left")
    zc["crime_count"] = zc["crime_count"].fillna(0)
    zc["crime_rate_km2"] = zc["crime_count"] / zc["area_km2"]
    zc["zone_decile"] = (
        pd.qcut(zc["crime_rate_km2"], q=10, labels=False, duplicates="drop") + 1
    )
    return zc[["LocationID", "crime_rate_km2", "zone_decile"]].copy()


# ---------------------------------------------------------------------------
# Station → zone mapping
# ---------------------------------------------------------------------------

def map_stations_to_zones(
    stations: pd.DataFrame,  # columns: station_id, lat, lng
    zones_gdf: gpd.GeoDataFrame,
) -> pd.Series:
    """Return Series {station_id: LocationID}."""
    pts = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations["lng"], stations["lat"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(
        pts[["station_id", "geometry"]],
        zones_gdf[["LocationID", "geometry"]],
        how="left",
        predicate="within",
    )
    # Take the first match per station (in case of duplicates at zone boundaries)
    result = (
        joined.dropna(subset=["LocationID"])
        .groupby("station_id")["LocationID"]
        .first()
        .astype(int)
    )
    return result


# ---------------------------------------------------------------------------
# OSRM routing (precise coordinates)
# ---------------------------------------------------------------------------

def fetch_routes(pairs_df: pd.DataFrame, cache_path: str = None,
                 n_workers: int = 4) -> pd.DataFrame:
    """Query OSRM /route for each unique (start, end) station pair.

    Uses a thread pool to issue requests concurrently.
    Results are cached to cache_path (pickle) to survive reruns.
    Returns DataFrame with start_station_id, end_station_id,
    osrm_duration (sec), geometry (shapely).
    """
    import pickle
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cached: dict = {}
    cache_lock = threading.Lock()

    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            cached = pickle.load(f)
        print(f"  Loaded {len(cached)} cached routes from {cache_path}")

    def _has_route(r):
        entry = cached.get((r.start_station_id, r.end_station_id))
        return entry is not None and not np.isnan(entry.get("osrm_duration", np.nan))

    todo_rows = [row for row in pairs_df.itertuples() if not _has_route(row)]
    n_todo = len(todo_rows)
    n_total = len(pairs_df)
    print(f"  Fetching OSRM routes for {n_todo} pairs ({n_total - n_todo} cached) "
          f"using {n_workers} threads...")

    counters = {"ok": 0, "noroute": 0, "err": 0, "done": 0}
    counter_lock = threading.Lock()

    def _fetch_one(row):
        url = (
            f"{OSRM_ROUTE_BASE}/"
            f"{row.start_lng:.6f},{row.start_lat:.6f};"
            f"{row.end_lng:.6f},{row.end_lat:.6f}"
            "?overview=full&geometries=geojson"
        )
        entry = {"osrm_duration": np.nan, "geometry": None}
        for attempt in range(5):
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                code = data.get("code", "")
                if code == "Ok" and data.get("routes"):
                    entry["osrm_duration"] = data["routes"][0]["duration"]
                    entry["geometry"] = shape(data["routes"][0]["geometry"])
                    with counter_lock:
                        counters["ok"] += 1
                    break
                else:
                    with counter_lock:
                        counters["noroute"] += 1
                    break  # routing failure, no point retrying
            except requests.exceptions.ConnectTimeout:
                # Server rate-limiting: back off significantly
                wait = 30 * (attempt + 1)
                time.sleep(wait)
            except Exception as exc:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                else:
                    with counter_lock:
                        counters["err"] += 1

        key = (row.start_station_id, row.end_station_id)
        with cache_lock:
            cached[key] = entry
        return key

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_fetch_one, row): row for row in todo_rows}
        for i, fut in enumerate(as_completed(futures)):
            fut.result()
            with counter_lock:
                counters["done"] += 1
                done = counters["done"]
            if done % 500 == 0:
                with counter_lock:
                    ok, nr, err = counters["ok"], counters["noroute"], counters["err"]
                print(f"  {done}/{n_todo} done  (ok={ok}, noroute={nr}, err={err})")
                if cache_path:
                    with cache_lock:
                        snap = dict(cached)
                    with open(cache_path, "wb") as f:
                        pickle.dump(snap, f)

    if cache_path and n_todo > 0:
        with open(cache_path, "wb") as f:
            pickle.dump(cached, f)
        ok, nr, err = counters["ok"], counters["noroute"], counters["err"]
        print(f"  Saved {len(cached)} routes to cache ({ok} ok, {nr} noroute, {err} err)")

    results = []
    for _, row in pairs_df.iterrows():
        key = (row["start_station_id"], row["end_station_id"])
        entry = cached.get(key, {"osrm_duration": np.nan, "geometry": None})
        results.append({
            "start_station_id": row["start_station_id"],
            "end_station_id": row["end_station_id"],
            "osrm_duration": entry["osrm_duration"],
            "geometry": entry["geometry"],
        })
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Passthrough time computation
# ---------------------------------------------------------------------------

def compute_hc_time(
    routes_gdf: gpd.GeoDataFrame,   # index = row index, has trip_idx col
    zones_gdf: gpd.GeoDataFrame,
    high_crime_ids: set,
    osrm_durations: pd.Series,      # indexed like routes_gdf
    start_zone_col: str = "start_zone_id",
    end_zone_col: str = "end_zone_id",
) -> pd.Series:
    """Estimate seconds each trip's optimal route spends in high-crime zones,
    excluding start and end zones. Returns Series indexed like routes_gdf."""

    routes_proj = routes_gdf.to_crs("EPSG:32618")
    zones_proj = zones_gdf[zones_gdf["LocationID"].isin(high_crime_ids)].to_crs("EPSG:32618")

    if zones_proj.empty:
        return pd.Series(0.0, index=routes_gdf.index)

    route_lengths = routes_proj.geometry.length

    joined = gpd.sjoin(
        routes_proj[["trip_idx", start_zone_col, end_zone_col, "geometry"]],
        zones_proj[["LocationID", "geometry"]],
        how="inner",
        predicate="intersects",
    )
    # Drop repeated route geometry to free memory before the intersection loop
    joined = pd.DataFrame(joined.drop(columns=["geometry"]))
    joined = joined[
        (joined["LocationID"] != joined[start_zone_col]) &
        (joined["LocationID"] != joined[end_zone_col])
    ]

    if joined.empty:
        return pd.Series(0.0, index=routes_gdf.index)

    route_geom_dict = dict(zip(routes_proj["trip_idx"], routes_proj.geometry))
    route_len_dict = dict(zip(routes_proj["trip_idx"], route_lengths))
    zone_geom_dict = {int(r.LocationID): r.geometry for r in zones_proj.itertuples()}

    # Build trip_idx → index mapping for duration lookup
    tidx_to_idx = dict(zip(routes_gdf["trip_idx"], routes_gdf.index))

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

        idx = tidx_to_idx.get(trip_idx)
        dur = osrm_durations.iloc[idx] if idx is not None else np.nan
        if np.isnan(dur):
            continue

        hc_time[trip_idx] = hc_time.get(trip_idx, 0.0) + (seg.length / total_len) * dur

    # Map back to routes_gdf index
    result = {}
    for trip_idx, val in hc_time.items():
        idx = tidx_to_idx.get(trip_idx)
        if idx is not None:
            result[idx] = val
    return pd.Series(result).reindex(routes_gdf.index, fill_value=0.0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Crime lookup
    print("Building zone crime lookup...")
    zone_crime = build_zone_crime_lookup(
        f"{INPUT_DIR}/nyc_crime.csv",
        f"{INPUT_DIR}/taxi_zones.geojson",
    )
    zone_decile_map = dict(zip(
        zone_crime["LocationID"].astype(int),
        zone_crime["zone_decile"],
    ))

    # 2. Load trips
    trips = pd.read_parquet(f"{INPUT_DIR}/citibike_trips.parquet")
    trips = trips.dropna(subset=["start_lat", "start_lng", "end_lat", "end_lng"])
    trips = trips.reset_index(drop=True)
    print(f"Trips loaded: {len(trips):,}")

    # 3. Load zones
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

    # 4. Map stations to zones
    print("Mapping stations to taxi zones...")
    all_stations = pd.concat([
        trips[["start_station_id", "start_lat", "start_lng"]]
        .rename(columns={"start_station_id": "station_id", "start_lat": "lat", "start_lng": "lng"}),
        trips[["end_station_id", "end_lat", "end_lng"]]
        .rename(columns={"end_station_id": "station_id", "end_lat": "lat", "end_lng": "lng"}),
    ], ignore_index=True).drop_duplicates("station_id")

    station_zone = map_stations_to_zones(all_stations, zones_gdf)
    trips["start_zone_id"] = trips["start_station_id"].map(station_zone).fillna(-1).astype(int)
    trips["end_zone_id"] = trips["end_station_id"].map(station_zone).fillna(-1).astype(int)
    trips["start_crime_decile"] = trips["start_zone_id"].map(zone_decile_map)

    # 5. Get unique station-pair coordinates for OSRM
    pairs_df = (
        trips[["start_station_id", "end_station_id",
               "start_lat", "start_lng", "end_lat", "end_lng"]]
        .drop_duplicates(subset=["start_station_id", "end_station_id"])
        .reset_index(drop=True)
    )
    # Use mean coordinates per station (in case of minor floating-point variation)
    coord_agg = (
        trips.groupby("start_station_id")[["start_lat", "start_lng"]].mean()
        .rename(columns={"start_lat": "slat", "start_lng": "slng"})
    )
    coord_agg_e = (
        trips.groupby("end_station_id")[["end_lat", "end_lng"]].mean()
        .rename(columns={"end_lat": "elat", "end_lng": "elng"})
    )
    pairs_df = (
        pairs_df.merge(coord_agg, on="start_station_id", how="left")
                .merge(coord_agg_e, on="end_station_id", how="left")
    )
    pairs_df["start_lat"] = pairs_df["slat"].fillna(pairs_df["start_lat"])
    pairs_df["start_lng"] = pairs_df["slng"].fillna(pairs_df["start_lng"])
    pairs_df["end_lat"] = pairs_df["elat"].fillna(pairs_df["end_lat"])
    pairs_df["end_lng"] = pairs_df["elng"].fillna(pairs_df["end_lng"])
    pairs_df = pairs_df.drop(columns=["slat", "slng", "elat", "elng"])

    print("Fetching OSRM routes...")
    routes_df = fetch_routes(pairs_df, cache_path=f"{OUTPUT_DIR}/geom_cache.pkl")
    routes_df = routes_df.dropna(subset=["osrm_duration"])
    print(f"  Pairs with valid OSRM route: {len(routes_df):,} / {len(pairs_df):,}")

    # 6. Merge OSRM results onto trips
    trips = trips.merge(
        routes_df[["start_station_id", "end_station_id", "osrm_duration"]],
        on=["start_station_id", "end_station_id"],
        how="left",
    )
    trips = trips.dropna(subset=["osrm_duration"]).copy()
    if trips.empty:
        raise RuntimeError(
            "No trips matched OSRM routes. "
            "Check geom_cache.pkl — if all entries have osrm_duration=NaN, "
            "the public OSRM server may have been unavailable."
        )
    # Exclude zero-duration OSRM routes (same-node snapping artefact)
    trips = trips[trips["osrm_duration"] > 0].copy()
    trips["pct_time_increase"] = (
        (trips["duration_sec"] - trips["osrm_duration"]) / trips["osrm_duration"] * 100
    )
    # Drop non-finite values and outliers beyond ±3 SD
    trips = trips[np.isfinite(trips["pct_time_increase"])].copy()
    m, s = trips["pct_time_increase"].mean(), trips["pct_time_increase"].std()
    trips = trips[trips["pct_time_increase"].between(m - 3 * s, m + 3 * s)].copy()
    trips = trips.reset_index(drop=True)
    print(f"Trips with OSRM estimates: {len(trips):,}")

    # 7. Build route GeoDataFrame for spatial intersection
    print("Computing time in high-crime intermediate zones...")
    geom_lookup = dict(zip(
        routes_df["start_station_id"] + "||" + routes_df["end_station_id"],
        routes_df["geometry"],
    ))
    osrm_lookup = dict(zip(
        routes_df["start_station_id"] + "||" + routes_df["end_station_id"],
        routes_df["osrm_duration"],
    ))

    route_rows = []
    for row in trips.itertuples():
        key = f"{row.start_station_id}||{row.end_station_id}"
        geom = geom_lookup.get(key)
        if geom is not None:
            route_rows.append({
                "trip_idx": row.Index,
                "start_zone_id": row.start_zone_id,
                "end_zone_id": row.end_zone_id,
                "osrm_duration": osrm_lookup.get(key, np.nan),
                "geometry": geom,
            })

    routes_gdf = gpd.GeoDataFrame(route_rows, geometry="geometry", crs="EPSG:4326")

    hc_time = compute_hc_time(
        routes_gdf, zones_gdf, high_crime_ids,
        osrm_durations=pd.Series(
            routes_gdf["osrm_duration"].values, index=routes_gdf.index
        ),
    )
    routes_gdf["hc_time_sec"] = hc_time
    trips = trips.join(
        routes_gdf.set_index("trip_idx")[["hc_time_sec"]], how="left"
    )
    trips["hc_time_sec"] = trips["hc_time_sec"].fillna(0)

    # 8. Sample selection
    g0 = trips[trips["hc_time_sec"] == 0]["pct_time_increase"]
    g1 = trips[trips["hc_time_sec"] >= MIN_HC_TIME_SEC]["pct_time_increase"]
    n_excluded = ((trips["hc_time_sec"] > 0) & (trips["hc_time_sec"] < MIN_HC_TIME_SEC)).sum()

    print(f"\nControl  (0 sec in high-crime):         n={len(g0):,}")
    print(f"Excluded (0 < t < {MIN_HC_TIME_SEC}s):              n={n_excluded:,}")
    print(f"Treated  (≥{MIN_HC_TIME_SEC//60} min in high-crime):    n={len(g1):,}")

    if len(g1) < 10:
        print("WARNING: too few treated trips — consider lowering MIN_HC_TIME_SEC")
        return

    t_stat, p_val = ttest_ind(g1, g0, equal_var=False)
    diff = g1.mean() - g0.mean()
    hc_mean_min = trips[trips["hc_time_sec"] >= MIN_HC_TIME_SEC]["hc_time_sec"].mean() / 60

    print(f"\nControl mean pct_time_increase: {g0.mean():.1f}%")
    print(
        f"Treated mean pct_time_increase: {g1.mean():.1f}%  "
        f"(mean time in high-crime zone = {hc_mean_min:.1f} min)"
    )
    print(f"Difference: {diff:+.1f} pp")
    print(f"Welch t-test: t = {t_stat:.3f},  p = {p_val:.4f}")

    trips.to_parquet(f"{OUTPUT_DIR}/citibike_passthrough_analysis.parquet", index=False)

    # 9. Passthrough bar chart
    fig, ax = plt.subplots(figsize=(7, 5))
    labels = [
        f"Route avoids\nhigh-crime zones\n(n={len(g0):,})",
        f"Route spends ≥{MIN_HC_TIME_SEC//60} min\nin high-crime zone\n(n={len(g1):,})",
    ]
    means = [g0.mean(), g1.mean()]
    cis = [1.96 * g0.sem(), 1.96 * g1.sem()]
    ax.bar(labels, means, color=["steelblue", "tomato"], alpha=0.8,
           width=0.45, yerr=cis, capsize=8)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM estimate − 1) × 100", fontsize=11)
    ax.set_title(
        f"In-Transit Crime Exposure and Trip Duration — Citi Bike\n"
        f"Weekday Peak-Hour Members,  NYC Jan 2024",
        fontsize=11,
    )
    p_str = "p < 0.001" if p_val < 0.001 else f"p = {p_val:.3f}"
    ax.text(0.5, 0.97, f"Δ = {diff:+.1f} pp  ({p_str})",
            ha="center", va="top", fontsize=11, fontweight="bold",
            transform=ax.transAxes)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/citibike_passthrough_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("Saved citibike_passthrough_comparison.png")

    # 10. Binned scatter by start-zone crime decile
    df_plot = trips.dropna(subset=["start_crime_decile", "pct_time_increase"]).copy()
    df_plot["start_crime_decile"] = df_plot["start_crime_decile"].astype(int)
    grouped = df_plot.groupby("start_crime_decile")["pct_time_increase"]
    deciles = sorted(df_plot["start_crime_decile"].unique())
    means_s = grouped.mean().reindex(deciles)
    cis_s = 1.96 * grouped.sem().reindex(deciles)
    counts_s = grouped.count().reindex(deciles)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(deciles, means_s, color="steelblue", zorder=5, s=80)
    ax.errorbar(deciles, means_s, yerr=cis_s,
                fmt="none", color="steelblue", alpha=0.55, capsize=5, linewidth=1.5)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_xticks(deciles)
    ax.set_xlabel("Start Station Zone Crime Decile  (1 = Lowest, 10 = Highest)", fontsize=11)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM estimate − 1) × 100", fontsize=11)
    ax.set_title(
        "Route Detour by Start-Zone Crime Level — Citi Bike\n"
        "Weekday Peak-Hour Members,  NYC Jan 2024",
        fontsize=12,
    )
    for x, y, n in zip(deciles, means_s, counts_s):
        ax.annotate(f"n={int(n)}", xy=(x, y), xytext=(0, 10),
                    textcoords="offset points", ha="center", fontsize=7, color="gray")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/citibike_binned_scatter.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("Saved citibike_binned_scatter.png")


if __name__ == "__main__":
    main()
