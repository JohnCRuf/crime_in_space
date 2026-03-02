"""Citi Bike origin crime analysis: do rides starting in high-crime zones have
higher excess duration than rides starting in low-crime zones?

Treatment: start station zone crime_decile >= HIGH_CRIME_MIN_DECILE (top 30%).
Control:   start station zone crime_decile <= CONTROL_MAX_DECILE (bottom 50%).
Outcome:   pct_time_increase = (actual − OSRM) / OSRM × 100.

Reads from citibike_passthrough_analysis.parquet (which already has pct_time_increase,
start_zone_id, end_zone_id). Rebuilds zone crime deciles from nyc_crime.csv and
taxi_zones.geojson to attach start_crime_decile.
"""

import os

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import ttest_ind

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"

HIGH_CRIME_MIN_DECILE = 7   # top 30% of zones — treatment threshold
CONTROL_MAX_DECILE    = 5   # bottom 50% of zones — clean control group


def build_zone_crime_lookup(crime_csv: str, zones_geojson: str) -> pd.DataFrame:
    crime = pd.read_csv(crime_csv)
    crime["latitude"]  = pd.to_numeric(crime["latitude"],  errors="coerce")
    crime["longitude"] = pd.to_numeric(crime["longitude"], errors="coerce")
    crime = crime.dropna(subset=["latitude", "longitude"])

    zones_gdf = gpd.read_file(zones_geojson).to_crs("EPSG:4326")
    id_col = next(
        c for c in zones_gdf.columns
        if c.lower().replace("_", "") in ("locationid", "locationi")
    )
    zones_gdf = zones_gdf.rename(columns={id_col: "LocationID"})
    zones_gdf["LocationID"] = pd.to_numeric(zones_gdf["LocationID"], errors="coerce").astype("Int64")
    zones_gdf["area_km2"] = zones_gdf.to_crs("EPSG:32618").geometry.area / 1e6

    crime_gdf = gpd.GeoDataFrame(
        crime,
        geometry=gpd.points_from_xy(crime["longitude"], crime["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(crime_gdf, zones_gdf[["LocationID", "geometry"]],
                       how="left", predicate="within")
    counts = joined.groupby("LocationID").size().reset_index(name="crime_count")
    counts["LocationID"] = counts["LocationID"].astype(int)

    meta = zones_gdf[["LocationID", "area_km2"]].copy()
    meta["LocationID"] = meta["LocationID"].astype(int)
    zc = meta.merge(counts, on="LocationID", how="left")
    zc["crime_count"]    = zc["crime_count"].fillna(0)
    zc["crime_rate_km2"] = zc["crime_count"] / zc["area_km2"]
    zc["zone_decile"]    = (
        pd.qcut(zc["crime_rate_km2"], q=10, labels=False, duplicates="drop") + 1
    )
    return zc[["LocationID", "crime_rate_km2", "zone_decile"]].copy()


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Zone crime lookup
    print("Building zone crime lookup...")
    zone_crime = build_zone_crime_lookup(
        f"{INPUT_DIR}/nyc_crime.csv",
        f"{INPUT_DIR}/taxi_zones.geojson",
    )
    zone_decile_map = dict(zip(zone_crime["LocationID"].astype(int), zone_crime["zone_decile"]))

    # 2. Load trips from passthrough output (already has pct_time_increase, start_zone_id)
    trips = pd.read_parquet(f"{INPUT_DIR}/citibike_passthrough_analysis.parquet")
    trips = trips.dropna(subset=["pct_time_increase", "start_zone_id"]).copy()
    trips["start_zone_id"]      = trips["start_zone_id"].astype(int)
    trips["start_crime_decile"] = trips["start_zone_id"].map(zone_decile_map)
    trips = trips.dropna(subset=["start_crime_decile"]).copy()
    trips["start_crime_decile"] = trips["start_crime_decile"].astype(int)
    print(f"Total trips: {len(trips):,}")

    g0 = trips[trips["start_crime_decile"] <= CONTROL_MAX_DECILE]["pct_time_increase"]
    g1 = trips[trips["start_crime_decile"] >= HIGH_CRIME_MIN_DECILE]["pct_time_increase"]
    n_mid = len(trips) - len(g0) - len(g1)

    print(f"\nControl (start decile ≤ {CONTROL_MAX_DECILE}):              n={len(g0):,}")
    print(f"Middle  (decile {CONTROL_MAX_DECILE+1}–{HIGH_CRIME_MIN_DECILE-1}, excluded):     n={n_mid:,}")
    print(f"Treated (start decile ≥ {HIGH_CRIME_MIN_DECILE}):              n={len(g1):,}")

    t_stat, p_val = ttest_ind(g1, g0, equal_var=False)
    diff = g1.mean() - g0.mean()

    print(f"\nControl mean pct_time_increase: {g0.mean():.1f}%")
    print(f"Treated mean pct_time_increase: {g1.mean():.1f}%")
    print(f"Difference: {diff:+.1f} pp")
    print(f"Welch t-test: t = {t_stat:.3f},  p = {p_val:.4f}")

    trips.to_parquet(f"{OUTPUT_DIR}/citibike_start_crime_analysis.parquet", index=False)

    # --- Bar chart ---
    fig, ax = plt.subplots(figsize=(7, 5))
    labels = [
        f"Low-crime start zone\n(decile ≤ {CONTROL_MAX_DECILE},  n={len(g0):,})",
        f"High-crime start zone\n(decile ≥ {HIGH_CRIME_MIN_DECILE},  n={len(g1):,})",
    ]
    means = [g0.mean(), g1.mean()]
    cis   = [1.96 * g0.sem(), 1.96 * g1.sem()]
    ax.bar(labels, means, color=["steelblue", "tomato"], alpha=0.8,
           width=0.45, yerr=cis, capsize=8)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM − 1) × 100", fontsize=11)
    ax.set_title(
        "Start-Zone Crime Level and Trip Duration Excess — Citi Bike\n"
        "Weekday Peak-Hour Members,  NYC Jan 2024",
        fontsize=11,
    )
    p_str = "p < 0.001" if p_val < 0.001 else f"p = {p_val:.3f}"
    ax.text(0.5, 0.97, f"Δ = {diff:+.1f} pp  ({p_str})",
            ha="center", va="top", fontsize=11, fontweight="bold",
            transform=ax.transAxes)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/citibike_start_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\nSaved citibike_start_comparison.png")

    # --- Binned scatter ---
    grouped  = trips.groupby("start_crime_decile")["pct_time_increase"]
    deciles  = sorted(trips["start_crime_decile"].unique())
    means_d  = grouped.mean().reindex(deciles)
    cis_d    = 1.96 * grouped.sem().reindex(deciles)
    counts_d = grouped.count().reindex(deciles)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(deciles, means_d, color="steelblue", zorder=5, s=80)
    ax.errorbar(deciles, means_d, yerr=cis_d,
                fmt="none", color="steelblue", alpha=0.55, capsize=5, linewidth=1.5)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_xticks(deciles)
    ax.set_xlabel("Start Station Zone Crime Decile  (1 = Lowest, 10 = Highest)", fontsize=11)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM − 1) × 100", fontsize=11)
    ax.set_title(
        "Route Detour by Start-Zone Crime Level — Citi Bike\n"
        "Weekday Peak-Hour Members,  NYC Jan 2024",
        fontsize=12,
    )
    for x, y, n in zip(deciles, means_d, counts_d):
        ax.annotate(f"n={int(n)}", xy=(x, y), xytext=(0, 10),
                    textcoords="offset points", ha="center", fontsize=7, color="gray")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/citibike_start_binned_scatter.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved citibike_start_binned_scatter.png")


if __name__ == "__main__":
    main()
