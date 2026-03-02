"""Destination crime analysis: do HVFHV trips ending in high-crime zones have
higher excess duration than trips ending in low-crime zones?

Treatment: dropoff zone do_crime_decile >= HIGH_CRIME_MIN_DECILE (top 30%).
Control:   dropoff zone do_crime_decile <= CONTROL_MAX_DECILE (bottom 50%).
Outcome:   pct_time_increase = (actual − OSRM) / OSRM × 100.

do_crime_decile (DO zone) and pct_time_increase are already in analysis_data.parquet
from clean_data — no additional routing computation is required.
"""

import os

import matplotlib.pyplot as plt
import pandas as pd
from scipy.stats import ttest_ind

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"

HIGH_CRIME_MIN_DECILE = 7   # top 30% of zones — treatment threshold
CONTROL_MAX_DECILE    = 5   # bottom 50% of zones — clean control group


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    trips = pd.read_parquet(f"{INPUT_DIR}/analysis_data.parquet")
    trips = trips.dropna(subset=["do_crime_decile", "pct_time_increase"]).copy()
    trips["do_crime_decile"] = trips["do_crime_decile"].astype(int)
    print(f"Total trips: {len(trips):,}")

    g0 = trips[trips["do_crime_decile"] <= CONTROL_MAX_DECILE]["pct_time_increase"]
    g1 = trips[trips["do_crime_decile"] >= HIGH_CRIME_MIN_DECILE]["pct_time_increase"]
    n_mid = len(trips) - len(g0) - len(g1)

    print(f"\nControl (DO decile ≤ {CONTROL_MAX_DECILE}):                n={len(g0):,}")
    print(f"Middle  (decile {CONTROL_MAX_DECILE+1}–{HIGH_CRIME_MIN_DECILE-1}, excluded):       n={n_mid:,}")
    print(f"Treated (DO decile ≥ {HIGH_CRIME_MIN_DECILE}):                n={len(g1):,}")

    t_stat, p_val = ttest_ind(g1, g0, equal_var=False)
    diff = g1.mean() - g0.mean()

    print(f"\nControl mean pct_time_increase: {g0.mean():.1f}%")
    print(f"Treated mean pct_time_increase: {g1.mean():.1f}%")
    print(f"Difference: {diff:+.1f} pp")
    print(f"Welch t-test: t = {t_stat:.3f},  p = {p_val:.4f}")

    trips.to_parquet(f"{OUTPUT_DIR}/do_crime_analysis.parquet", index=False)

    # --- Bar chart ---
    fig, ax = plt.subplots(figsize=(7, 5))
    labels = [
        f"Low-crime dropoff zone\n(decile ≤ {CONTROL_MAX_DECILE},  n={len(g0):,})",
        f"High-crime dropoff zone\n(decile ≥ {HIGH_CRIME_MIN_DECILE},  n={len(g1):,})",
    ]
    means = [g0.mean(), g1.mean()]
    cis   = [1.96 * g0.sem(), 1.96 * g1.sem()]
    ax.bar(labels, means, color=["steelblue", "tomato"], alpha=0.8,
           width=0.45, yerr=cis, capsize=8)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM − 1) × 100", fontsize=11)
    ax.set_title(
        "Dropoff-Zone Crime Level and Trip Duration Excess — HVFHV\n"
        "NYC Jan 2024",
        fontsize=11,
    )
    p_str = "p < 0.001" if p_val < 0.001 else f"p = {p_val:.3f}"
    ax.text(0.5, 0.97, f"Δ = {diff:+.1f} pp  ({p_str})",
            ha="center", va="top", fontsize=11, fontweight="bold",
            transform=ax.transAxes)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/do_crime_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\nSaved do_crime_comparison.png")

    # --- Binned scatter ---
    df_plot = trips.dropna(subset=["do_crime_decile", "pct_time_increase"]).copy()
    grouped  = df_plot.groupby("do_crime_decile")["pct_time_increase"]
    deciles  = sorted(df_plot["do_crime_decile"].unique())
    means_d  = grouped.mean().reindex(deciles)
    cis_d    = 1.96 * grouped.sem().reindex(deciles)
    counts_d = grouped.count().reindex(deciles)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(deciles, means_d, color="steelblue", zorder=5, s=80)
    ax.errorbar(deciles, means_d, yerr=cis_d,
                fmt="none", color="steelblue", alpha=0.55, capsize=5, linewidth=1.5)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_xticks(deciles)
    ax.set_xlabel("Dropoff Zone Crime Decile  (1 = Lowest, 10 = Highest)", fontsize=11)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM − 1) × 100", fontsize=11)
    ax.set_title(
        "Route Detour by Dropoff-Zone Crime Level — HVFHV\n"
        "NYC Jan 2024",
        fontsize=12,
    )
    for x, y, n in zip(deciles, means_d, counts_d):
        ax.annotate(f"n={int(n)}", xy=(x, y), xytext=(0, 10),
                    textcoords="offset points", ha="center", fontsize=7, color="gray")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/do_crime_binned_scatter.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved do_crime_binned_scatter.png")


if __name__ == "__main__":
    main()
