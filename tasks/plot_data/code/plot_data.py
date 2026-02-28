"""Binned scatterplots: trip duration excess by pickup-zone and dropoff-zone crime decile.

Research question: do for-hire vehicle drivers take longer-than-optimal routes
when operating in or through high-crime neighborhoods?
A positive value on the y-axis means the actual trip took longer than the OSRM estimate.
"""

import os

import matplotlib.pyplot as plt
import pandas as pd

INPUT_DIR = "../input"
OUTPUT_DIR = "../output"


def binned_scatter(ax, df, decile_col, title, xlabel):
    df = df.dropna(subset=[decile_col, "pct_time_increase"]).copy()
    df[decile_col] = df[decile_col].astype(int)

    grouped = df.groupby(decile_col)["pct_time_increase"]
    deciles = sorted(df[decile_col].unique())
    means = grouped.mean().reindex(deciles)
    sems = grouped.sem().reindex(deciles)
    cis = 1.96 * sems
    counts = grouped.count().reindex(deciles)

    ax.scatter(deciles, means, color="steelblue", zorder=5, s=80)
    ax.errorbar(
        deciles, means, yerr=cis,
        fmt="none", color="steelblue", alpha=0.55, capsize=5, linewidth=1.5,
    )
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.set_xticks(deciles)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel("% Trip Time Increase  (actual / OSRM estimate − 1) × 100", fontsize=10)
    ax.set_title(title, fontsize=11)

    for x, y, n in zip(deciles, means, counts):
        ax.annotate(
            f"n={int(n)}",
            xy=(x, y),
            xytext=(0, 10),
            textcoords="offset points",
            ha="center",
            fontsize=7,
            color="gray",
        )


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_parquet(f"{INPUT_DIR}/analysis_data.parquet")

    # Single-panel: pickup zone (original plot, kept for downstream compatibility)
    fig, ax = plt.subplots(figsize=(8, 5))
    binned_scatter(
        ax, df,
        decile_col="crime_decile",
        title="Route Detour by Pickup-Zone Crime Level\n"
              "NYC For-Hire Vehicles vs. OSRM Optimal Route  (Jan 2024)",
        xlabel="Pickup Zone Crime Decile  (1 = Lowest Crime, 10 = Highest Crime)",
    )
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/binned_scatter_crime_decile.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("Plot saved to ../output/binned_scatter_crime_decile.png")

    # Single-panel: dropoff zone
    fig, ax = plt.subplots(figsize=(8, 5))
    binned_scatter(
        ax, df,
        decile_col="do_crime_decile",
        title="Route Detour by Dropoff-Zone Crime Level\n"
              "NYC For-Hire Vehicles vs. OSRM Optimal Route  (Jan 2024)",
        xlabel="Dropoff Zone Crime Decile  (1 = Lowest Crime, 10 = Highest Crime)",
    )
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/binned_scatter_do_crime_decile.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("Plot saved to ../output/binned_scatter_do_crime_decile.png")

    # Side-by-side comparison
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    binned_scatter(
        axes[0], df,
        decile_col="crime_decile",
        title="By Pickup Zone",
        xlabel="Pickup Zone Crime Decile",
    )
    binned_scatter(
        axes[1], df,
        decile_col="do_crime_decile",
        title="By Dropoff Zone",
        xlabel="Dropoff Zone Crime Decile",
    )
    axes[1].set_ylabel("")
    fig.suptitle(
        "Route Detour by Neighborhood Crime Level  (NYC For-Hire Vehicles, Jan 2024)",
        fontsize=12, y=1.01,
    )
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/binned_scatter_pu_do_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("Plot saved to ../output/binned_scatter_pu_do_comparison.png")


if __name__ == "__main__":
    main()
