---
title: Analyze DO Crime
tags: [Analysis]
---

# Analyze DO Crime

Tests whether HVFHV trips that terminate in high-crime zones have systematically
higher trip-duration excess (`pct_time_increase`) than trips terminating in
low-crime zones.

**Treatment:** dropoff zone `do_crime_decile ≥ 7` (top 30% by crime rate per sq km).
**Control:** dropoff zone `do_crime_decile ≤ 5` (bottom 50%).

The outcome variable is already in `analysis_data.parquet`, so no routing
computation is required. This analysis is fast.

## Method

1. Load `analysis_data.parquet` from `clean_data` (contains `do_crime_decile` for
   the dropoff zone and `pct_time_increase` for each trip).
2. Split into control (decile ≤ 5) and treated (decile ≥ 7) groups; trips in
   deciles 6 are excluded.
3. Compare mean `pct_time_increase` with a Welch t-test.
4. Plot a bar chart with 95% CIs and a binned scatter across all deciles.

## Inputs

- `analysis_data.parquet`: Cleaned trip dataset from `clean_data`.

## Outputs

- `do_crime_analysis.parquet`: Full trip dataset (passthrough to downstream tasks).
- `do_crime_comparison.png`: Bar chart comparing treated vs. control groups.
- `do_crime_binned_scatter.png`: Mean `pct_time_increase` by dropoff-zone crime decile.
