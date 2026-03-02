---
title: Analyze Citi Bike End Crime
tags: [Analysis]
---

# Analyze Citi Bike End Crime

Tests whether Citi Bike trips terminating in high-crime zones have systematically
higher trip-duration excess (`pct_time_increase`) than trips terminating in
low-crime zones.

**Treatment:** end station zone `crime_decile ≥ 7` (top 30% by crime rate per sq km).
**Control:** end station zone `crime_decile ≤ 5` (bottom 50%).

Uses precise station coordinates mapped to taxi zones — no zone-centroid approximation.

## Method

1. Load `citibike_passthrough_analysis.parquet` from `analyze_citibike_passthrough`
   (contains `pct_time_increase`, `start_zone_id`, `end_zone_id`).
2. Rebuild zone crime rates and deciles from NYPD complaint data and taxi zone polygons.
3. Map `end_zone_id` to `end_crime_decile`.
4. Split into control (decile ≤ 5) and treated (decile ≥ 7) groups; decile 6 excluded.
5. Compare mean `pct_time_increase` with a Welch t-test.
6. Plot a bar chart with 95% CIs and a binned scatter across all deciles.

## Inputs

- `citibike_passthrough_analysis.parquet`: Trip data with OSRM estimates from
  `analyze_citibike_passthrough`.
- `nyc_crime.csv`: NYPD complaint records from `download_crime_data`.
- `taxi_zones.geojson`: Zone polygon boundaries from `download_u2_data`.

## Outputs

- `citibike_end_crime_analysis.parquet`: Trip dataset with `end_crime_decile`.
- `citibike_end_comparison.png`: Bar chart comparing treated vs. control groups.
- `citibike_end_binned_scatter.png`: Mean `pct_time_increase` by end-zone crime decile.
