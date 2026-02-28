---
title: Analyze Citi Bike Passthrough
tags: [Analysis]
---

# Analyze Citi Bike Passthrough

Tests whether Citi Bike riders take longer-than-optimal routes when the OSRM-optimal
path spends meaningful time in a high-crime zone.

Citi Bike is better suited to this test than HVFHV because:
- Station coordinates are precise, so OSRM benchmarks are not contaminated by
  zone-centroid approximation error.
- Cyclists make active route choices with full street-level exposure, making
  avoidance behavior more plausible and detectable.
- The commute-hour filter targets repeated, deliberate trips rather than one-off rides.

## Method

1. Load filtered Citi Bike commute-hour trips from `download_citibike_data`.
2. Build zone-level crime rates and deciles from NYPD complaint data.
3. Map each start and end station to its enclosing taxi zone via spatial join.
4. Query OSRM for each unique (start station, end station) pair using precise
   coordinates (`overview=full&geometries=geojson`).
5. Compute `pct_time_increase = (actual − OSRM) / OSRM × 100`.
6. Project route LineStrings to EPSG:32618; intersect with taxi zone polygons.
7. Estimate time in each intermediate high-crime zone as:
   `(intersection_length / total_route_length) × OSRM_duration`.
8. Treatment group: trips where estimated time in high-crime intermediate zones
   ≥ `MIN_HC_TIME_SEC` (default 300 s = 5 min).
   Control group: trips with zero time in any high-crime intermediate zone.
9. Compare `pct_time_increase` with Welch t-test and bar chart.

Also produces a binned scatter of `pct_time_increase` by start-station zone crime
decile, comparable to the HVFHV `plot_data` output.

**Note:** OSRM uses a driving profile. Cycling routes may differ, particularly
in areas with dedicated bike infrastructure. The benchmark is used comparatively
(treatment vs. control), so systematic bias does not affect the estimated gap.

## Inputs

- `citibike_trips.parquet`: Filtered commute trips from `download_citibike_data`.
- `nyc_crime.csv`: NYPD complaint records from `download_crime_data`.
- `taxi_zones.geojson`: Zone polygon boundaries from `download_u2_data`.

## Outputs

- `citibike_passthrough_analysis.parquet`: Trips with OSRM estimates and
  `hc_time_sec`, `passes_high_crime` flags.
- `citibike_passthrough_comparison.png`: Bar chart comparing trip-time excess
  between treatment and control groups.
- `citibike_binned_scatter.png`: Binned scatter by start-zone crime decile.
