---
title: Clean Data
tags: [Data]
---

# Clean Data

This task merges the HVFHV trip-route data with NYC crime data to produce the final
analysis dataset.
Crime counts are aggregated to the taxi zone level via a spatial join, then attached to
each trip by both pickup and dropoff zone.
Trips are binned into crime deciles (separately for PU and DO zones) to support the
analyses in `plot_data` and `analyze_passthrough`.

## Method

1. Load crime complaint records and convert to a GeoDataFrame using WGS84 coordinates.
2. Load taxi zone polygon boundaries.
3. Spatially join each crime point to its enclosing taxi zone.
4. Count total complaints per zone to produce a zone-level crime count.
5. Merge zone crime rates onto trips by `PULocationID` (`crime_rate_km2`) and
   separately by `DOLocationID` (`do_crime_rate_km2`).
6. Compute `pct_time_increase = (actual − estimated) / estimated × 100`.
7. Drop outliers beyond ±3 standard deviations of `pct_time_increase`.
8. Assign each trip two crime deciles (1–10): `crime_decile` based on the pickup zone
   and `do_crime_decile` based on the dropoff zone.

## Inputs

- `trips_with_routes.parquet`: HVFHV trips with actual and estimated durations from `get_routes`.
- `nyc_crime.csv`: NYPD complaint records from `download_crime_data`.
- `taxi_zones.geojson`: Zone polygon boundaries from `download_u2_data`.

## Outputs

- `analysis_data.parquet`: Cleaned, merged dataset with columns `crime_decile`,
  `do_crime_decile`, `crime_rate_km2`, `do_crime_rate_km2`, `pct_time_increase`,
  `PULocationID`, and `DOLocationID`.
