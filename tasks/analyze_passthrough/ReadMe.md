---
title: Analyze Passthrough
tags: [Analysis]
---

# Analyze Passthrough

Tests whether HVFHV (Uber/Lyft) drivers take longer-than-optimal routes when the
OSRM-optimal path spends meaningful time in a high-crime area.

**Finding:** Treated trips show *lower* excess time than control (Δ = −24.3 pp,
p < 0.0001), a wrong-direction result explained by the zone-centroid confounder —
OSRM routes are computed between zone centroids rather than precise coordinates, so
large high-crime zones produce artificially long benchmarks that understate apparent
excess for treated trips. See `analyze_citibike_passthrough` for the centroid-free test.

## Method

1. Compute crime rates (complaints per sq km) and deciles for all 263 taxi zones,
   using the same spatial-join approach as `clean_data`.
2. For each unique (PU, DO) pair in the full dataset, fetch the OSRM route geometry
   (`overview=full&geometries=geojson`). Geometries are cached in `geom_cache.pkl`
   for resumable runs.
3. Project route LineStrings and zone polygons to EPSG:32618. Intersect each route
   with high-crime zones (decile ≥ 9, top 20%), excluding the trip's origin and
   destination zones.
4. Estimate time in each intermediate high-crime zone as:
   `(intersection_length / total_route_length) × OSRM_duration`.
5. Sum across zones to get total high-crime intermediate time per trip.
6. **Treatment** (≥ 10 min in high-crime intermediate zones) vs.
   **Control** (0 sec); trips with 0 < t < 10 min excluded.
7. Compare `pct_time_increase` with Welch t-test and a bar chart with 95% CIs.

## Inputs

- `analysis_data.parquet`: Cleaned trip dataset from `clean_data`.
- `taxi_zones.geojson`: Zone polygon boundaries from `download_u2_data`.
- `nyc_crime.csv`: NYPD complaint records from `download_crime_data`.

## Outputs

- `passthrough_analysis.parquet`: Full trip dataset with `hc_time_sec` and
  `passes_high_crime` flag.
- `passthrough_comparison.png`: Bar chart comparing trip-time excess between the
  two groups.
- `geom_cache.pkl`: Cached OSRM route geometries (5,626 pairs).

## Results (January 2024, n = 9,320 trips)

| Group | n | Mean pct\_time\_increase |
|---|---|---|
| Control (0 sec in HC zones) | 2,748 | 87.1% |
| Treated (≥ 10 min in HC zones) | 552 | 62.8% |
| Excluded (0 < t < 10 min) | 6,020 | — |

Welch t-test: t = −9.648, p < 0.0001.

**Interpretation:** The negative difference reflects the zone-centroid confounder, not
avoidance behavior. See logbook entry 20260226_JR.md for full discussion.
