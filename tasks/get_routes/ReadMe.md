---
title: Get Routes
tags: [Data, API]
---

# Get Routes

This task queries the [OSRM (Open Source Routing Machine)](http://project-osrm.org/) public API
to estimate the optimal driving duration for a sample of HVFHV trips.
The estimated duration is compared against the actual trip duration recorded in the TLC data
to measure route deviation.

## Method

For each sampled trip, the centroid of the pickup taxi zone and the centroid of the dropoff
taxi zone are computed from the GeoJSON boundaries downloaded in `download_u2_data`.
These centroids serve as the origin and destination coordinates for the OSRM query.

OSRM returns the shortest-time driving route and its estimated duration in seconds under
free-flow conditions.
We record both the estimated and actual durations so that `time_diff = actual − estimated`
can be computed in `clean_data`.

## Inputs

- `hvfhv_trips.parquet`: HVFHV trip records from `download_u2_data`.
- `taxi_zones.geojson`: Zone polygon boundaries from `download_u2_data`.

## Outputs

- `trips_with_routes.parquet`: Sampled trips augmented with OSRM-estimated duration,
  actual duration, and raw time difference (seconds).

## Notes

The OSRM public API (`router.project-osrm.org`) is free and requires no API key but is
subject to rate limits.
A 0.1-second delay between requests is enforced.
Adjust `SAMPLE_SIZE` at the top of `get_routes.py` to control how many trips are queried.
