---
title: Download Citi Bike Data
tags: [Data, Download]
---

# Download Citi Bike Data

Downloads NYC Citi Bike trip records for January 2024 and filters to weekday
peak-hour trips made by annual members.

Citi Bike is preferred over HVFHV for the passthrough analysis because station
coordinates are precise (not zone-level), giving accurate OSRM benchmarks, and
cyclists are exposed to street-level conditions and make active route choices.

## Method

1. Download `202401-citibike-tripdata.csv.zip` from the Citi Bike S3 bucket.
2. Parse timestamps; compute `duration_sec`.
3. Filter to annual members (`member_casual == "member"`).
4. Filter to weekday trips (Mon–Fri) starting in peak hours (7–9am or 5–7pm).
5. Drop trips under 2 minutes or over 60 minutes, and same-station trips.
6. Sample down to `SAMPLE_SIZE` trips (default 15,000).

## Inputs

None — downloads directly from public S3.

## Outputs

- `citibike_trips.parquet`: Filtered sample of commute-hour trips with columns
  `ride_id`, `started_at`, `ended_at`, `duration_sec`, `start_station_id`,
  `end_station_id`, `start_lat`, `start_lng`, `end_lat`, `end_lng`.
