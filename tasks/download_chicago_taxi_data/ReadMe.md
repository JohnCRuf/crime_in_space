---
title: Download Chicago Taxi Data
tags: [Data, Download, Taxi, Chicago]
---

# Download Chicago Taxi Data

This task downloads Chicago taxi trips from the City of Chicago Socrata API with
paging and a configurable timestamp window.

## Data Source

- City of Chicago Socrata API, default dataset:
  `ajtu-isnz` (Taxi Trips, 2024-)
- Optional legacy dataset:
  `wrvz-psew` (Taxi Trips, 2013-2023) via `TAXI_DATASET_ID`

## Output

- `chicago_taxi_trips.csv`

## Configuration

- `TAXI_DATASET_ID` (default `ajtu-isnz`)
- `TAXI_START_TIMESTAMP` (default `2024-01-01T00:00:00`)
- `TAXI_END_TIMESTAMP` (default `2025-01-01T00:00:00`)
- `TAXI_PAGE_SIZE` (default `50000`)
- `TAXI_MAX_ROWS` (default `0`, meaning no cap)
- `TAXI_REQUEST_TIMEOUT` (default `180` seconds)
- `CHICAGO_SODA_APP_TOKEN` (optional, recommended for higher API limits)
