---
title: Download U2 Data
tags: [Data, Download]
---

# Download U2 Data

This task downloads NYC Taxi & Limousine Commission (TLC) High Volume For-Hire Vehicle (HVFHV)
trip record data and the taxi zone boundary files needed to geocode trips.

HVFHV covers all app-dispatched rides (Uber, Lyft, Via, etc.) and records the pickup zone,
dropoff zone, and precise start and end timestamps for every trip.
This dataset serves as the ground truth for actual driver routes and trip durations.

## Data Sources

- **HVFHV trip records**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
  hosted on AWS S3 in Parquet format. One month of data is downloaded (January 2024).
- **Taxi zone lookup**: CSV mapping zone ID to borough and neighborhood name.
  Also from the TLC S3 bucket.
- **Taxi zone boundaries**: GeoJSON polygon boundaries for all 263 NYC taxi zones,
  from [NYC Open Data](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc).
  Zone centroids are computed from these polygons in `get_routes`.

## Outputs

- `hvfhv_trips.parquet`: One month of HVFHV trip records with pickup/dropoff zone IDs and timestamps.
- `taxi_zone_lookup.csv`: Zone ID to borough and neighborhood name mapping.
- `taxi_zones.geojson`: Polygon boundaries for all 263 NYC taxi zones.
