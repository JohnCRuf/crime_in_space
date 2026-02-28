---
title: Download Crime Data
tags: [Data, Download]
---

# Download Crime Data

This task downloads NYPD complaint data from NYC Open Data via the Socrata Open Data API.
Each record represents a felony, misdemeanor, or violation reported to the NYPD with an
associated offense type, precinct, borough, and geographic coordinates.

The crime data is used to construct a zone-level crime rate that is merged onto HVFHV trips
in `clean_data` to test whether drivers detour around high-crime pickup zones.

## Data Source

[NYC Open Data — NYPD Complaint Data Historic](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

The Socrata API is queried without authentication (public dataset).
Records are filtered to calendar year 2024 to match the HVFHV trip data and limited to
complaints with valid latitude and longitude coordinates.

## Outputs

- `nyc_crime.csv`: Up to 200,000 NYPD complaint records with offense description,
  law category, borough, precinct, and WGS84 coordinates.
