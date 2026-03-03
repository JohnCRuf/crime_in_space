---
title: Download Chicago Crime Data By Tract
tags: [Data, Download, Crime]
---

# Download Chicago Crime Data By Tract

This task downloads Chicago crime incidents from the City of Chicago open data Socrata API,
geocodes incidents to Chicago census tracts using Census TIGER boundaries, and aggregates
them to annual tract-level counts.

## Data Source

- **Crimes - 2001 to Present** (Chicago Open Data):
  `https://data.cityofchicago.org/resource/ijzp-q8t2.csv`
- **Census TIGER/Line 2024** tract and place shapefiles:
  `https://www2.census.gov/geo/tiger/TIGER2024/`

## Outputs

- `chicago_crime_raw.csv`: Raw filtered incident records for the selected year
- `chicago_crime_by_tract.csv`: Tract-level annual crime counts

## Configuration

- `CRIME_YEAR` (default `2024`)
- `CRIME_PAGE_SIZE` (default `50000`)
