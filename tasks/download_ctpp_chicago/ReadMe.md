---
title: Download CTPP Chicago OD Matrices
tags: [Data, Download, Commuting]
---

# Download CTPP Chicago OD Matrices

This task downloads Census Transportation Planning Products (CTPP) Part 3 flow data and
builds three tract-to-tract commuting origin/destination (OD) matrices for the City of Chicago:

- all commute modes,
- drive-based modes,
- public-transit modes.

The task is designed as a direct-download, script-driven workflow with deterministic outputs.

## Data Sources

- **CTPP 2017-2021 Part 3** (AASHTO/Census direct download):
  `CTPP_PART3_URL` (optional override) or built-in candidate URLs under `downloads.transportation.org/CensusData`.
- **Census TIGER/Line 2024**:
  Illinois tract and place shapefiles from `www2.census.gov` used to construct the set of Chicago tracts
  (`PLACEFP=14000`) in a transparent, reproducible way.

## Outputs

- `ctpp_chicago_all_modes_matrix.csv`
- `ctpp_chicago_drive_matrix.csv`
- `ctpp_chicago_transit_matrix.csv`

Each output contains:

- `origin_tract` (11-digit tract GEOID)
- `destination_tract` (11-digit tract GEOID)
- `commuters` (summed worker count)

## Notes

- The official Part 3 ZIP is very large (about 19 GB as of March 2026); this task streams
  it to `../temp/ctpp_part3.zip` and reuses the cached file on reruns.
- If CTPP archive paths change, set `CTPP_PART3_URL` in your environment and rerun.
- If automatic table selection picks the wrong CSV member, set `CTPP_PART3_MEMBER` to the
  exact member name inside the ZIP.
- Mode classification is text-pattern based and can be tightened later if you want exact CTPP code mappings.
