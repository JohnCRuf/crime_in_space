---
title: Plot Data
tags: [Analysis]
---

# Plot Data

This task produces binned scatterplots of mean trip duration excess by crime decile,
separately for the pickup zone and the dropoff zone, plus a side-by-side comparison.

The research question is whether for-hire vehicle drivers take longer-than-optimal routes
when operating in or through high-crime neighborhoods, consistent with deliberate route
deviation to avoid crime.
A positive `pct_time_increase` indicates the actual trip took longer than the OSRM estimate.

## Method

Trips are grouped by crime decile (1 = lowest crime, 10 = highest crime).
Within each decile, the mean and 95% confidence interval (±1.96 SE) of `pct_time_increase`
are plotted as a scatter with error bars.
Two groupings are produced: by pickup zone (`crime_decile`) and by dropoff zone
(`do_crime_decile`).

## Inputs

- `analysis_data.parquet`: Cleaned analysis dataset from `clean_data`.

## Outputs

- `binned_scatter_crime_decile.png`: Binned scatter by pickup zone crime decile.
- `binned_scatter_do_crime_decile.png`: Binned scatter by dropoff zone crime decile.
- `binned_scatter_pu_do_comparison.png`: Side-by-side comparison of PU and DO panels.
