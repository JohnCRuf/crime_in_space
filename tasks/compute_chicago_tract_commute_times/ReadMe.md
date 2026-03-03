---
title: Compute Chicago Tract Commute Times
tags: [Data, Compute, Commuting]
---

# Compute Chicago Tract Commute Times

This task computes a full Chicago tract-to-tract commuting-time matrix using tract centroids
and the public OSRM table API.

The implementation is sharded by origin tract so you can run shard targets in parallel with Make.

## Inputs

- Census TIGER/Line 2024 Illinois tract and place boundaries (direct download from `www2.census.gov`)
- OSRM table API (`https://router.project-osrm.org`) for driving travel times

## Outputs

- `chicago_tract_centroids.csv`: Chicago tract centroid coordinates used for routing
- `shards/commute_times_shard_*.csv`: per-shard OD travel times
- `chicago_tract_commute_times.csv`: merged full-city matrix

Output columns:

- `origin_tract`
- `destination_tract`
- `drive_time_minutes`

## Running

Default parallel run with 8 shards:

```bash
cd tasks/compute_chicago_tract_commute_times/code
make -j 8 all
```

You can tune shard and batch sizes:

```bash
make -j 12 SHARDS=12 ORIGIN_BATCH_SIZE=20 DESTINATION_BATCH_SIZE=80 all
```

## Notes

- This task computes **driving** commute times from centroid to centroid.
- Runtime depends on API latency/rate limiting; retries and backoff are built in.
- If a shard fails transiently, rerun `make` and completed shard files are reused.
