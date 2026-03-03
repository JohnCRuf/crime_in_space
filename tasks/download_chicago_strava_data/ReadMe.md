---
title: Download Chicago Strava Metro Exports
tags: [Data, Download, Strava, Metro, Chicago]
---

# Download Chicago Strava Metro Exports

This task downloads Strava Metro export ZIP files after they are queued in Metroview,
then extracts files and writes a manifest.

Official Strava Metro workflow (as of September-December 2025 docs):

1. In Metroview Map, create an export (Edge Counts or Origins & Destinations).
2. Click **Save to Data**.
3. Wait until the Data page shows an orange **Download** button.
4. Copy the ready download URL(s).
5. Run this task with those URL(s).

References:
- Data Page: https://stravametro.zendesk.com/hc/en-us/articles/4411059883799-Data-Page
- Edge Counts Export: https://stravametro.zendesk.com/hc/en-us/articles/360051202734-Edge-Counts-Data-Export-and-Download
- Origins & Destinations Export: https://stravametro.zendesk.com/hc/en-us/articles/8187514314263-Origins-Destinations-Data-Export-and-Download

## Outputs

- `downloads/*.zip`: downloaded Strava Metro export archives
- `extracted/<zip_stem>/*`: extracted CSV/shapefile/txt contents
- `strava_metro_download_manifest.csv`: row-level manifest of extracted files

## Configuration

Required (one of):
- `STRAVA_METRO_DOWNLOAD_URLS`: comma-separated URL list
- `STRAVA_METRO_URLS_FILE`: text file with one URL per line

Optional:
- `STRAVA_METRO_BEARER_TOKEN`: bearer token if your URL requires auth
- `STRAVA_METRO_COOKIE`: cookie header string for authenticated session links
- `STRAVA_REQUEST_TIMEOUT`: request timeout in seconds (default `300`)
- `STRAVA_METRO_USER_AGENT`: user-agent header (default `crime-in-space/1.0`)

## Example

```bash
make -C tasks/download_chicago_strava_data/code all \
  STRAVA_METRO_DOWNLOAD_URLS='https://.../download1.zip,https://.../download2.zip'
```
