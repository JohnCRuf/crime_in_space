"""Download NYC TLC High Volume For-Hire Vehicle (HVFHV) trip data and taxi zone files.

HVFHV covers all app-dispatched rides (Uber, Lyft, etc.) with pickup/dropoff zone IDs
and precise timestamps for each trip.
"""

import io
import os
import zipfile

import geopandas as gpd
import requests

OUTPUT_DIR = "../output"

# One month of HVFHV data from the TLC S3 bucket (January 2024)
HVFHV_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# TLC-hosted shapefile zip (more reliable than NYC Open Data GeoJSON endpoint)
ZONE_SHAPE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


def download_file(url: str, path: str) -> None:
    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=65_536):
            f.write(chunk)
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"  Saved {size_mb:.1f} MB to {path}")


def download_zone_geojson(url: str, out_path: str) -> None:
    """Download TLC taxi zone shapefile zip and convert to GeoJSON."""
    print(f"Downloading {url} ...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        # Read whichever .shp is in the archive
        shp_name = next(n for n in zf.namelist() if n.endswith(".shp"))
        zf.extractall(OUTPUT_DIR)
    gdf = gpd.read_file(f"{OUTPUT_DIR}/{shp_name}")
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"  Saved taxi zones GeoJSON to {out_path}")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_file(HVFHV_URL, f"{OUTPUT_DIR}/hvfhv_trips.parquet")
    download_file(ZONE_LOOKUP_URL, f"{OUTPUT_DIR}/taxi_zone_lookup.csv")
    download_zone_geojson(ZONE_SHAPE_URL, f"{OUTPUT_DIR}/taxi_zones.geojson")
    print("All downloads complete.")
