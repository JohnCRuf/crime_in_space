"""Prepare Chicago tract centroid coordinates from TIGER boundaries."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import requests

OUTPUT_DIR = Path("../output")
TEMP_DIR = Path("../temp")

TRACT_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_17_tract.zip"
PLACE_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/tl_2024_17_place.zip"
CHICAGO_PLACEFP = "14000"


def download_binary(url: str, out_path: Path) -> None:
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    tract_zip = TEMP_DIR / "tl_2024_17_tract.zip"
    place_zip = TEMP_DIR / "tl_2024_17_place.zip"

    print("Downloading TIGER tract/place boundaries ...")
    if not tract_zip.exists():
        download_binary(TRACT_ZIP_URL, tract_zip)
    if not place_zip.exists():
        download_binary(PLACE_ZIP_URL, place_zip)

    tracts = gpd.read_file(f"zip://{tract_zip}")
    places = gpd.read_file(f"zip://{place_zip}")

    chicago = places[places["PLACEFP"] == CHICAGO_PLACEFP].copy()
    if chicago.empty:
        raise ValueError("Chicago place boundary not found in TIGER place file.")

    if tracts.crs != chicago.crs:
        chicago = chicago.to_crs(tracts.crs)

    chicago_tracts = gpd.sjoin(
        tracts[["GEOID", "geometry"]],
        chicago[["geometry"]],
        how="inner",
        predicate="intersects",
    )
    chicago_tracts = chicago_tracts[["GEOID", "geometry"]].drop_duplicates("GEOID")

    # Compute centroids in projected CRS, then transform back to WGS84.
    projected = chicago_tracts.to_crs("EPSG:3435")
    projected["geometry"] = projected.geometry.centroid
    centroids = projected.to_crs("EPSG:4326")

    out = centroids.assign(
        tract_geoid=centroids["GEOID"].astype(str),
        latitude=centroids.geometry.y,
        longitude=centroids.geometry.x,
    )[["tract_geoid", "latitude", "longitude"]].sort_values("tract_geoid")

    out_path = OUTPUT_DIR / "chicago_tract_centroids.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out):,} tract centroids to {out_path}")


if __name__ == "__main__":
    main()
