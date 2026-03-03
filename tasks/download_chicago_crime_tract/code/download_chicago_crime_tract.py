"""Download and aggregate Chicago crime data to tract-level annual counts."""

from __future__ import annotations

import io
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

OUTPUT_DIR = Path("../output")
TEMP_DIR = Path("../temp")

CHICAGO_CRIME_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.csv"
CRIME_YEAR = int(os.getenv("CRIME_YEAR", "2024"))
PAGE_SIZE = int(os.getenv("CRIME_PAGE_SIZE", "50000"))

TRACT_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_17_tract.zip"
PLACE_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/tl_2024_17_place.zip"
CHICAGO_PLACEFP = "14000"


def _download_binary(url: str, out_path: Path) -> None:
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)


def load_chicago_tracts() -> gpd.GeoDataFrame:
    tracts_zip = TEMP_DIR / "tl_2024_17_tract.zip"
    places_zip = TEMP_DIR / "tl_2024_17_place.zip"

    print("Downloading TIGER tract/place boundaries ...")
    _download_binary(TRACT_ZIP_URL, tracts_zip)
    _download_binary(PLACE_ZIP_URL, places_zip)

    tracts = gpd.read_file(f"zip://{tracts_zip}")
    places = gpd.read_file(f"zip://{places_zip}")

    chicago = places[places["PLACEFP"] == CHICAGO_PLACEFP].copy()
    if chicago.empty:
        raise ValueError("Chicago place boundary (PLACEFP=14000) not found in TIGER place file.")

    if tracts.crs != chicago.crs:
        chicago = chicago.to_crs(tracts.crs)

    chicago_tracts = gpd.sjoin(
        tracts[["GEOID", "geometry"]],
        chicago[["geometry"]],
        how="inner",
        predicate="intersects",
    )
    chicago_tracts = chicago_tracts[["GEOID", "geometry"]].drop_duplicates("GEOID")
    print(f"Loaded {len(chicago_tracts):,} Chicago tracts")
    return chicago_tracts


def download_crime_rows() -> pd.DataFrame:
    print(f"Downloading Chicago crimes for {CRIME_YEAR} in pages of {PAGE_SIZE:,} ...")
    frames: list[pd.DataFrame] = []
    offset = 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$select": "id,date,primary_type,description,latitude,longitude",
            "$where": (
                "latitude IS NOT NULL"
                " AND longitude IS NOT NULL"
                f" AND date >= '{CRIME_YEAR}-01-01T00:00:00'"
                f" AND date < '{CRIME_YEAR + 1}-01-01T00:00:00'"
            ),
            "$order": "date ASC",
        }
        resp = requests.get(CHICAGO_CRIME_URL, params=params, timeout=600)
        resp.raise_for_status()
        chunk = pd.read_csv(io.StringIO(resp.text), dtype=str)

        if chunk.empty:
            break

        frames.append(chunk)
        offset += len(chunk)
        print(f"  Pulled {offset:,} rows so far")

        if len(chunk) < PAGE_SIZE:
            break

    if not frames:
        raise ValueError("No crime rows downloaded. Check year or API endpoint.")

    return pd.concat(frames, ignore_index=True)


def aggregate_by_tract(crime_df: pd.DataFrame, chicago_tracts: gpd.GeoDataFrame) -> pd.DataFrame:
    crime = crime_df.copy()
    crime["longitude"] = pd.to_numeric(crime["longitude"], errors="coerce")
    crime["latitude"] = pd.to_numeric(crime["latitude"], errors="coerce")
    crime = crime.dropna(subset=["longitude", "latitude"])

    points = gpd.GeoDataFrame(
        crime,
        geometry=gpd.points_from_xy(crime["longitude"], crime["latitude"]),
        crs="EPSG:4326",
    )

    if chicago_tracts.crs != points.crs:
        chicago_tracts = chicago_tracts.to_crs(points.crs)

    joined = gpd.sjoin(
        points,
        chicago_tracts[["GEOID", "geometry"]],
        how="inner",
        predicate="within",
    )

    agg = (
        joined.groupby("GEOID", dropna=False)
        .size()
        .reset_index(name="crime_count")
        .rename(columns={"GEOID": "tract_geoid"})
        .sort_values("tract_geoid")
    )
    agg["year"] = CRIME_YEAR
    return agg[["year", "tract_geoid", "crime_count"]]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    chicago_tracts = load_chicago_tracts()
    crime = download_crime_rows()

    raw_out = OUTPUT_DIR / "chicago_crime_raw.csv"
    crime.to_csv(raw_out, index=False)

    agg = aggregate_by_tract(crime, chicago_tracts)
    agg_out = OUTPUT_DIR / "chicago_crime_by_tract.csv"
    agg.to_csv(agg_out, index=False)

    print(f"Saved {len(crime):,} raw records to {raw_out}")
    print(f"Saved {len(agg):,} tracts to {agg_out}")


if __name__ == "__main__":
    main()
