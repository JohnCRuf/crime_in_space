"""Download CTPP flows and build Chicago tract-to-tract mode matrices.

This script targets the official CTPP Part 3 flow archive and extracts only rows needed
for Chicago-to-Chicago tract OD cells.
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pandas as pd
import requests

OUTPUT_DIR = Path("../output")
TEMP_DIR = Path("../temp")

# Optional manual override for direct URL/member inside the ZIP.
CTPP_PART3_URL = os.getenv("CTPP_PART3_URL", "")
CTPP_PART3_MEMBER = os.getenv("CTPP_PART3_MEMBER", "")

CTPP_PART3_CANDIDATES = [
    "https://downloads.transportation.org/CensusData/CTPP%202017-2021%20Part%203%20-%20Flow%20Tables.zip",
]

TRACT_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_17_tract.zip"
PLACE_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/tl_2024_17_place.zip"
CHICAGO_PLACEFP = "14000"

ORIGIN_ALIASES = [
    "res_geoid",
    "residence_geoid",
    "h_geoid",
    "h_geocode",
    "home_geoid",
    "origin_geoid",
    "geoid_from",
]
DEST_ALIASES = [
    "wrk_geoid",
    "work_geoid",
    "workplace_geoid",
    "w_geoid",
    "w_geocode",
    "dest_geoid",
    "destination_geoid",
    "geoid_to",
]
MODE_ALIASES = [
    "mode",
    "means_of_transportation",
    "means_of_transport",
    "transport_mode",
    "ttmode",
    "main_mode",
]
COUNT_ALIASES = ["workers", "estimate", "est", "value", "n", "flow", "count"]

DRIVE_MODE_PATTERN = re.compile(
    r"drive|drove|car|truck|van|taxi|motorcycle|motorbike|auto",
    re.IGNORECASE,
)
TRANSIT_MODE_PATTERN = re.compile(
    r"public transport|transit|bus|subway|streetcar|trolley|rail|ferry",
    re.IGNORECASE,
)
TRACT_PAIR_PATTERN = re.compile(r"(17\d{9})(17\d{9})")

# B302103 = Means of Transportation to Work (17) for each flow-pair GEOID.
ALL_LINES = {1}
DRIVE_LINES = {2, 3, 4, 5, 6, 7, 15, 16}
TRANSIT_LINES = {8, 9, 10, 11, 12}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.columns = [re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_") for c in clean.columns]
    return clean


def _find_column(columns: Iterable[str], aliases: list[str], regex_fallback: str) -> str:
    cols = list(columns)
    col_set = set(cols)
    for alias in aliases:
        if alias in col_set:
            return alias
    pattern = re.compile(regex_fallback)
    matches = [c for c in cols if pattern.search(c)]
    if not matches:
        raise ValueError(f"Missing required column. aliases={aliases}, fallback={regex_fallback}")
    return sorted(matches, key=len)[0]


def _clean_tract(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"(\d{11})", expand=False)


def _download_binary(url: str, out_path: Path) -> None:
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)


def _stream_download(url: str, out_path: Path) -> None:
    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", "0"))
        downloaded = 0
        step = 500 * 1_048_576
        next_mark = step
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=16 * 1_048_576):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded >= next_mark:
                    if total:
                        print(f"  {downloaded / 1_048_576:.0f} MB / {total / 1_048_576:.0f} MB")
                    else:
                        print(f"  {downloaded / 1_048_576:.0f} MB")
                    next_mark += step


def _load_chicago_tracts_from_tiger() -> set[str]:
    tracts_zip = TEMP_DIR / "tl_2024_17_tract.zip"
    places_zip = TEMP_DIR / "tl_2024_17_place.zip"

    print("Downloading TIGER tracts/place files for Illinois ...")
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
    geoid_set = set(chicago_tracts["GEOID"].astype(str).unique())
    if not geoid_set:
        raise ValueError("No Chicago tracts identified from TIGER files.")
    print(f"Loaded {len(geoid_set):,} Chicago tracts from TIGER")
    return geoid_set


def _download_ctpp_zip() -> Path:
    out_path = TEMP_DIR / "ctpp_part3.zip"
    if out_path.exists() and zipfile.is_zipfile(out_path):
        print(f"Using cached CTPP ZIP: {out_path}")
        return out_path

    urls = [CTPP_PART3_URL] if CTPP_PART3_URL else []
    urls.extend(CTPP_PART3_CANDIDATES)

    last_error = None
    for url in urls:
        try:
            _stream_download(url, out_path)
            if not zipfile.is_zipfile(out_path):
                raise ValueError("Downloaded file is not a valid ZIP archive.")
            return out_path
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"  Failed {url}: {exc}")
            if out_path.exists():
                out_path.unlink()

    raise ValueError(f"Could not download a valid CTPP Part 3 zip. Last error: {last_error}")


def _score_member(name: str) -> int:
    lower = name.lower()
    score = 0
    if lower.endswith(".zip"):
        score += 2
    if "b302103" in lower:
        score += 10
    if "_csv" in lower:
        score += 5
    if "/17/" in lower or "il_" in lower or "illinois" in lower:
        score += 4
    if "flow" in lower or "part3" in lower:
        score += 3
    if "tract" in lower:
        score += 2
    return score


def _pick_member(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        nested_zip_names = [
            n for n in zf.namelist() if n.lower().endswith(".zip") and ("/17/" in n or "/il_" in n.lower())
        ]
    if not nested_zip_names:
        raise ValueError("No Illinois nested ZIP members found inside CTPP archive.")

    if CTPP_PART3_MEMBER:
        if CTPP_PART3_MEMBER not in nested_zip_names:
            raise ValueError(f"CTPP_PART3_MEMBER not found in ZIP: {CTPP_PART3_MEMBER}")
        return CTPP_PART3_MEMBER

    return sorted(nested_zip_names, key=_score_member, reverse=True)[0]


def _extract_filtered_ctpp_rows(zip_path: Path, member: str, chicago_tracts: set[str]) -> pd.DataFrame:
    keep_frames: list[pd.DataFrame] = []

    with zipfile.ZipFile(zip_path) as outer:
        with outer.open(member) as nested_fh:
            nested_data = io.BytesIO(nested_fh.read())

    with zipfile.ZipFile(nested_data) as inner:
        inner_csv_names = [n for n in inner.namelist() if n.lower().endswith(".csv")]
        if not inner_csv_names:
            raise ValueError(f"No CSV found inside nested member: {member}")
        inner_csv = inner_csv_names[0]
        print(f"Using nested CSV: {inner_csv}")

        with inner.open(inner_csv) as csv_fh:
            for i, chunk in enumerate(
                pd.read_csv(
                    csv_fh,
                    dtype=str,
                    low_memory=False,
                    skiprows=2,
                    chunksize=250_000,
                )
            ):
                chunk = _normalize_columns(chunk)
                required = {"geoid", "lineno", "est"}
                missing = required - set(chunk.columns)
                if missing:
                    raise ValueError(f"Missing required columns in selected table: {sorted(missing)}")

                pairs = chunk["geoid"].astype(str).str.extract(TRACT_PAIR_PATTERN)
                chunk["origin"] = pairs[0]
                chunk["destination"] = pairs[1]
                chunk = chunk.dropna(subset=["origin", "destination"])
                if chunk.empty:
                    continue

                chunk = chunk[
                    chunk["origin"].isin(chicago_tracts) & chunk["destination"].isin(chicago_tracts)
                ].copy()
                if chunk.empty:
                    continue

                chunk["lineno"] = pd.to_numeric(chunk["lineno"], errors="coerce")
                chunk["count"] = (
                    chunk["est"]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("+/-", "", regex=False)
                )
                chunk["count"] = pd.to_numeric(chunk["count"], errors="coerce").fillna(0)
                keep_frames.append(chunk[["origin", "destination", "lineno", "count"]])

                if (i + 1) % 5 == 0:
                    print(f"  Processed {(i + 1) * 250_000:,}+ rows from {inner_csv}")

    if not keep_frames:
        raise ValueError("No Chicago-to-Chicago tract rows were found in selected CTPP member.")

    return pd.concat(keep_frames, ignore_index=True)


def _build_mode_matrix(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["origin", "destination"], dropna=False)["count"]
        .sum()
        .reset_index()
        .rename(columns={"origin": "origin_tract", "destination": "destination_tract", "count": "commuters"})
        .sort_values(["origin_tract", "destination_tract"])
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    chicago_tracts = _load_chicago_tracts_from_tiger()
    ctpp_zip_path = _download_ctpp_zip()
    member = _pick_member(ctpp_zip_path)
    print(f"Using CTPP member: {member}")

    filtered = _extract_filtered_ctpp_rows(ctpp_zip_path, member, chicago_tracts)
    all_modes = _build_mode_matrix(filtered[filtered["lineno"].isin(ALL_LINES)])
    drive = _build_mode_matrix(filtered[filtered["lineno"].isin(DRIVE_LINES)])
    transit = _build_mode_matrix(filtered[filtered["lineno"].isin(TRANSIT_LINES)])

    all_modes.to_csv(OUTPUT_DIR / "ctpp_chicago_all_modes_matrix.csv", index=False)
    drive.to_csv(OUTPUT_DIR / "ctpp_chicago_drive_matrix.csv", index=False)
    transit.to_csv(OUTPUT_DIR / "ctpp_chicago_transit_matrix.csv", index=False)

    print(f"Wrote {len(all_modes):,} rows to ctpp_chicago_all_modes_matrix.csv")
    print(f"Wrote {len(drive):,} rows to ctpp_chicago_drive_matrix.csv")
    print(f"Wrote {len(transit):,} rows to ctpp_chicago_transit_matrix.csv")


if __name__ == "__main__":
    main()
