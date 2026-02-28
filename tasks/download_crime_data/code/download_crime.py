"""Download NYPD complaint data from NYC Open Data via the Socrata API.

Filters to calendar year 2024 to align with the HVFHV trip data.
Only records with valid lat/lon are retained.
"""

import os

import requests

OUTPUT_DIR = "../output"

# NYPD Complaint Data Historic — Socrata dataset ID: qgea-i56i
SOCRATA_URL = "https://data.cityofnewyork.us/resource/qgea-i56i.csv"
LIMIT = 200_000


def download_crime_data() -> None:
    print(f"Downloading up to {LIMIT:,} NYPD complaint records (2024) ...")
    params = {
        "$limit": LIMIT,
        "$select": (
            "cmplnt_num,cmplnt_fr_dt,addr_pct_cd,law_cat_cd,"
            "boro_nm,latitude,longitude,ofns_desc"
        ),
        "$where": (
            "latitude IS NOT NULL"
            " AND longitude IS NOT NULL"
            " AND cmplnt_fr_dt >= '2024-01-01T00:00:00'"
            " AND cmplnt_fr_dt < '2025-01-01T00:00:00'"
        ),
        "$order": "cmplnt_fr_dt DESC",
    }
    response = requests.get(SOCRATA_URL, params=params, timeout=300)
    response.raise_for_status()

    out_path = f"{OUTPUT_DIR}/nyc_crime.csv"
    with open(out_path, "wb") as f:
        f.write(response.content)

    # Quick row count (subtract header)
    n_rows = response.content.count(b"\n") - 1
    print(f"Downloaded ~{n_rows:,} records. Saved to {out_path}")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_crime_data()
