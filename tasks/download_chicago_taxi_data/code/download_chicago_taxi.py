"""Download Chicago taxi trip records from Socrata with date filtering and paging."""

from __future__ import annotations

import os
from pathlib import Path

import requests

OUTPUT_DIR = Path("../output")

BASE_DOMAIN = os.getenv("CHICAGO_SODA_DOMAIN", "data.cityofchicago.org")
DATASET_ID = os.getenv("TAXI_DATASET_ID", "ajtu-isnz")
START_TS = os.getenv("TAXI_START_TIMESTAMP", "2024-01-01T00:00:00")
END_TS = os.getenv("TAXI_END_TIMESTAMP", "2025-01-01T00:00:00")
PAGE_SIZE = int(os.getenv("TAXI_PAGE_SIZE", "50000"))
REQUEST_TIMEOUT = int(os.getenv("TAXI_REQUEST_TIMEOUT", "180"))
MAX_ROWS = int(os.getenv("TAXI_MAX_ROWS", "0"))
APP_TOKEN = os.getenv("CHICAGO_SODA_APP_TOKEN", "")


def fetch_taxi_csv() -> Path:
    out_path = OUTPUT_DIR / "chicago_taxi_trips.csv"
    url = f"https://{BASE_DOMAIN}/resource/{DATASET_ID}.csv"

    headers: dict[str, str] = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    total_rows = 0
    offset = 0
    wrote_header = False

    with out_path.open("w", encoding="utf-8", newline="") as out_file:
        while True:
            if MAX_ROWS and total_rows >= MAX_ROWS:
                break

            page_limit = PAGE_SIZE
            if MAX_ROWS:
                page_limit = min(page_limit, MAX_ROWS - total_rows)

            params = {
                "$limit": page_limit,
                "$offset": offset,
                "$where": (
                    f"trip_start_timestamp >= '{START_TS}' "
                    f"AND trip_start_timestamp < '{END_TS}'"
                ),
                "$order": "trip_start_timestamp ASC",
            }

            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            text = resp.text.strip()
            if not text:
                break

            lines = text.splitlines()
            if len(lines) <= 1:
                break

            if wrote_header:
                out_file.write("\n".join(lines[1:]))
                out_file.write("\n")
            else:
                out_file.write(text)
                out_file.write("\n")
                wrote_header = True

            n_rows = len(lines) - 1
            total_rows += n_rows
            offset += n_rows
            print(f"Fetched {total_rows:,} rows from {DATASET_ID}")

            if n_rows < page_limit:
                break

    if total_rows == 0:
        raise ValueError(
            "No taxi rows downloaded. Check TAXI_DATASET_ID, timestamps, or API access settings."
        )

    return out_path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = fetch_taxi_csv()
    print(f"Saved taxi data to {out_path}")


if __name__ == "__main__":
    main()
