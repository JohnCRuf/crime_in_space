"""Compute Chicago tract-to-tract driving times using OSRM table API."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests

OSRM_BASE_URL = "https://router.project-osrm.org"


def build_table_request(
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    mode: str,
    timeout: int,
    max_retries: int,
    pause_seconds: float,
) -> list[list[float | None]]:
    coords = [f"{row.longitude:.6f},{row.latitude:.6f}" for row in origins.itertuples(index=False)]
    coords.extend(f"{row.longitude:.6f},{row.latitude:.6f}" for row in destinations.itertuples(index=False))

    n_orig = len(origins)
    n_dest = len(destinations)

    sources = ";".join(str(i) for i in range(n_orig))
    destinations_idx = ";".join(str(n_orig + i) for i in range(n_dest))

    url = (
        f"{OSRM_BASE_URL}/table/v1/{mode}/"
        f"{';'.join(coords)}"
        f"?sources={sources}&destinations={destinations_idx}&annotations=duration"
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != "Ok":
                raise ValueError(f"OSRM returned code={payload.get('code')}")
            durations = payload.get("durations")
            if durations is None:
                raise ValueError("OSRM response missing durations")
            return durations
        except Exception as exc:  # noqa: BLE001
            if attempt == max_retries:
                raise
            sleep_seconds = pause_seconds * (2 ** (attempt - 1))
            print(f"  Request failed (attempt {attempt}/{max_retries}): {exc}; retrying in {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)

    raise RuntimeError("Unreachable retry state")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--centroids", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument("--mode", default="driving")
    parser.add_argument("--origin-batch-size", type=int, default=20)
    parser.add_argument("--destination-batch-size", type=int, default=80)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--pause-seconds", type=float, default=1.0)
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    centroids = pd.read_csv(args.centroids, dtype={"tract_geoid": str})
    centroids = centroids.sort_values("tract_geoid").reset_index(drop=True)

    origins = centroids.iloc[args.shard_index :: args.num_shards].reset_index(drop=True)
    destinations = centroids

    print(
        f"Shard {args.shard_index}/{args.num_shards}: "
        f"{len(origins):,} origin tracts x {len(destinations):,} destinations"
    )

    rows: list[pd.DataFrame] = []
    completed_origin = 0

    for o_start in range(0, len(origins), args.origin_batch_size):
        o_end = min(o_start + args.origin_batch_size, len(origins))
        o_chunk = origins.iloc[o_start:o_end].reset_index(drop=True)

        chunk_frames: list[pd.DataFrame] = []
        for d_start in range(0, len(destinations), args.destination_batch_size):
            d_end = min(d_start + args.destination_batch_size, len(destinations))
            d_chunk = destinations.iloc[d_start:d_end].reset_index(drop=True)

            durations = build_table_request(
                origins=o_chunk,
                destinations=d_chunk,
                mode=args.mode,
                timeout=args.timeout,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
            )

            block_rows: list[dict[str, object]] = []
            for i, origin in enumerate(o_chunk.itertuples(index=False)):
                for j, destination in enumerate(d_chunk.itertuples(index=False)):
                    seconds = durations[i][j] if durations[i][j] is not None else None
                    minutes = (seconds / 60.0) if seconds is not None else None
                    block_rows.append(
                        {
                            "origin_tract": origin.tract_geoid,
                            "destination_tract": destination.tract_geoid,
                            "drive_time_minutes": minutes,
                        }
                    )

            chunk_frames.append(pd.DataFrame(block_rows))
            time.sleep(args.pause_seconds)

        rows.append(pd.concat(chunk_frames, ignore_index=True))
        completed_origin += len(o_chunk)
        print(f"  Completed {completed_origin:,}/{len(origins):,} origins")

    result = pd.concat(rows, ignore_index=True)
    result.to_csv(out_path, index=False)
    print(f"Wrote {len(result):,} rows to {out_path}")


if __name__ == "__main__":
    main()
