"""Merge tract commute-time shard CSVs into a single matrix."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    shard_dir = Path(args.shard_dir)
    shard_files = sorted(shard_dir.glob("commute_times_shard_*.csv"))
    if not shard_files:
        raise ValueError(f"No shard files found in {shard_dir}")

    frames = [pd.read_csv(path, dtype={"origin_tract": str, "destination_tract": str}) for path in shard_files]
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["origin_tract", "destination_tract"]).reset_index(drop=True)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"Merged {len(shard_files)} shard files")
    print(f"Wrote {len(out):,} rows to {out_path}")


if __name__ == "__main__":
    main()
