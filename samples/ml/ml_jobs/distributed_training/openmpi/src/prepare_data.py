#!/usr/bin/env python3
"""Stream and partition the HIGGS dataset without downloading the full archive."""

from __future__ import annotations

import argparse
import gzip
import json
import urllib.request
from pathlib import Path


DEFAULT_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--rank", type=int, required=True)
    parser.add_argument("--world-size", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 0 <= args.rank < args.world_size:
        raise ValueError("rank must be in [0, world_size).")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        args.url,
        headers={"User-Agent": "snowflake-ml-jobs-distributed-sample/1.0"},
    )
    local_rows = 0
    with urllib.request.urlopen(request, timeout=120) as response:
        with gzip.GzipFile(fileobj=response) as decompressed:
            with args.output.open("wb") as output:
                for row_index, row in enumerate(decompressed):
                    if row_index >= args.max_rows:
                        break
                    if row_index % args.world_size == args.rank:
                        if row.count(b",") != 28:
                            raise ValueError(
                                f"Unexpected HIGGS row shape at row {row_index}."
                            )
                        output.write(row)
                        local_rows += 1

    metadata = {
        "global_row_limit": args.max_rows,
        "local_rows": local_rows,
        "rank": args.rank,
        "source": args.url,
        "world_size": args.world_size,
    }
    args.output.with_suffix(".json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"[data] wrote {local_rows} rows to {args.output}", flush=True)


if __name__ == "__main__":
    main()
