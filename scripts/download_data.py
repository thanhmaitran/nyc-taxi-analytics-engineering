"""Download Yellow Taxi monthly Parquet files from TLC. Run this on your own machine.

Example: python scripts/download_data.py --start 2022-01 --end 2026-09
Months TLC has not published yet are reported as gaps, not errors.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion.download import download  # noqa: E402


def parse(value: str) -> tuple[int, int]:
    year, month = value.split("-")
    return int(year), int(month)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2022-01")
    parser.add_argument("--end", default="2026-12")
    parser.add_argument("--dest", type=Path, default=ROOT / "data/raw/yellow_taxi")
    args = parser.parse_args()

    (y, m), (end_y, end_m) = parse(args.start), parse(args.end)
    results: dict[str, list[str]] = {}
    while (y, m) <= (end_y, end_m):
        outcome = download(y, m, args.dest)
        results.setdefault(outcome, []).append(f"{y}-{m:02d}")
        print(f"{y}-{m:02d}: {outcome}", flush=True)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    print({k: len(v) for k, v in results.items()})
    return 1 if "failed" in results else 0


if __name__ == "__main__":
    raise SystemExit(main())
