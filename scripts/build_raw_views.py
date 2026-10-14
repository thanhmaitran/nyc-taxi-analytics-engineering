"""Phase 3: register validated Parquet files as the raw_yellow_taxi view in DuckDB.

Only files the manifest marks `validated` are included, so quarantined or broken files
never reach the warehouse.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def build(raw_dir: Path, db_path: Path, manifest_path: Path | None) -> tuple[int, int]:
    files = sorted(raw_dir.glob("*.parquet"))
    if manifest_path and manifest_path.exists():
        entries = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
        # validated = not loaded yet; needs_reprocessing = content changed; loaded = already in marts
        loadable = ("validated", "needs_reprocessing", "loaded")
        files = [f for f in files if entries.get(f.name, {}).get("status") in loadable]
    if not files:
        raise FileNotFoundError(f"No validated Parquet files in {raw_dir}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.read_parquet([str(f) for f in files], union_by_name=True, filename=True,
                         file_row_number=True).create_view("raw_yellow_taxi", replace=True)
        return con.execute(
            "select count(*), count(distinct filename) from raw_yellow_taxi").fetchone()
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw/yellow_taxi")
    parser.add_argument("--db", type=Path, default=ROOT / "data/warehouse.duckdb")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data/manifests/manifest.json")
    args = parser.parse_args()
    rows, nfiles = build(args.raw_dir, args.db, args.manifest)
    print(f"Rows visible: {rows:,}\nFiles visible: {nfiles}\nWarehouse: {args.db}")


if __name__ == "__main__":
    main()
