"""Scale smoke test (SYNTHETIC data): does the by-file load stay inside a small memory limit?

Generates N monthly Parquet files of R rows each with DuckDB (TLC-like columns, a few injected defects),
loads them with the memory-bounded loader under DUCKDB_MEMORY_LIMIT, then reconciles row counts.
This shows memory behaviour and plumbing. It says nothing about real TLC data quality.

    python scripts/scale_smoke_test.py --files 6 --rows 3000000 --memory-limit 2GB
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import audit_raw_data  # noqa: E402
import pipeline_lib as lib  # noqa: E402
from ingestion import manifest as mf  # noqa: E402

ROOT = lib.ROOT


def generate(raw_dir: Path, files: int, rows: int) -> list[str]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set preserve_insertion_order=false")
    names = []
    for i in range(files):
        year, month = 2022 + i // 12, i % 12 + 1
        name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        start = f"{year}-{month:02d}-01"
        con.execute(f"""
            copy (
              with t as (
                select i, timestamp '{start}' + to_minutes(cast(random() * 27 * 24 * 60 as integer)) as pu,
                       cast(random() * 50 + 5 as decimal(10,2)) as fare
                from range({rows}) r(i))
              select cast(1 + i % 2 as int) as VendorID,
                     case when i % 1000 = 3 then null else pu end as tpep_pickup_datetime,
                     case when i % 1000 = 0 then pu - interval 5 minute
                          else pu + to_minutes(3 + cast(random() * 50 as integer)) end as tpep_dropoff_datetime,
                     cast(1 + i % 4 as bigint) as passenger_count,
                     case when i % 5000 = 1 then -1.0 else cast(random() * 15 as double) end as trip_distance,
                     cast(1 as bigint) as RatecodeID, 'N' as store_and_fwd_flag,
                     cast(1 + (i * 7) % 263 as int) as PULocationID,
                     cast(1 + (i * 13) % 263 as int) as DOLocationID,
                     cast(1 + i % 3 as bigint) as payment_type,
                     cast(fare as double) as fare_amount, 1.0 as extra, 0.5 as mta_tax,
                     cast(fare * 0.1 as double) as tip_amount, 0.0 as tolls_amount, 1.0 as improvement_surcharge,
                     cast(fare + 3 as double) as total_amount, 2.5 as congestion_surcharge, 0.0 as airport_fee
              from t) to '{raw_dir / name}' (format parquet)""")
        names.append(name)
        print(f"generated {name}", flush=True)
    con.close()
    return names


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--files", type=int, default=6)
    p.add_argument("--rows", type=int, default=3_000_000)
    p.add_argument("--memory-limit", default="2GB")
    p.add_argument("--work-dir", type=Path, default=ROOT / "data" / "scale_smoke")
    p.add_argument("--out", type=Path, default=None)
    a = p.parse_args()

    os.environ["DUCKDB_MEMORY_LIMIT"] = a.memory_limit
    shutil.rmtree(a.work_dir, ignore_errors=True)
    raw, man_dir, db = a.work_dir / "raw", a.work_dir / "manifests", a.work_dir / "wh.duckdb"
    man_dir.mkdir(parents=True)
    generate(raw, a.files, a.rows)

    audit = audit_raw_data.run(raw, man_dir, ROOT / "config/data_contract.yml",
                               ROOT / "data/reference/taxi_zone_lookup.csv", deep=False)
    manifest = mf.load(man_dir / "manifest.json")
    t0 = time.perf_counter()
    result = lib.load_by_file(raw, db, manifest, full_refresh=True, log=print)
    secs = round(time.perf_counter() - t0, 1)
    ok = "error_output" not in result and result.get("summary", {}).get("error", 1) == 0

    out = {"label": "synthetic-scale-smoke", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
           "memory_limit": a.memory_limit, "files": a.files, "rows_per_file": a.rows,
           "total_rows": a.files * a.rows, "audit_blocking": audit["blocking"],
           "load_seconds": secs, "ok": ok, "summary": result.get("summary"),
           "per_file_seconds": [(f["file"], f["seconds"]) for f in result["files"]],
           "error_output": result.get("error_output"), "db_size_bytes": db.stat().st_size if db.exists() else None}
    try:
        import resource
        out["peak_child_rss_mb"] = round(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024)
    except ImportError:
        out["peak_child_rss_mb"] = None
    if ok:
        con = duckdb.connect(str(db), read_only=True)
        rec = con.execute("select sum(source_rows), sum(valid_rows), sum(quarantined_rows), sum(fact_rows) "
                          "from main.mart_quality_reconciliation").fetchone()
        out["reconciliation"] = dict(zip(("source", "valid", "quarantined", "fact"), map(int, rec)))
        out["reconciled"] = (out["reconciliation"]["source"] == out["total_rows"]
                             and out["reconciliation"]["source"] == out["reconciliation"]["valid"] + out["reconciliation"]["quarantined"]
                             and out["reconciliation"]["valid"] == out["reconciliation"]["fact"])
        con.close()
    path = a.out or ROOT / "docs" / "results" / f"synthetic-scale-smoke_{datetime.now():%Y-%m-%d}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in out.items() if k != "per_file_seconds"}, indent=2, default=str))
    return 0 if ok and out.get("reconciled") else 1


if __name__ == "__main__":
    raise SystemExit(main())
