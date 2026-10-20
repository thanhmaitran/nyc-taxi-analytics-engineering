"""Benchmark full refresh vs incremental on the SAME data, and check the results are equivalent.

Scenario (files sorted by period; the last --new files are the "new months"):
  A  full refresh over all files                      -> db_full
  B  full refresh over the initial files, then an incremental run adding the new files -> db_incr
  C  replay: run the incremental again with no new data (must change nothing)
Equivalence = row-level EXCEPT in both directions over every core table and mart.
Results are written as JSON with timestamp, commit, scope and environment. Nothing is estimated.
"""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_raw_views  # noqa: E402
import pipeline_lib as lib  # noqa: E402

ROOT = lib.ROOT


def link_or_copy(src: Path, dst: Path) -> None:
    """Symlink when possible (no multi-GB copies); fall back to copy (e.g. Windows without privileges)."""
    try:
        dst.symlink_to(src.resolve())
    except (OSError, NotImplementedError):
        shutil.copy2(src, dst)


def stage_files(files: list[Path], target: Path) -> None:
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    for f in files:
        link_or_copy(f, target / f.name)  # raw files are never modified


def timed_run(label: str, raw: Path, db: Path, full_refresh: bool) -> dict:
    t0 = time.perf_counter()
    rows, nfiles = build_raw_views.build(raw, db, None)
    t_raw = time.perf_counter() - t0
    t1 = time.perf_counter()
    result = lib.dbt(["build"], db, full_refresh=full_refresh)
    t_dbt = time.perf_counter() - t1
    summary = lib.dbt_summary(result)
    if result.returncode != 0:
        raise RuntimeError(f"{label}: dbt build failed\n{result.stdout[-2000:]}")
    return {"label": label, "mode": "full_refresh" if full_refresh else "incremental",
            "raw_rows_visible": rows, "raw_files": nfiles,
            "build_raw_seconds": round(t_raw, 3), "dbt_seconds": round(t_dbt, 3),
            "wall_seconds": round(time.perf_counter() - t0, 3),
            "db_size_bytes": db.stat().st_size, "output_rows": lib.table_counts(db),
            "dbt_tests": summary}


def run(source_dir: Path, work: Path, new_files: int, last_n: int | None = None) -> dict:
    files = sorted(source_dir.glob("yellow_tripdata_*.parquet"))
    if last_n:
        files = files[-last_n:]  # benchmark scope: most recent N files
    if len(files) <= new_files:
        raise SystemExit(f"need more than {new_files} files, found {len(files)}")
    initial, added = files[:-new_files], files[-new_files:]
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)

    runs = []
    # A: full refresh, everything
    raw_all = work / "raw_all"; stage_files(files, raw_all)
    db_full = work / "full" / "wh.duckdb"; db_full.parent.mkdir()
    runs.append(timed_run("A_full_refresh_all", raw_all, db_full, True))

    # B: initial load, then add new months incrementally
    raw_inc = work / "raw_inc"; stage_files(initial, raw_inc)
    db_incr = work / "incr" / "wh.duckdb"; db_incr.parent.mkdir()
    runs.append(timed_run("B1_initial_load", raw_inc, db_incr, True))
    for f in added:
        link_or_copy(f, raw_inc / f.name)
    runs.append(timed_run("B2_incremental_add_new", raw_inc, db_incr, False))

    # C: replay with no new data
    before = lib.table_counts(db_incr)
    runs.append(timed_run("C_incremental_replay", raw_inc, db_incr, False))
    replay_idempotent = lib.table_counts(db_incr) == before

    # D: memory-bounded by-file loader (the path used on the full history) from scratch, same files
    db_byfile = work / "byfile" / "wh.duckdb"; db_byfile.parent.mkdir()
    t0 = time.perf_counter()
    manifest = {"files": {f.name: {"status": "validated"} for f in files}}
    load = lib.load_by_file(raw_all, db_byfile, manifest, full_refresh=True, log=lambda *_: None)
    if load.get("summary", {}).get("error", 1) != 0 or "error_output" in load:
        raise RuntimeError("by-file load failed\n" + load.get("error_output", ""))
    runs.append({"label": "D_by_file_full_load", "mode": "by_file", "raw_files": len(files),
                 "wall_seconds": round(time.perf_counter() - t0, 3), "dbt_seconds": None,
                 "db_size_bytes": db_byfile.stat().st_size, "output_rows": lib.table_counts(db_byfile),
                 "dbt_tests": load["summary"], "per_file_seconds": load["files"]})

    equivalence = lib.compare_databases(db_full, db_incr)
    equivalence_byfile = lib.compare_databases(db_full, db_byfile)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "commit": lib.git_sha(),
        "scope": {"source_dir": source_dir.name, "files": [f.name for f in files],
                  "initial": [f.name for f in initial], "added_incrementally": [f.name for f in added]},
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "cpu_count": __import__("os").cpu_count()},
        "runs": runs,
        "replay_idempotent": replay_idempotent,
        "equivalent_to_full_refresh": all(v["equal"] for v in equivalence.values()),
        "equivalence_detail": equivalence,
        "by_file_equivalent_to_single_pass": all(v["equal"] for v in equivalence_byfile.values()),
        "by_file_equivalence_detail": equivalence_byfile,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-dir", type=Path, default=ROOT / "data/raw/yellow_taxi")
    p.add_argument("--work-dir", type=Path, default=ROOT / "benchmarks/work")
    p.add_argument("--new-files", type=int, default=1)
    p.add_argument("--last-n", type=int, default=None, help="only benchmark the most recent N files")
    p.add_argument("--out", type=Path, default=None)
    a = p.parse_args()
    result = run(a.source_dir, a.work_dir, a.new_files, a.last_n)
    out = a.out or ROOT / "benchmarks" / f"benchmark_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    for r in result["runs"]:
        print(f"{r['label']:<26} {r['mode']:<13} wall={r['wall_seconds']:>8}s  rows(fact)={r['output_rows']['fact_trips']:,}")
    print("replay idempotent:", result["replay_idempotent"])
    print("incremental == full refresh:", result["equivalent_to_full_refresh"])
    print("by-file load == single-pass full refresh:", result["by_file_equivalent_to_single_pass"])
    print("written:", out)
    return 0 if (result["replay_idempotent"] and result["equivalent_to_full_refresh"]
                 and result["by_file_equivalent_to_single_pass"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
