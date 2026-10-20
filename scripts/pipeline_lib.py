"""Shared orchestration helpers: run dbt against a chosen DuckDB file, manifest state, DB comparison."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import time

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
sys.path.insert(0, str(ROOT))

LOADABLE = ("validated", "needs_reprocessing", "loaded")

COMPARE_TABLES = [
    "fact_trips", "quarantine_trips", "dim_zone", "dim_payment_type", "dim_datetime",
    "mart_daily_operations", "mart_hourly_demand", "mart_zone_performance",
    "mart_quality_reconciliation",
]


def total_ram_bytes() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    try:  # Windows
        import ctypes

        class MemStatus(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
        status = MemStatus()
        status.dwLength = ctypes.sizeof(MemStatus)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return int(status.ullTotalPhys)
    except Exception:  # noqa: BLE001
        return None


def memory_limit() -> str:
    """DuckDB needs an absolute limit (no percentages). Default: 60% of RAM, never below 2GB."""
    if os.environ.get("DUCKDB_MEMORY_LIMIT"):
        return os.environ["DUCKDB_MEMORY_LIMIT"]
    ram = total_ram_bytes()
    return f"{max(2, int(ram * 0.6 / 1e9))}GB" if ram else "4GB"


def total_ram_bytes() -> int | None:
    """Physical RAM in bytes (Linux/macOS via sysconf, Windows via GlobalMemoryStatusEx)."""
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    try:
        import ctypes

        class Status(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong),
                        ("total_phys", ctypes.c_ulonglong), ("avail_phys", ctypes.c_ulonglong),
                        ("total_page", ctypes.c_ulonglong), ("avail_page", ctypes.c_ulonglong),
                        ("total_virt", ctypes.c_ulonglong), ("avail_virt", ctypes.c_ulonglong),
                        ("avail_ext", ctypes.c_ulonglong)]
        st = Status()
        st.length = ctypes.sizeof(Status)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))  # type: ignore[attr-defined]
        return int(st.total_phys)
    except Exception:  # noqa: BLE001
        return None


def memory_limit_gb(fraction: float = 0.6) -> int:
    """DuckDB memory limit: 60% of RAM (headroom for OS and dbt), at least 2 GB. Overflow spills to disk."""
    ram = total_ram_bytes()
    return max(2, int(ram * fraction / 1e9)) if ram else 8


def dbt_env(db_path: Path) -> dict:
    """Environment read by warehouse/profiles.yml. Names must match the env_var() calls there."""
    return {**os.environ,
            "DBT_DUCKDB_PATH": str(db_path),
            "DBT_DUCKDB_MEMORY_LIMIT": memory_limit(),
            "DBT_DUCKDB_TEMP": os.environ.get("DBT_DUCKDB_TEMP", str(db_path.parent / "duckdb_tmp")),
            "DBT_THREADS": os.environ.get("DBT_THREADS", "2"),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "False"}


def dbt(command: list[str], db_path: Path, variables: dict | None = None,
        full_refresh: bool = False, quiet: bool = True) -> subprocess.CompletedProcess:
    """Run `dbt <command>` inside warehouse/ against db_path. Uses the dbt in the active env."""
    args = [sys.executable, "-m", "dbt.cli.main", *command,
            "--project-dir", str(WAREHOUSE), "--profiles-dir", str(WAREHOUSE),
            "--target-path", str(db_path.parent / "dbt_target"),
            "--log-path", str(db_path.parent / "dbt_logs")]
    if variables:
        args += ["--vars", json.dumps(variables)]
    if full_refresh:
        args.append("--full-refresh")
    env = dbt_env(db_path)
    return subprocess.run(args, cwd=WAREHOUSE, env=env, capture_output=quiet, text=True)


def dbt_summary(result: subprocess.CompletedProcess) -> dict:
    """Parse the last 'Done. PASS=.. WARN=.. ERROR=..' line."""
    for line in reversed((result.stdout or "").splitlines()):
        if "Done. PASS=" in line:
            parts = dict(p.split("=") for p in line.split("Done.")[1].split() if "=" in p)
            return {k.lower(): int(v) for k, v in parts.items()} | {"returncode": result.returncode}
    return {"returncode": result.returncode}


def merge_summaries(parts: list[dict]) -> dict:
    keys = ("pass", "warn", "error", "skip", "no-op", "reused", "total")
    out = {k: sum(p.get(k, 0) for p in parts) for k in keys}
    out["returncode"] = max((p.get("returncode", 1) for p in parts), default=1)
    return out


def file_integrity(db_path: Path, source_file: str) -> dict:
    """Per-file uniqueness of trip_record_key in fact_trips and quarantine_trips (bounded by one month).

    Keys embed the file name, so uniqueness within each file implies uniqueness across the table.
    Rows are stored file by file, so the scan prunes to that file's row groups.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute(f"set memory_limit='{memory_limit()}'")
        out = {"file": source_file}
        for table in ("fact_trips", "quarantine_trips"):
            n, d = con.execute(f"select count(*), count(distinct trip_record_key) from main.{table} "
                               f"where source_file = ?", [source_file]).fetchone()
            out[table] = {"rows": n, "distinct_keys": d}
        out["ok"] = all(v["rows"] == v["distinct_keys"] for k, v in out.items() if k != "file" and k != "ok")
        return out
    finally:
        con.close()


INCREMENTAL_MODELS = ["fact_trips", "quarantine_trips", "mart_quality_reconciliation"]
PER_FILE_SELECT = ["taxi_zone_lookup_clean", "stg_yellow_taxi", "int_trip_validated", "int_trip_enriched", *INCREMENTAL_MODELS]


def load_by_file(raw_dir: Path, db_path: Path, manifest: dict, full_refresh: bool = False,
                 log=print, only: list[str] | None = None) -> dict:
    """Memory-bounded load. One monthly file at a time goes through the per-file incremental models, then one
    final dbt build creates dimensions and marts from the stored fact table.

    Why: running the validation window, uniqueness tests and distincts over all rows at once needs memory
    proportional to the whole history (197M rows ran out of memory). Per-file work is bounded by one month.
    Equivalence with a single-pass `dbt build` is verified by the integration tests and the benchmark.
    """
    import build_raw_views  # local import: scripts/ is on sys.path when called from scripts

    names = only or sorted(n for n, e in manifest["files"].items()
                           if e.get("status") in LOADABLE and (raw_dir / n).exists())
    if not full_refresh:
        pending = [n for n in names if manifest["files"][n].get("status") in ("validated", "needs_reprocessing")]
    else:
        pending = names
    reprocess = reprocess_list(manifest)
    result = {"mode": "by_file", "files": [], "summaries": []}
    started = time.perf_counter()

    # seed the zone lookup once; per-file runs do not execute seeds
    seed = dbt(["seed"], db_path, full_refresh=full_refresh)
    if seed.returncode != 0:
        raise RuntimeError("dbt seed failed\n" + (seed.stdout or "")[-2000:])

    for i, name in enumerate(pending):
        t0 = time.perf_counter()
        build_raw_views.build(raw_dir, db_path, None, only=[name])
        run = dbt(["run", "--select", *PER_FILE_SELECT], db_path,
                  variables={"reprocess_files": [n for n in reprocess if n == name]} if name in reprocess else None,
                  full_refresh=full_refresh and i == 0)
        summary = dbt_summary(run)
        secs = round(time.perf_counter() - t0, 2)
        integrity = file_integrity(db_path, name) if run.returncode == 0 else None
        result["files"].append({"file": name, "seconds": secs,
                                "ok": run.returncode == 0 and bool(integrity and integrity["ok"]),
                                "integrity": integrity})
        result["summaries"].append(summary)
        file_ok = result["files"][-1]["ok"]
        log(f"     [{i + 1}/{len(pending)}] {name}  {secs}s  {'ok' if file_ok else 'FAILED'}")
        if not file_ok and run.returncode == 0:
            run.stdout = f"duplicate trip_record_key detected in {name}: {integrity}"
            summary["error"] = summary.get("error", 0) + 1
        if not file_ok:
            result["error_output"] = (run.stdout or "")[-3000:]
            result["summary"] = merge_summaries(result["summaries"])
            return result

    # final stage: raw view over every loadable file, dimensions + marts + tests on the stored tables
    build_raw_views.build(raw_dir, db_path, None, only=names)
    no_full_scan = {"full_table_unique_tests": False}   # replaced by the per-file check above
    final = dbt(["build", "--exclude", *INCREMENTAL_MODELS], db_path, variables=no_full_scan)
    tests = dbt(["test", "--select", *INCREMENTAL_MODELS], db_path, variables=no_full_scan)
    result["final_build"] = dbt_summary(final)
    result["incremental_tests"] = dbt_summary(tests)
    result["summaries"] += [result["final_build"], result["incremental_tests"]]
    result["summary"] = merge_summaries(result["summaries"])
    result["seconds"] = round(time.perf_counter() - started, 2)
    result["dbt_log_tail"] = ((final.stdout or "")[-1500:] + (tests.stdout or "")[-1500:])
    if final.returncode != 0 or tests.returncode != 0:
        result["error_output"] = ((final.stdout or "") + (tests.stdout or ""))[-3000:]
    return result


def reprocess_list(manifest: dict) -> list[str]:
    return sorted(n for n, e in manifest["files"].items() if e.get("status") == "needs_reprocessing")


def mark_loaded(manifest: dict) -> None:
    for entry in manifest["files"].values():
        if entry.get("status") in ("validated", "needs_reprocessing"):
            entry["status"] = "loaded"


def table_counts(db_path: Path, tables: list[str] = COMPARE_TABLES) -> dict[str, int]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return {t: con.execute(f"select count(*) from main.{t}").fetchone()[0] for t in tables}
    finally:
        con.close()


def compare_databases(db_a: Path, db_b: Path, tables: list[str] = COMPARE_TABLES) -> dict[str, dict]:
    """Row-level EXCEPT in both directions. Equivalent iff both differences are 0."""
    con = duckdb.connect()
    con.execute(f"attach '{db_a}' as a (read_only)")
    con.execute(f"attach '{db_b}' as b (read_only)")
    report = {}
    for t in tables:
        a_minus_b = con.execute(f"select count(*) from (select * from a.main.{t} except select * from b.main.{t})").fetchone()[0]
        b_minus_a = con.execute(f"select count(*) from (select * from b.main.{t} except select * from a.main.{t})").fetchone()[0]
        rows_a = con.execute(f"select count(*) from a.main.{t}").fetchone()[0]
        rows_b = con.execute(f"select count(*) from b.main.{t}").fetchone()[0]
        report[t] = {"rows_a": rows_a, "rows_b": rows_b, "a_minus_b": a_minus_b, "b_minus_a": b_minus_a,
                     "equal": a_minus_b == 0 and b_minus_a == 0 and rows_a == rows_b}
    con.close()
    return report


def git_sha() -> str | None:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True)
        return out.stdout.strip() or None if out.returncode == 0 else None
    except FileNotFoundError:
        return None
