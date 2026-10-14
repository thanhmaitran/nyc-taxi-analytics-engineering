"""Shared orchestration helpers: run dbt against a chosen DuckDB file, manifest state, DB comparison."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

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
    env = {**os.environ, "DBT_DUCKDB_PATH": str(db_path)}
    return subprocess.run(args, cwd=WAREHOUSE, env=env, capture_output=quiet, text=True)


def dbt_summary(result: subprocess.CompletedProcess) -> dict:
    """Parse the last 'Done. PASS=.. WARN=.. ERROR=..' line."""
    for line in reversed((result.stdout or "").splitlines()):
        if "Done. PASS=" in line:
            parts = dict(p.split("=") for p in line.split("Done.")[1].split() if "=" in p)
            return {k.lower(): int(v) for k, v in parts.items()} | {"returncode": result.returncode}
    return {"returncode": result.returncode}


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
