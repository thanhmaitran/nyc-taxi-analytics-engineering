"""Steps B-E in one command: run on the raw data, check quality, benchmark, write an evidence report.

    python scripts/verify_real_data.py --label real-2022-2026

Writes (nothing is estimated; every number comes from this run):
  reports/run_<ts>/run.log              full console log
  reports/run_<ts>/evidence.json        all measurements
  data/manifests/{manifest,raw_audit}.json
  docs/results/<label>_<date>.md        human-readable report (commit this one)
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import audit_raw_data  # noqa: E402
import benchmark  # noqa: E402
import build_raw_views  # noqa: E402
import pipeline_lib as lib  # noqa: E402
from ingestion import manifest as mf  # noqa: E402

ROOT = lib.ROOT


class Tee:
    def __init__(self, path: Path):
        self.fh = path.open("w", encoding="utf-8")

    def log(self, msg: str = "") -> None:
        line = f"{datetime.now():%H:%M:%S} {msg}"
        print(line, flush=True)
        self.fh.write(line + "\n")
        self.fh.flush()


def environment() -> dict:
    env = {"python": platform.python_version(), "platform": platform.platform(),
           "machine": platform.machine(), "cpu_count": os.cpu_count()}
    ram = lib.total_ram_bytes()
    env["ram_gb"] = round(ram / 1e9, 1) if ram else None
    env["duckdb_memory_limit"] = lib.memory_limit()
    import importlib.metadata as md
    env["packages"] = {p: md.version(p) for p in ("duckdb", "pyarrow", "dbt-core", "dbt-duckdb", "sqlfluff")}
    env["commit"] = lib.git_sha()
    return env


def q(con, sql):
    return con.execute(sql).fetchdf()


def quality_checks(db: Path, manifest: dict, raw_view_rows: int, per_file_checks: list[dict]) -> dict:
    """Row-count reconciliation across layers, computed from stored tables (no scan of the 197M-row views)."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        per_file = q(con, "select source_file, source_period, source_rows, valid_rows, quarantined_rows, "
                          "fact_rows, unmapped_pickup_rows, negative_distance_rows, negative_fare_rows, "
                          "exact_duplicate_rows, dropoff_before_pickup_rows, pickup_outside_period_rows, "
                          "quarantine_pct from main.mart_quality_reconciliation order by source_period")
        meta_rows = {n: e["row_count"] for n, e in manifest["files"].items()
                     if e.get("status") in ("loaded", "validated", "needs_reprocessing")}
        per_file["parquet_metadata_rows"] = per_file.source_file.map(meta_rows)
        layers = {
            "parquet_metadata (sum of footers)": int(sum(meta_rows.values())),
            "raw_view": int(raw_view_rows),
            "source rows seen by validation": int(per_file.source_rows.sum()),
            "valid": int(per_file.valid_rows.sum()),
            "quarantined (recon mart)": int(per_file.quarantined_rows.sum()),
            "quarantine_trips (table)": int(q(con, "select count(*) n from main.quarantine_trips").n[0]),
            "fact_trips (table)": int(q(con, "select count(*) n from main.fact_trips").n[0]),
        }
        reasons = q(con, "select quarantine_reason, count(*) as n from main.quarantine_trips group by 1 order by 2 desc")
        by_year = q(con, "select left(source_period, 4) as year, sum(source_rows) as source_rows, "
                         "sum(quarantined_rows) as quarantined, sum(unmapped_pickup_rows) as unmapped_pickup, "
                         "sum(negative_fare_rows) as negative_fare, sum(negative_distance_rows) as negative_distance "
                         "from main.mart_quality_reconciliation group by 1 order by 1")
        cbd = q(con, "select left(source_period, 4) as year, count(*) as rows, count(cbd_congestion_fee) as non_null_cbd "
                     "from main.fact_trips group by 1 order by 1")
        rng = q(con, "select min(full_date) as min_pickup, max(full_date) as max_pickup from main.dim_datetime")
        meta = per_file.parquet_metadata_rows
        checks = {
            "raw view rows == sum of Parquet footers": layers["raw_view"] == layers["parquet_metadata (sum of footers)"],
            "validation saw every source row": layers["source rows seen by validation"] == layers["raw_view"],
            "per-file source rows == per-file Parquet footer rows": bool((meta == per_file.source_rows).all()),
            "source == valid + quarantined": layers["source rows seen by validation"] == layers["valid"] + layers["quarantined (recon mart)"],
            "valid == fact_trips": layers["valid"] == layers["fact_trips (table)"],
            "quarantined == quarantine_trips table": layers["quarantined (recon mart)"] == layers["quarantine_trips (table)"],
            "trip_record_key unique within every file (fact and quarantine)":
                bool(per_file_checks) and all(c and c["ok"] for c in per_file_checks),
            "every manifest file loaded": set(meta_rows) == set(per_file.source_file),
        }
        return {"layers": layers, "checks": checks, "per_file": per_file.to_dict("records"),
                "quarantine_reasons": reasons.to_dict("records"), "by_year": by_year.to_dict("records"),
                "cbd_fee_coverage": cbd.to_dict("records"),
                "pickup_range": {k: str(v) for k, v in rng.iloc[0].items()}}
    finally:
        con.close()


def md_table(rows: list[dict], cols: list[str] | None = None) -> str:
    if not rows:
        return "_none_\n"
    cols = cols or list(rows[0])
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        def fmt(v):
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                return str(v)
            return f"{int(v):,}" if float(v).is_integer() else str(v)
        out.append("| " + " | ".join(fmt(r[c]) for c in cols) + " |")
    return "\n".join(out) + "\n"


def write_markdown(path: Path, ev: dict) -> None:
    a, qc, bm = ev["audit"], ev["quality"], ev.get("benchmark")
    L = [f"# Verification results: {ev['label']}", "",
         f"Generated {ev['generated_at_utc']} · commit `{ev['environment']['commit']}` · "
         f"all numbers are measured in this run.", ""]
    if ev["label"].startswith("synthetic"):
        L += ["> **Synthetic data, not TLC data.** It shows memory/scale behaviour and the report format; it says nothing about real data quality.", ""]
    env = ev["environment"]
    L += ["## 1. Environment", "",
          f"- Python {env['python']} on {env['platform']} ({env['machine']}), {env['cpu_count']} CPUs, RAM {env['ram_gb']} GB",
          "- Packages: " + ", ".join(f"{k} {v}" for k, v in env["packages"].items()), "",
          "## 2. Raw data audit", "",
          f"- Files: {a['file_count']}, total {a['total_bytes']/1e9:.2f} GB",
          f"- File status: {a['status_counts']}",
          f"- Duplicate periods: {a['duplicate_periods'] or 'none'}",
          f"- **Missing periods (gaps): {a['missing_periods'] or 'none'}**",
          f"- Zone lookup: {a['zone_lookup']['status']}",
          f"- Blocking problems: {a['blocking']}",
          f"- Date range of `dim_datetime` (pickup and dropoff hours of valid trips): {qc['pickup_range']['min_pickup']} to {qc['pickup_range']['max_pickup']}",
          "", "### Schema drift between consecutive periods", ""]
    if a["schema_drift"]:
        for d in a["schema_drift"]:
            L.append(f"- {d['from']} → {d['to']}: " + "; ".join(
                f"{c['change']} `{c['column']}`" + (f" ({c.get('from','')}→{c.get('to','')})" if c["change"] == "type_changed" else "")
                for c in d["changes"]))
    else:
        L.append("- none")
    L += ["", "### Schema findings (unknown columns, case variants, missing required)", "",
          md_table(a["schema_findings"], ["file", "kind", "column", "severity"]) if a["schema_findings"] else "_none_\n",
          "## 3. Warehouse build", "",
          f"- Load mode: {ev['build']['mode']} (one monthly file at a time, then dimensions, marts and tests); "
          f"{ev['build']['wall_seconds']} s wall-clock; DuckDB memory_limit {env['duckdb_memory_limit']}",
          f"- dbt results (sum of every dbt invocation): {ev['build']['dbt_tests']}",
          f"- DuckDB file: {(ev['build']['db_size_bytes'] or 0)/1e9:.2f} GB",
          f"- Slowest files: " + ", ".join(f"{x['file'][-15:-8]} {x['seconds']}s" for x in sorted(
              ev['build']['per_file_seconds'], key=lambda x: -x['seconds'])[:3]), "",
          "## 4. Data quality", "", "### Row reconciliation across layers", "",
          md_table([{"layer": k, "rows": v} for k, v in qc["layers"].items()]),
          md_table([{"check": k, "result": "PASS" if v else "FAIL"} for k, v in qc["checks"].items()]),
          "### Quarantine by reason", "", md_table(qc["quarantine_reasons"]),
          "### By year", "", md_table(qc["by_year"]),
          "### `cbd_congestion_fee` coverage in fact_trips (NULL before 2025 is expected)", "", md_table(qc["cbd_fee_coverage"]),
          "### Per file", "",
          md_table(qc["per_file"], ["source_period", "source_rows", "valid_rows", "quarantined_rows",
                                    "unmapped_pickup_rows", "negative_fare_rows", "negative_distance_rows", "quarantine_pct"])]
    if bm:
        L += ["## 5. Benchmark: full refresh vs incremental", "",
              f"Scope: {len(bm['scope']['files'])} files ({bm['scope']['files'][0]} … {bm['scope']['files'][-1]}); "
              f"added incrementally: {bm['scope']['added_incrementally']}. Same machine, same data, one run each "
              "(single measurements, no variance estimate).", "",
              md_table([{"run": r["label"], "mode": r["mode"], "wall_s": r["wall_seconds"], "dbt_s": r.get("dbt_seconds", "-"),
                         "raw_files": r.get("raw_files", "-"), "fact_rows": r["output_rows"]["fact_trips"],
                         "db_size_mb": round(r["db_size_bytes"] / 1e6, 1)} for r in bm["runs"]]),
              f"- Replay idempotent: **{bm['replay_idempotent']}**",
              f"- Incremental output equals full refresh (row-level EXCEPT on every core table and mart): "
              f"**{bm['equivalent_to_full_refresh']}**",
              f"- By-file loader output equals single-pass full refresh: **{bm['by_file_equivalent_to_single_pass']}**", ""]
    if not bm:
        L += ["## 5. Benchmark: full refresh vs incremental", "", "Not run in this report (`--skip-benchmark`).", ""]
    L += ["## 6. Reproduce", "", "```", ev["command"], "```", "",
          "Raw logs: `reports/run_*/run.log`; machine-readable: `reports/run_*/evidence.json`."]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(L) + "\n", encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--label", required=True, help="e.g. real-2022-2026; use a name starting with 'synthetic' for fixtures")
    p.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw/yellow_taxi")
    p.add_argument("--work-dir", type=Path, default=ROOT / "data")
    p.add_argument("--docs-dir", type=Path, default=ROOT / "docs/results")
    p.add_argument("--bench-files", type=int, default=6, help="benchmark only the most recent N files (0 = all)")
    p.add_argument("--bench-new", type=int, default=1)
    p.add_argument("--skip-benchmark", action="store_true")
    p.add_argument("--fast-audit", action="store_true")
    p.add_argument("--continue-on-error", action="store_true")
    a = p.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ROOT / "reports" / f"run_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log = Tee(run_dir / "run.log").log
    manifest_dir, db = a.work_dir / "manifests", a.work_dir / "warehouse.duckdb"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    ev = {"label": a.label, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
          "environment": environment(), "command": "python scripts/verify_real_data.py " + " ".join(sys.argv[1:])}

    log(f"[B1] audit {a.raw_dir}")
    audit = audit_raw_data.run(a.raw_dir, manifest_dir, ROOT / "config/data_contract.yml",
                               ROOT / "data/reference/taxi_zone_lookup.csv", deep=not a.fast_audit)
    ev["audit"] = audit
    log(f"     files={audit['file_count']} status={audit['status_counts']} gaps={audit['missing_periods']} "
        f"drift_events={len(audit['schema_drift'])} blocking={audit['blocking']}")
    for f in audit["files"]:
        if f["status"] != "ok":
            log(f"     PROBLEM {f['file_name']}: {f['status']} {f['error'] or ''}")
    if audit["blocking"] and not a.continue_on_error:
        log("[B1] blocking problem: fix/remove the files above or re-run with --continue-on-error")
        (run_dir / "evidence.json").write_text(json.dumps(ev, indent=2, default=str))
        return 1

    log(f"[B2] full load, one monthly file at a time (duckdb memory_limit={lib.memory_limit()})")
    manifest = mf.load(manifest_dir / "manifest.json")
    for entry in manifest["files"].values():          # a verification run always rebuilds from scratch
        if entry.get("status") in ("loaded", "needs_reprocessing"):
            entry["status"] = "validated"
    if db.exists():
        db.unlink()
    t0 = time.perf_counter()
    load = lib.load_by_file(a.raw_dir, db, manifest, full_refresh=True, log=log)
    secs = round(time.perf_counter() - t0, 2)
    (run_dir / "dbt_build.log").write_text(load.get("dbt_log_tail", "") + "\n" + load.get("error_output", ""),
                                           encoding="utf-8")
    summary = load.get("summary", {})
    ev["build"] = {"mode": "by_file", "wall_seconds": secs, "dbt_tests": summary,
                   "db_size_bytes": db.stat().st_size if db.exists() else None,
                   "per_file_seconds": load["files"], "final_build": load.get("final_build"),
                   "incremental_tests": load.get("incremental_tests")}
    log(f"     total {secs}s {summary}")
    if "error_output" in load or summary.get("error", 1) != 0:
        log("[B2] dbt failed:\n" + load.get("error_output", "")[-2000:])
        (run_dir / "evidence.json").write_text(json.dumps(ev, indent=2, default=str))
        return 1
    con = duckdb.connect(str(db), read_only=True)
    raw_rows = con.execute("select count(*) from main.raw_yellow_taxi").fetchone()[0]
    con.close()
    lib.mark_loaded(manifest)
    mf.save(manifest_dir / "manifest.json", manifest)

    log("[C] quality reconciliation")
    ev["quality"] = quality_checks(db, manifest, raw_rows, [f.get("integrity") for f in load["files"]])
    for k, v in ev["quality"]["checks"].items():
        log(f"     {'PASS' if v else 'FAIL'}  {k}")

    if not a.skip_benchmark:
        log(f"[D] benchmark (last {a.bench_files or 'all'} files, {a.bench_new} added incrementally)")
        ev["benchmark"] = benchmark.run(a.raw_dir, a.work_dir / "benchmark_work", a.bench_new, a.bench_files or None)
        for r in ev["benchmark"]["runs"]:
            log(f"     {r['label']:<26} {r['wall_seconds']}s")
        log(f"     replay idempotent={ev['benchmark']['replay_idempotent']} "
            f"incremental==full={ev['benchmark']['equivalent_to_full_refresh']} "
            f"by_file==single_pass={ev['benchmark']['by_file_equivalent_to_single_pass']}")

    (run_dir / "evidence.json").write_text(json.dumps(ev, indent=2, default=str), encoding="utf-8")
    report = a.docs_dir / f"{a.label}_{datetime.now():%Y-%m-%d}.md"
    write_markdown(report, ev)
    log(f"[E] report written: {report}")
    ok = all(ev["quality"]["checks"].values()) and ev["build"]["dbt_tests"].get("error", 1) == 0 and (
        a.skip_benchmark or (ev["benchmark"]["replay_idempotent"] and ev["benchmark"]["equivalent_to_full_refresh"]
                          and ev["benchmark"]["by_file_equivalent_to_single_pass"]))
    log("RESULT: " + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED, read the report"))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
