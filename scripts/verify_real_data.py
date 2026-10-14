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
    try:
        env["ram_gb"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 1)
    except (ValueError, OSError, AttributeError):
        env["ram_gb"] = None
    import importlib.metadata as md
    env["packages"] = {p: md.version(p) for p in ("duckdb", "pyarrow", "dbt-core", "dbt-duckdb", "sqlfluff")}
    env["commit"] = lib.git_sha()
    return env


def q(con, sql):
    return con.execute(sql).fetchdf()


def quality_checks(db: Path, manifest: dict) -> dict:
    """Row-count reconciliation across layers + quarantine + coverage, all from the built warehouse."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        layers = {
            "raw_view": q(con, "select count(*) n from main.raw_yellow_taxi").n[0],
            "stg_yellow_taxi": q(con, "select count(*) n from main.stg_yellow_taxi").n[0],
            "int_trip_validated": q(con, "select count(*) n from main.int_trip_validated").n[0],
            "valid": q(con, "select count(*) n from main.int_trip_validated where is_valid").n[0],
            "quarantine_trips": q(con, "select count(*) n from main.quarantine_trips").n[0],
            "fact_trips": q(con, "select count(*) n from main.fact_trips").n[0],
        }
        layers = {k: int(v) for k, v in layers.items()}
        meta_rows = {n: e["row_count"] for n, e in manifest["files"].items()
                     if e.get("status") in ("loaded", "validated", "needs_reprocessing")}
        per_file = q(con, "select source_file, source_period, source_rows, valid_rows, quarantined_rows, "
                          "fact_rows, unmapped_pickup_rows, negative_distance_rows, negative_fare_rows, "
                          "exact_duplicate_rows, dropoff_before_pickup_rows, pickup_outside_period_rows, "
                          "quarantine_pct from main.mart_quality_reconciliation order by source_period")
        per_file["parquet_metadata_rows"] = per_file.source_file.map(meta_rows)
        reasons = q(con, "select quarantine_reason, count(*) as n from main.quarantine_trips group by 1 order by 2 desc")
        by_year = q(con, "select left(source_period, 4) as year, sum(source_rows) as source_rows, "
                         "sum(quarantined_rows) as quarantined, sum(unmapped_pickup_rows) as unmapped_pickup, "
                         "sum(negative_fare_rows) as negative_fare, sum(negative_distance_rows) as negative_distance "
                         "from main.mart_quality_reconciliation group by 1 order by 1")
        cbd = q(con, "select left(source_period, 4) as year, count(*) as rows, count(cbd_congestion_fee) as non_null_cbd "
                     "from main.fact_trips group by 1 order by 1")
        rng = q(con, "select min(pickup_datetime) as min_pickup, max(pickup_datetime) as max_pickup "
                     "from main.int_trip_enriched")
        checks = {
            "raw_view_rows == sum(parquet metadata rows)": layers["raw_view"] == sum(meta_rows.values()),
            "raw == staging": layers["raw_view"] == layers["stg_yellow_taxi"],
            "staging == validated": layers["stg_yellow_taxi"] == layers["int_trip_validated"],
            "validated == valid + quarantine": layers["int_trip_validated"] == layers["valid"] + layers["quarantine_trips"],
            "valid == fact": layers["valid"] == layers["fact_trips"],
            "per-file parquet rows == per-file source rows":
                bool((per_file.parquet_metadata_rows == per_file.source_rows).all()),
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
        out.append("| " + " | ".join(f"{r[c]:,}" if isinstance(r[c], (int, float)) and not isinstance(r[c], bool)
                                      and float(r[c]).is_integer() else str(r[c]) for c in cols) + " |")
    return "\n".join(out) + "\n"


def write_markdown(path: Path, ev: dict) -> None:
    a, qc, bm = ev["audit"], ev["quality"], ev.get("benchmark")
    L = [f"# Verification results: {ev['label']}", "",
         f"Generated {ev['generated_at_utc']} · commit `{ev['environment']['commit']}` · "
         f"all numbers are measured in this run.", ""]
    if ev["label"].startswith("synthetic"):
        L += ["> **These are synthetic fixtures, not TLC data. They show the report format only.**", ""]
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
          f"- Pickup timestamp range after validation: {qc['pickup_range']['min_pickup']} to {qc['pickup_range']['max_pickup']}",
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
          f"- Full-refresh `dbt build`: {ev['build']['dbt_seconds']} s wall-clock; tests: {ev['build']['dbt_tests']}",
          f"- DuckDB file: {ev['build']['db_size_bytes']/1e9:.2f} GB", "",
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
              md_table([{"run": r["label"], "mode": r["mode"], "wall_s": r["wall_seconds"], "dbt_s": r["dbt_seconds"],
                         "raw_rows": r["raw_rows_visible"], "fact_rows": r["output_rows"]["fact_trips"],
                         "db_size_mb": round(r["db_size_bytes"] / 1e6, 1)} for r in bm["runs"]]),
              f"- Replay idempotent: **{bm['replay_idempotent']}**",
              f"- Incremental output equals full refresh (row-level EXCEPT on every core table and mart): "
              f"**{bm['equivalent_to_full_refresh']}**", ""]
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

    log("[B2] build raw view")
    rows, nfiles = build_raw_views.build(a.raw_dir, db, manifest_dir / "manifest.json")
    log(f"     rows={rows:,} files={nfiles}")

    log("[B3] dbt build --full-refresh (timed)")
    t0 = time.perf_counter()
    res = lib.dbt(["build"], db, full_refresh=True)
    secs = round(time.perf_counter() - t0, 2)
    (run_dir / "dbt_build.log").write_text(res.stdout or "", encoding="utf-8")
    summary = lib.dbt_summary(res)
    ev["build"] = {"dbt_seconds": secs, "dbt_tests": summary, "db_size_bytes": db.stat().st_size}
    log(f"     {secs}s {summary}")
    if res.returncode != 0:
        log("[B3] dbt failed; see dbt_build.log in the run folder")
        (run_dir / "evidence.json").write_text(json.dumps(ev, indent=2, default=str))
        return 1
    manifest = mf.load(manifest_dir / "manifest.json")
    lib.mark_loaded(manifest)
    mf.save(manifest_dir / "manifest.json", manifest)

    log("[C] quality reconciliation")
    ev["quality"] = quality_checks(db, manifest)
    for k, v in ev["quality"]["checks"].items():
        log(f"     {'PASS' if v else 'FAIL'}  {k}")

    if not a.skip_benchmark:
        log(f"[D] benchmark (last {a.bench_files or 'all'} files, {a.bench_new} added incrementally)")
        ev["benchmark"] = benchmark.run(a.raw_dir, a.work_dir / "benchmark_work", a.bench_new, a.bench_files or None)
        for r in ev["benchmark"]["runs"]:
            log(f"     {r['label']:<26} {r['wall_seconds']}s")
        log(f"     replay idempotent={ev['benchmark']['replay_idempotent']} "
            f"incremental==full={ev['benchmark']['equivalent_to_full_refresh']}")

    (run_dir / "evidence.json").write_text(json.dumps(ev, indent=2, default=str), encoding="utf-8")
    report = a.docs_dir / f"{a.label}_{datetime.now():%Y-%m-%d}.md"
    write_markdown(report, ev)
    log(f"[E] report written: {report}")
    ok = all(ev["quality"]["checks"].values()) and ev["build"]["dbt_tests"].get("error", 1) == 0 and (
        a.skip_benchmark or (ev["benchmark"]["replay_idempotent"] and ev["benchmark"]["equivalent_to_full_refresh"]))
    log("RESULT: " + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED, read the report"))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
