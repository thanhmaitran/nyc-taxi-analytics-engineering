"""Pipeline entry point. Each stage can be run alone or all together.

  audit      inventory + validate + schema audit (updates the manifest)
  build-raw  register validated files as raw_yellow_taxi in DuckDB
  transform  dbt build (incremental by default; --full-refresh to rebuild)
  report     write reports/pipeline_report.{json,md} from the warehouse
  all        audit -> build-raw -> transform -> report
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import audit_raw_data  # noqa: E402
import build_raw_views  # noqa: E402
import pipeline_lib as lib  # noqa: E402
from ingestion import manifest as mf  # noqa: E402

ROOT = lib.ROOT


def stage_audit(a) -> int:
    a.manifest_dir.mkdir(parents=True, exist_ok=True)
    report = audit_raw_data.run(a.raw_dir, a.manifest_dir, a.contract, a.zone_csv, deep=not a.fast)
    print(f"[audit] files={report['file_count']} status={report['status_counts']} "
          f"gaps={report['missing_periods']} blocking={report['blocking']}")
    if report["blocking"] and not a.continue_on_error:
        print("[audit] blocking problem; fix or quarantine files, or pass --continue-on-error "
              "to load only validated files")
        return 1
    return 0


def stage_build_raw(a) -> int:
    rows, files = build_raw_views.build(a.raw_dir, a.db, a.manifest_dir / "manifest.json")
    print(f"[build-raw] rows={rows:,} files={files}")
    return 0


def stage_transform(a) -> int:
    manifest_path = a.manifest_dir / "manifest.json"
    manifest = mf.load(manifest_path)
    reprocess = lib.reprocess_list(manifest)
    if reprocess:
        print(f"[transform] checksum changed, reloading: {reprocess}")
    print(f"[transform] mode={a.mode} duckdb memory_limit={lib.memory_limit()}")
    if a.mode == "single-pass":
        res = lib.dbt(["build"], a.db, variables={"reprocess_files": reprocess} if reprocess else None,
                      full_refresh=a.full_refresh)
        summary = lib.dbt_summary(res)
        ok, detail = res.returncode == 0, res.stdout[-3000:]
    else:
        result = lib.load_by_file(a.raw_dir, a.db, manifest, full_refresh=a.full_refresh, log=print)
        summary = result.get("summary", {})
        ok, detail = "error_output" not in result and summary.get("error", 1) == 0, result.get("error_output", "")
    print(f"[transform] {summary}")
    if not ok:
        print(detail)
        return 1
    lib.mark_loaded(manifest)
    mf.save(manifest_path, manifest)
    return 0


def stage_report(a) -> int:
    con = duckdb.connect(str(a.db), read_only=True)
    try:
        rec = con.execute("select * from main.mart_quality_reconciliation order by source_period").fetchdf()
        reasons = con.execute(
            "select quarantine_reason, count(*) n from main.quarantine_trips group by 1 order by 2 desc").fetchdf()
    finally:
        con.close()
    a.report_dir.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "commit": lib.git_sha(),
               "per_file": json.loads(rec.to_json(orient="records")),
               "quarantine_reasons": json.loads(reasons.to_json(orient="records"))}
    (a.report_dir / "pipeline_report.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# Pipeline report", "", f"Generated: {payload['generated_at_utc']}", "",
             "| file | source | valid | quarantined | unmapped pickup | fact |", "|---|---:|---:|---:|---:|---:|"]
    for r in payload["per_file"]:
        n = {k: int(v) for k, v in r.items() if k.endswith("_rows")}
        lines.append(f"| {r['source_file']} | {n['source_rows']:,} | {n['valid_rows']:,} | "
                     f"{n['quarantined_rows']:,} | {n['unmapped_pickup_rows']:,} | {n['fact_rows']:,} |")
    (a.report_dir / "pipeline_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[report] {a.report_dir / 'pipeline_report.md'}")
    return 0


STAGES = {"audit": stage_audit, "build-raw": stage_build_raw, "transform": stage_transform, "report": stage_report}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("stage", choices=[*STAGES, "all"])
    p.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw/yellow_taxi")
    p.add_argument("--manifest-dir", type=Path, default=ROOT / "data/manifests")
    p.add_argument("--db", type=Path, default=ROOT / "data/warehouse.duckdb")
    p.add_argument("--contract", type=Path, default=ROOT / "config/data_contract.yml")
    p.add_argument("--zone-csv", type=Path, default=ROOT / "data/reference/taxi_zone_lookup.csv")
    p.add_argument("--report-dir", type=Path, default=ROOT / "reports")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--mode", choices=["by-file", "single-pass"], default="by-file",
                   help="by-file (default): one monthly file at a time, bounded memory; "
                        "single-pass: one dbt build over everything (needs RAM proportional to all rows)")
    p.add_argument("--fast", action="store_true", help="audit: metadata only")
    p.add_argument("--continue-on-error", action="store_true",
                   help="proceed with validated files when others are broken/quarantined")
    a = p.parse_args()
    for name in (list(STAGES) if a.stage == "all" else [a.stage]):
        code = STAGES[name](a)
        if code:
            return code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
