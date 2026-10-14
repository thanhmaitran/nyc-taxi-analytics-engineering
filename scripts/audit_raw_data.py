"""Phase 1+2: inventory, validate, schema-audit raw files and update the manifest.

Read-only on raw data. Exit code 1 when a blocking problem is found.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingestion import inventory, manifest, schema_audit, validate_source  # noqa: E402


def run(raw_dir: Path, manifest_dir: Path, contract_path: Path, zone_csv: Path, deep: bool = True) -> dict:
    items = inventory.scan(raw_dir)
    contract = schema_audit.load_contract(contract_path)
    manifest_path = manifest_dir / "manifest.json"
    mf = manifest.load(manifest_path)

    validations = {i.file_name: validate_source.validate_file(i, deep=deep) for i in items}
    usable = [(i.period, i.path) for i in items
              if i.period and validations[i.file_name]["status"] == "ok"]
    registry = schema_audit.build_registry(sorted(usable), contract)
    fingerprints = {f["file"]: f for f in registry["files"]}

    changes = {}
    for item in items:
        val = validations[item.file_name]
        reg = fingerprints.get(item.file_name)
        status = val["status"]
        if status == "ok":
            blocked = reg and any(f["severity"] == "error" for f in reg["findings"])
            status = "quarantined" if blocked else "validated"
        changes[item.file_name] = manifest.record(
            mf, val, reg["fingerprint"] if reg else None, status)
    manifest.save(manifest_path, mf)

    if zone_csv.exists() and zone_csv.stat().st_size > 0:
        with zone_csv.open(encoding="utf-8-sig") as fh:
            header = fh.readline().strip().replace('"', "").split(",")
        zone = {"status": "ok", "columns": header}
    else:
        zone = {"status": "missing"}

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_count": len(items),
        "total_bytes": sum(i.size_bytes for i in items),
        "duplicate_periods": inventory.duplicate_periods(items),
        "missing_periods": inventory.missing_periods(items),
        "zone_lookup": zone,
        "status_counts": {},
        "changed_checksums": [n for n, c in changes.items() if c == "changed"],
        "schema_drift": registry["drift"],
        "schema_findings": [{"file": f["file"], **x} for f in registry["files"] for x in f["findings"]],
        "blocking": registry["blocking"] or any(v["status"] != "ok" for v in validations.values()),
        "files": list(validations.values()),
    }
    for v in validations.values():
        report["status_counts"][v["status"]] = report["status_counts"].get(v["status"], 0) + 1
    (manifest_dir / "raw_audit.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw/yellow_taxi")
    parser.add_argument("--manifest-dir", type=Path, default=ROOT / "data/manifests")
    parser.add_argument("--contract", type=Path, default=ROOT / "config/data_contract.yml")
    parser.add_argument("--zone-csv", type=Path, default=ROOT / "data/reference/taxi_zone_lookup.csv")
    parser.add_argument("--fast", action="store_true", help="metadata only, skip full read check")
    args = parser.parse_args()
    args.manifest_dir.mkdir(parents=True, exist_ok=True)

    report = run(args.raw_dir, args.manifest_dir, args.contract, args.zone_csv, deep=not args.fast)
    print(f"Files: {report['file_count']}  bytes: {report['total_bytes']:,}")
    print(f"Status counts: {report['status_counts']}")
    print(f"Duplicate periods: {report['duplicate_periods']}")
    print(f"Missing periods (gaps): {report['missing_periods']}")
    print(f"Changed checksums: {report['changed_checksums']}")
    print(f"Schema drift events: {len(report['schema_drift'])}; findings: {len(report['schema_findings'])}")
    print(f"Zone lookup: {report['zone_lookup']['status']}")
    print(f"BLOCKING: {report['blocking']}")
    return 1 if report["blocking"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
