"""Schema registry: compare schemas across files and classify drift against the data contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import yaml

# Columns documented in the TLC Yellow Trips data dictionary (2025-03-18).
KNOWN_COLUMNS = {
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee",
}


def load_contract(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def read_schema(path: Path) -> dict[str, str]:
    schema = pq.ParquetFile(path).schema_arrow
    return {f.name: str(f.type) for f in schema}


def fingerprint(schema: dict[str, str]) -> str:
    payload = json.dumps(sorted(schema.items()), separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _norm(name: str) -> str:
    return name.lower()


def audit_schema(schema: dict[str, str], contract: dict) -> list[dict]:
    """Return findings for one file: {kind, column, severity, detail}."""
    findings: list[dict] = []
    names = {_norm(c): c for c in schema}
    rules = contract.get("schema_evolution", {})

    for col in contract.get("required_fields", []):
        if _norm(col) not in names:
            findings.append({"kind": "missing_required_column", "column": col,
                             "severity": rules.get("missing_required_columns", "error"),
                             "detail": "required column absent"})

    known = {_norm(c) for c in KNOWN_COLUMNS}
    for col in schema:
        if _norm(col) not in known:
            findings.append({"kind": "unknown_column", "column": col,
                             "severity": "warning" if rules.get("unknown_columns") == "report" else "error",
                             "detail": "column not in data dictionary"})
        elif col not in KNOWN_COLUMNS:
            findings.append({"kind": "column_case_variant", "column": col, "severity": "warning",
                             "detail": "documented column with different capitalisation"})
    return findings


def diff_schemas(prev: dict[str, str], curr: dict[str, str]) -> list[dict]:
    """Added / removed / type-changed columns between two consecutive periods."""
    changes = []
    for col in sorted(set(curr) - set(prev)):
        changes.append({"change": "added_column", "column": col, "to": curr[col]})
    for col in sorted(set(prev) - set(curr)):
        changes.append({"change": "removed_column", "column": col, "from": prev[col]})
    for col in sorted(set(prev) & set(curr)):
        if prev[col] != curr[col]:
            changes.append({"change": "type_changed", "column": col, "from": prev[col], "to": curr[col]})
    return changes


def build_registry(files: list[tuple[str, Path]], contract: dict) -> dict:
    """files: [(period, path)] sorted by period. Returns per-file findings and period-to-period drift."""
    registry = {"files": [], "drift": [], "blocking": False}
    prev_schema: dict[str, str] | None = None
    prev_period = None
    for period, path in files:
        schema = read_schema(path)
        findings = audit_schema(schema, contract)
        registry["files"].append({"period": period, "file": path.name,
                                  "fingerprint": fingerprint(schema), "findings": findings})
        if any(f["severity"] == "error" for f in findings):
            registry["blocking"] = True
        if prev_schema is not None:
            changes = diff_schemas(prev_schema, schema)
            if changes:
                registry["drift"].append({"from": prev_period, "to": period, "changes": changes})
        prev_schema, prev_period = schema, period
    return registry
