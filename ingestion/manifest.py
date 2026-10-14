"""Manifest: persistent per-file state keyed by file name, with checksum change detection."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def load(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"dataset": "yellow_taxi", "files": {}}


def save(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def classify_change(manifest: dict, file_name: str, sha256: str | None) -> str:
    """new | unchanged | changed (same name, different checksum -> must be investigated)."""
    previous = manifest["files"].get(file_name)
    if previous is None or previous.get("sha256") is None:
        return "new"
    return "unchanged" if previous["sha256"] == sha256 else "changed"


def record(manifest: dict, validation: dict, fingerprint: str | None, status: str,
           source_url: str | None = None) -> str:
    """Upsert one file. Returns the change classification."""
    change = classify_change(manifest, validation["file_name"], validation["sha256"])
    entry = {
        "dataset": manifest["dataset"],
        "source_file": validation["file_name"],
        "period": validation["period"],
        "source_url": source_url,
        "file_size_bytes": validation["size_bytes"],
        "sha256": validation["sha256"],
        "row_count": validation["row_count"],
        "schema_fingerprint": fingerprint,
        "status": status,
        "change": change,
        "error": validation["error"],
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    previous = manifest["files"].get(validation["file_name"], {})
    if change == "changed" and status in ("validated", "loaded", "needs_reprocessing"):
        entry["status"] = "needs_reprocessing"
        entry["previous_sha256"] = previous["sha256"]
    elif change == "unchanged" and status == "validated" and previous.get("status") in (
        "loaded", "needs_reprocessing",
    ):
        # Re-auditing must not erase "loaded" or a pending reprocess flag.
        entry["status"] = previous["status"]
        if "previous_sha256" in previous:
            entry["previous_sha256"] = previous["previous_sha256"]
    manifest["files"][validation["file_name"]] = entry
    return change
