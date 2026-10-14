"""Validate source files: readability (metadata and a real data read), checksum, row count."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from .inventory import InventoryItem


def sha256_of(path: Path, chunk: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def validate_file(item: InventoryItem, deep: bool = True) -> dict:
    """Return a validation record. `deep` reads every row group, not just the footer."""
    result = {"file_name": item.file_name, "period": item.period, "size_bytes": item.size_bytes,
              "status": item.status, "row_count": None, "sha256": None, "error": None}
    if item.status != "ok":
        return result
    try:
        parquet = pq.ParquetFile(item.path)
        result["row_count"] = parquet.metadata.num_rows
        if deep:
            rows = sum(parquet.read_row_group(i, columns=[parquet.schema_arrow.names[0]]).num_rows
                       for i in range(parquet.num_row_groups))
            if rows != result["row_count"]:
                result["status"] = "row_count_mismatch"
                return result
        result["sha256"] = sha256_of(item.path)
    except Exception as exc:  # noqa: BLE001 - any read error means the file is unusable
        result["status"] = "unreadable"
        result["error"] = str(exc)
    return result
