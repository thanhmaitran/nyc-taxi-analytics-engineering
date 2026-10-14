import pyarrow as pa
import pyarrow.parquet as pq

from ingestion import inventory, manifest, validate_source


def make_parquet(path, values=(1, 2, 3)):
    pq.write_table(pa.table({"x": list(values)}), path)
    return path


def validate(path):
    return validate_source.validate_file(inventory.classify(path))


def test_valid_file_gets_row_count_and_checksum(tmp_path):
    result = validate(make_parquet(tmp_path / "yellow_tripdata_2024-01.parquet"))
    assert result["status"] == "ok" and result["row_count"] == 3
    assert len(result["sha256"]) == 64


def test_truncated_file_is_unreadable(tmp_path):
    path = make_parquet(tmp_path / "yellow_tripdata_2024-01.parquet", range(1000))
    path.write_bytes(path.read_bytes()[: path.stat().st_size // 2])
    assert validate(path)["status"] == "unreadable"


def test_garbage_file_is_unreadable(tmp_path):
    path = tmp_path / "yellow_tripdata_2024-01.parquet"
    path.write_bytes(b"this is not parquet")
    assert validate(path)["status"] == "unreadable"


def test_checksum_is_deterministic_and_content_sensitive(tmp_path):
    a = validate(make_parquet(tmp_path / "yellow_tripdata_2024-01.parquet", (1, 2)))
    b = validate(make_parquet(tmp_path / "yellow_tripdata_2024-01.parquet", (1, 2)))
    c = validate(make_parquet(tmp_path / "yellow_tripdata_2024-01.parquet", (1, 9)))
    assert a["sha256"] == b["sha256"] != c["sha256"]


def test_manifest_detects_changed_checksum_and_keeps_reprocess_flag(tmp_path):
    path = tmp_path / "yellow_tripdata_2024-01.parquet"
    mf = {"dataset": "yellow_taxi", "files": {}}

    assert manifest.record(mf, validate(make_parquet(path, (1, 2))), "fp", "validated") == "new"
    mf["files"][path.name]["status"] = "loaded"

    assert manifest.record(mf, validate(path), "fp", "validated") == "unchanged"
    assert mf["files"][path.name]["status"] == "loaded"          # re-audit must not downgrade

    assert manifest.record(mf, validate(make_parquet(path, (5, 6))), "fp", "validated") == "changed"
    assert mf["files"][path.name]["status"] == "needs_reprocessing"

    manifest.record(mf, validate(path), "fp", "validated")       # audited again before transform ran
    assert mf["files"][path.name]["status"] == "needs_reprocessing"  # flag survives
