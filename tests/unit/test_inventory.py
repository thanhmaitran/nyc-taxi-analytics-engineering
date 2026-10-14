from pathlib import Path

from ingestion import inventory


def touch(path: Path, content: bytes = b"x") -> Path:
    path.write_bytes(content)
    return path


def test_classify_valid_name(tmp_path):
    item = inventory.classify(touch(tmp_path / "yellow_tripdata_2024-01.parquet"))
    assert (item.year, item.month, item.period, item.status) == (2024, 1, "2024-01", "ok")


def test_classify_rejects_green_taxi_and_bad_month(tmp_path):
    green = inventory.classify(touch(tmp_path / "green_tripdata_2024-01.parquet"))
    bad_month = inventory.classify(touch(tmp_path / "yellow_tripdata_2024-13.parquet"))
    assert green.status == "unrecognized_filename"
    assert bad_month.status == "invalid_filename_month"


def test_empty_file_is_flagged(tmp_path):
    item = inventory.classify(touch(tmp_path / "yellow_tripdata_2024-02.parquet", b""))
    assert item.status == "empty_file"


def test_missing_periods_are_reported_across_year_boundary(tmp_path):
    for name in ("2024-11", "2025-02"):
        touch(tmp_path / f"yellow_tripdata_{name}.parquet")
    items = inventory.scan(tmp_path)
    assert inventory.missing_periods(items) == ["2024-12", "2025-01"]


def test_duplicate_periods_detected(tmp_path):
    touch(tmp_path / "yellow_tripdata_2024-03.parquet")
    touch(tmp_path / "yellow_tripdata_2024-03.PARQUET")
    items = inventory.scan(tmp_path) if False else [
        inventory.classify(tmp_path / "yellow_tripdata_2024-03.parquet"),
        inventory.classify(tmp_path / "yellow_tripdata_2024-03.PARQUET"),
    ]
    assert inventory.duplicate_periods(items) == {"2024-03": 2}
