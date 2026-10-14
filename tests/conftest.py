from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import make_fixtures  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


def write_month(directory: Path, period: str, rows: int = 300, seed: int = 1, defects: bool = True) -> Path:
    year, month = map(int, period.split("-"))
    table = make_fixtures.make_month(year, month, rows, seed, with_cbd=year >= 2025, defects=defects)
    path = directory / f"yellow_tripdata_{period}.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture()
def raw_dir(tmp_path: Path) -> Path:
    d = tmp_path / "raw"
    d.mkdir()
    for n, period in enumerate(["2024-12", "2025-01", "2025-02"]):
        write_month(d, period, seed=10 + n)
    return d


@pytest.fixture()
def contract_path() -> Path:
    return ROOT / "config" / "data_contract.yml"


@pytest.fixture()
def zone_csv() -> Path:
    return ROOT / "data" / "reference" / "taxi_zone_lookup.csv"
