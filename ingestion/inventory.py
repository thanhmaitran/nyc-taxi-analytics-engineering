"""Scan raw Parquet files, derive the period and detect duplicate / missing months."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

FILENAME_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet", re.IGNORECASE)


@dataclass(frozen=True)
class InventoryItem:
    path: Path
    file_name: str
    size_bytes: int
    year: int | None
    month: int | None
    status: str  # ok | unrecognized_filename | invalid_filename_month | empty_file

    @property
    def period(self) -> str | None:
        if self.year is None or self.month is None:
            return None
        return f"{self.year}-{self.month:02d}"


def classify(path: Path) -> InventoryItem:
    size = path.stat().st_size
    match = FILENAME_PATTERN.fullmatch(path.name)
    year = month = None
    status = "ok"
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if not 1 <= month <= 12:
            status = "invalid_filename_month"
    else:
        status = "unrecognized_filename"
    if size == 0 and status == "ok":
        status = "empty_file"
    return InventoryItem(path, path.name, size, year, month, status)


def scan(raw_dir: Path) -> list[InventoryItem]:
    return [classify(p) for p in sorted(raw_dir.glob("*.parquet"))]


def duplicate_periods(items: list[InventoryItem]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if item.period:
            counts[item.period] = counts.get(item.period, 0) + 1
    return {k: v for k, v in counts.items() if v > 1}


def missing_periods(items: list[InventoryItem]) -> list[str]:
    """Gaps between the first and last period found. A gap is reported, never assumed empty."""
    periods = sorted({(i.year, i.month) for i in items if i.period})
    if len(periods) < 2:
        return []
    present = set(periods)
    (y, m), (end_y, end_m) = periods[0], periods[-1]
    gaps = []
    while (y, m) <= (end_y, end_m):
        if (y, m) not in present:
            gaps.append(f"{y}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return gaps
