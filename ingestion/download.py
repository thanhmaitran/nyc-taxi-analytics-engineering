"""Download TLC monthly Parquet files with retry. Existing complete files are never overwritten."""
from __future__ import annotations

import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def url_for(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download(year: int, month: int, dest_dir: Path, retries: int = 3) -> str:
    """Return: downloaded | exists | not_available | failed."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"yellow_tripdata_{year}-{month:02d}.parquet"
    if target.exists() and target.stat().st_size > 0:
        return "exists"
    partial = target.with_suffix(".parquet.part")
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url_for(year, month), timeout=60) as resp, partial.open("wb") as out:
                while block := resp.read(1024 * 1024):
                    out.write(block)
            partial.replace(target)  # atomic: no half-downloaded file ever has the final name
            return "downloaded"
        except urllib.error.HTTPError as exc:
            partial.unlink(missing_ok=True)
            if exc.code in (403, 404):  # TLC publishes with a delay; a missing month is a gap, not an error
                return "not_available"
        except (urllib.error.URLError, TimeoutError):
            partial.unlink(missing_ok=True)
        time.sleep(2 ** attempt)
    return "failed"
