"""The one-command verification script must run end to end and write its report (guards the report writer)."""
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_verify_real_data_runs_end_to_end_on_fixtures(raw_dir, tmp_path):
    docs = tmp_path / "docs"
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_real_data.py"), "--label", "synthetic-test",
         "--raw-dir", str(raw_dir), "--work-dir", str(tmp_path / "work"), "--docs-dir", str(docs),
         "--bench-files", "3"],
        capture_output=True, text=True, cwd=ROOT)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    report = next(docs.glob("synthetic-test_*.md")).read_text()
    assert "ALL CHECKS PASSED" in result.stdout
    for heading in ("## 2. Raw data audit", "## 4. Data quality", "## 5. Benchmark"):
        assert heading in report
    assert "By-file loader output equals single-pass full refresh: **True**" in report
    assert "renamed_case" not in report and "cbd_congestion_fee" in report
