"""End-to-end tests on small synthetic fixtures (real dbt + DuckDB, no TLC download needed)."""
from __future__ import annotations

import json
import shutil

import duckdb
import pytest

import audit_raw_data
import benchmark
import build_raw_views
import pipeline_lib as lib
from conftest import write_month
from ingestion import manifest as mf

pytestmark = pytest.mark.integration


def audit(raw_dir, workdir, contract_path, zone_csv):
    man_dir = workdir / "manifests"
    man_dir.mkdir(exist_ok=True)
    return audit_raw_data.run(raw_dir, man_dir, contract_path, zone_csv), man_dir / "manifest.json"


def query(db, sql):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_schema_drift_2025_is_detected(raw_dir, tmp_path, contract_path, zone_csv):
    report, _ = audit(raw_dir, tmp_path, contract_path, zone_csv)
    assert report["blocking"] is False
    added = [c for d in report["schema_drift"] for c in d["changes"] if c["change"] == "added_column"]
    assert [c["column"] for c in added] == ["cbd_congestion_fee"]
    assert report["schema_drift"][0]["to"] == "2025-01"


def test_build_catches_injected_defects_and_reconciles(raw_dir, tmp_path, contract_path, zone_csv):
    _, manifest_path = audit(raw_dir, tmp_path, contract_path, zone_csv)
    db = tmp_path / "wh.duckdb"
    build_raw_views.build(raw_dir, db, manifest_path)
    result = lib.dbt(["build"], db)
    summary = lib.dbt_summary(result)
    assert result.returncode == 0, result.stdout[-2000:]
    assert summary["error"] == 0

    reasons = dict(query(db, "select quarantine_reason, count(*) from main.quarantine_trips group by 1"))
    # per file: row0 dropoff<pickup, row3 missing pickup; no cross-period rows in fixtures
    assert reasons == {"dropoff_before_pickup": 3, "missing_pickup_timestamp": 3}

    unmapped, neg_dist = query(db, "select sum(unmapped_pickup_rows), sum(negative_distance_rows) "
                                   "from main.mart_quality_reconciliation")[0]
    assert (unmapped, neg_dist) == (3, 3)  # injected defects are reported as warnings, rows kept
    assert query(db, "select count(*) from main.fact_trips where pickup_zone_key = -1")[0][0] == 3

    # 2024 file has no cbd column: staging must still work and yield NULL
    assert query(db, "select count(cbd_congestion_fee) from main.fact_trips "
                     "where source_period = '2024-12'")[0][0] == 0
    assert query(db, "select count(cbd_congestion_fee) from main.fact_trips "
                     "where source_period = '2025-01'")[0][0] > 0


def test_exact_duplicate_rows_are_quarantined(tmp_path, contract_path, zone_csv):
    import pyarrow as pa
    import pyarrow.parquet as pq

    raw = tmp_path / "raw"
    raw.mkdir()
    path = write_month(raw, "2025-01", rows=50, seed=3, defects=False)
    table = pq.read_table(path)
    pq.write_table(pa.concat_tables([table, table.slice(10, 1)]), path)  # row 10 duplicated

    _, manifest_path = audit(raw, tmp_path, contract_path, zone_csv)
    db = tmp_path / "wh.duckdb"
    build_raw_views.build(raw, db, manifest_path)
    assert lib.dbt(["build"], db).returncode == 0
    assert query(db, "select quarantine_reason, count(*) from main.quarantine_trips group by 1") == [
        ("exact_duplicate_row", 1)]
    assert query(db, "select count(*) from main.fact_trips")[0][0] == 50


def test_file_missing_required_column_is_quarantined_and_never_loaded(raw_dir, tmp_path, contract_path, zone_csv):
    import pyarrow.parquet as pq

    bad = raw_dir / "yellow_tripdata_2025-02.parquet"
    pq.write_table(pq.read_table(bad).drop(["tpep_pickup_datetime"]), bad)

    report, manifest_path = audit(raw_dir, tmp_path, contract_path, zone_csv)
    assert report["blocking"] is True
    entries = json.loads(manifest_path.read_text())["files"]
    assert entries[bad.name]["status"] == "quarantined"

    db = tmp_path / "wh.duckdb"
    rows, nfiles = build_raw_views.build(raw_dir, db, manifest_path)
    assert nfiles == 2  # the broken file never reaches the warehouse


def test_incremental_equals_full_refresh_and_replay_is_idempotent(raw_dir, tmp_path):
    result = benchmark.run(raw_dir, tmp_path / "bench", new_files=1)
    assert result["replay_idempotent"] is True
    bad = {t: v for t, v in result["equivalence_detail"].items() if not v["equal"]}
    assert not bad, bad
    assert all(r["dbt_tests"]["error"] == 0 for r in result["runs"])


def test_changed_file_is_reloaded_without_duplicates(raw_dir, tmp_path, contract_path, zone_csv):
    man_dir = tmp_path / "manifests"
    man_dir.mkdir()
    db = tmp_path / "wh.duckdb"

    def pipeline(full_refresh=False):
        audit_raw_data.run(raw_dir, man_dir, contract_path, zone_csv)
        build_raw_views.build(raw_dir, db, man_dir / "manifest.json")
        manifest = mf.load(man_dir / "manifest.json")
        reprocess = lib.reprocess_list(manifest)
        res = lib.dbt(["build"], db, variables={"reprocess_files": reprocess} if reprocess else None,
                      full_refresh=full_refresh)
        assert res.returncode == 0, res.stdout[-2000:]
        lib.mark_loaded(manifest)
        mf.save(man_dir / "manifest.json", manifest)
        return reprocess

    assert pipeline() == []
    before = query(db, "select count(*) from main.fact_trips")[0][0]

    assert pipeline() == []  # replay: nothing to do, nothing duplicated
    assert query(db, "select count(*) from main.fact_trips")[0][0] == before

    # same file name, new content (different row count)
    write_month(raw_dir, "2025-01", rows=120, seed=99)
    assert pipeline() == ["yellow_tripdata_2025-01.parquet"]
    assert query(db, "select count(*) from main.fact_trips where source_file='yellow_tripdata_2025-01.parquet'"
                 )[0][0] == 120 - 2  # 2 quarantined defects
    assert query(db, "select count(*) from (select trip_record_key from main.fact_trips "
                     "group by 1 having count(*)>1)")[0][0] == 0

    # incremental result must match a clean full refresh over the final files
    ref_db = tmp_path / "ref" / "wh.duckdb"
    ref_db.parent.mkdir()
    ref_raw = tmp_path / "ref_raw"
    shutil.copytree(raw_dir, ref_raw)
    benchmark.timed_run("ref", ref_raw, ref_db, True)
    diff = lib.compare_databases(db, ref_db)
    assert all(v["equal"] for v in diff.values()), diff
