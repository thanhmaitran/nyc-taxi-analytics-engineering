import pyarrow as pa
import pyarrow.parquet as pq

from ingestion import schema_audit

CONTRACT = {
    "required_fields": ["tpep_pickup_datetime", "PULocationID"],
    "schema_evolution": {"unknown_columns": "report", "missing_required_columns": "error"},
}


def test_missing_required_column_is_error():
    findings = schema_audit.audit_schema({"PULocationID": "int32"}, CONTRACT)
    assert [(f["kind"], f["column"], f["severity"]) for f in findings] == [
        ("missing_required_column", "tpep_pickup_datetime", "error")]


def test_unknown_column_is_reported_not_ignored():
    schema = {"tpep_pickup_datetime": "timestamp[us]", "PULocationID": "int32", "mystery": "string"}
    findings = schema_audit.audit_schema(schema, CONTRACT)
    assert findings == [{"kind": "unknown_column", "column": "mystery", "severity": "warning",
                         "detail": "column not in data dictionary"}]


def test_case_variant_is_flagged_as_warning():
    schema = {"tpep_pickup_datetime": "x", "PULocationID": "x", "Airport_fee": "double"}
    kinds = {f["kind"] for f in schema_audit.audit_schema(schema, CONTRACT)}
    assert kinds == {"column_case_variant"}


def test_diff_detects_added_removed_and_type_change():
    prev = {"a": "int64", "b": "double", "c": "string"}
    curr = {"a": "string", "b": "double", "cbd_congestion_fee": "double"}
    changes = {(c["change"], c["column"]) for c in schema_audit.diff_schemas(prev, curr)}
    assert changes == {("type_changed", "a"), ("removed_column", "c"), ("added_column", "cbd_congestion_fee")}


def test_registry_blocks_when_required_column_missing(tmp_path):
    path = tmp_path / "yellow_tripdata_2024-01.parquet"
    pq.write_table(pa.table({"PULocationID": pa.array([1], pa.int32())}), path)
    registry = schema_audit.build_registry([("2024-01", path)], CONTRACT)
    assert registry["blocking"] is True


def test_fingerprint_changes_with_type():
    assert schema_audit.fingerprint({"a": "int64"}) != schema_audit.fingerprint({"a": "string"})
    assert schema_audit.fingerprint({"a": "int64", "b": "x"}) == schema_audit.fingerprint({"b": "x", "a": "int64"})
