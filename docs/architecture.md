# Architecture

## Data source and its limits
NYC TLC Yellow Taxi Trip Records, published monthly as Parquet, plus the Taxi Zone Lookup CSV.
Records are supplied by technology providers; TLC does not guarantee every record is accurate, and Parquet
schemas can change between years (for example `cbd_congestion_fee` from January 2025). The project therefore
treats the source as *untrusted until audited*.

## Flow
```
TLC monthly Parquet (data/raw, immutable, local only)
        │  scripts/download_data.py  (atomic download, retry, gaps reported)
        ▼
ingestion/ + scripts/audit_raw_data.py
   inventory → validate (full read + SHA-256) → schema audit vs config/data_contract.yml → manifest
        │  only manifest status validated / needs_reprocessing / loaded passes on
        ▼
scripts/build_raw_views.py  →  DuckDB view raw_yellow_taxi (union_by_name, filename, file_row_number)
        ▼
dbt (warehouse/)
   staging       stg_yellow_taxi, taxi_zone_lookup_clean      views, rename + safe cast, no filtering
   intermediate  int_trip_validated (flags + quarantine_reason), int_trip_enriched (valid rows + keys)
   core marts    fact_trips (incremental), dim_zone, dim_payment_type, dim_datetime, quarantine_trips
   analytics     mart_daily_operations, mart_hourly_demand, mart_zone_performance,
                 mart_quality_reconciliation
        ▼
dbt tests (structural + singular reconciliation) · scripts/run_pipeline.py report · scripts/benchmark.py
```

## Roles
| Component | Role |
|---|---|
| Python (`ingestion/`) | file inventory, checksums, schema registry, manifest |
| Parquet | raw data, never modified |
| DuckDB | local analytical engine and warehouse file |
| dbt Core | transformation SQL, dependencies, tests, docs |
| SQLFluff / pytest | SQL style, Python and end-to-end tests |
| GitHub Actions | lint, parse and tests on every pull request |

## Schema evolution rules
Defined in `config/data_contract.yml` and enforced by `ingestion/schema_audit.py`:
- required column missing → `error`: file is `quarantined` in the manifest and never reaches the warehouse;
- unknown column → reported (`warning`), never silently dropped;
- column with different capitalisation → reported;
- added/removed/type-changed columns between consecutive periods → written to `data/manifests/raw_audit.json`
  (`schema_drift`);
- staging uses `col_or_null`, so a column that does not exist yet (e.g. `cbd_congestion_fee` before 2025)
  becomes `NULL` instead of breaking the build. All casts are `try_cast`; a cast failure yields `NULL`, which
  the validation layer then sees (it is not hidden).

## Re-runs, failures, testing
- Re-running is safe: manifest checksums skip unchanged files; `fact_trips` skips files already loaded.
- Same file name with a new checksum → status `needs_reprocessing` → `fact_trips` deletes and reloads that file's
  partition (`delete+insert` on `source_file`). See ADR 0002.
- A broken file is recorded with its error; `run_pipeline.py audit` exits non-zero unless
  `--continue-on-error` is given, in which case only validated files are loaded.
- Tests: `pytest` (unit + integration on synthetic fixtures), dbt tests, SQLFluff.

## Not implemented
No cloud, Spark or orchestrator. No `dbt-utils`. Real TLC data has not been run through this repository inside
the authoring environment (network access to TLC was blocked there); see README "Status".
