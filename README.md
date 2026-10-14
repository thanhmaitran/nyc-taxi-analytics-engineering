# Analytics Engineering Data Warehouse

NYC TLC Yellow Taxi trip records (2022–2026) → audited raw Parquet → DuckDB → dbt star schema → quality
reports. Python + Parquet + DuckDB + dbt + SQLFluff + pytest + GitHub Actions.

## Status (read this first)
| Item | State |
|---|---|
| Pipeline code, dbt models, tests, CI, docs | done; 22 pytest tests pass, 40+ dbt tests pass, SQLFluff clean |
| Verified on | **synthetic fixtures** with TLC-like schema (`scripts/make_fixtures.py`) |
| Real TLC data 2022–2026 | **not yet processed.** The environment this was built in could not reach the TLC download host (HTTP 403, rechecked), so no real file was audited and no real row count, runtime or quality figure exists yet. One command (below) produces all of them on your machine. |
| Verification report | `scripts/verify_real_data.py` was dry-run on synthetic fixtures and works; the real-data report will appear in `docs/results/` after you run it |
| Benchmarks | harness works; fixture timings are not performance claims |

## Verify on real data (steps B–E, one command)
```powershell
python scripts/download_data.py --start 2022-01 --end 2026-12     # skip if files are already in data/raw/yellow_taxi/
python scripts/verify_real_data.py --label real-2022-2026
```
It audits the files, builds the warehouse with a timed full refresh, reconciles row counts across every layer
(Parquet metadata → raw view → staging → validated → quarantine + fact), summarises quarantine and coverage by year
and file, benchmarks full refresh vs incremental (default: the 6 most recent files, `--bench-files 0` for all),
and writes `docs/results/real-2022-2026_<date>.md` (commit it) plus `reports/run_<ts>/run.log` and `evidence.json`.
Exit code 0 means every check passed; 1 means a blocking source problem; 2 means a check failed.
Add `--continue-on-error` to load the valid files when some are broken, and `--skip-benchmark` for a faster run.
Link the generated report below once it exists.

## Results
_Pending: no real-data run yet._

## Quick start
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1          # or: python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python scripts/download_data.py --start 2022-01 --end 2026-12   # months TLC has not published are reported as gaps
python scripts/run_pipeline.py audit        # inventory, checksums, schema drift -> data/manifests/
python scripts/run_pipeline.py all          # audit -> build-raw -> transform (dbt build) -> report
python scripts/benchmark.py --new-files 1   # full refresh vs incremental, same data, equivalence check
```
Already have the files? Put them in `data/raw/yellow_taxi/` (names `yellow_tripdata_YYYY-MM.parquet`).

Recommended order on real data (see `docs/architecture.md`): run `audit` on everything, read
`data/manifests/raw_audit.json` (gaps, drift, unreadable files), then build 1–3 months first, then backfill.
If a file is quarantined or broken, `audit` exits non-zero; use `--continue-on-error` to load the rest.

Stages can be run separately: `audit`, `build-raw`, `transform [--full-refresh]`, `report`.

## Layout
`ingestion/` inventory, validation, schema registry, manifest · `scripts/` CLI stages, fixtures, benchmark, `verify_real_data.py` ·
`warehouse/` dbt project · `config/data_contract.yml` · `tests/` unit + integration · `docs/` architecture, metrics,
dictionary, quality, lineage, ADRs.

## Design decisions
- Grain of `fact_trips` is one retained **source record** (TLC has no trip id). Key = md5(file + row number).
- Nothing is silently dropped: bad rows go to `quarantine_trips` with a reason; schema surprises are reported.
- Incremental = delete+insert per monthly file, proven equal to full refresh (ADR 0002); money is `decimal(18,2)`
  so that equality is exact (ADR 0003).
- Same file name with a different checksum is reloaded, never appended.

## Tests
```
pytest -q                                      # unit + dbt integration on fixtures (~1 min)
sqlfluff lint warehouse/models warehouse/tests
dbt docs generate --project-dir warehouse --profiles-dir warehouse
```

## Limitations
Only the columns needed for the marts are carried (see `docs/data_dictionary.md`). Duplicate detection uses a
window over each file and may be heavy on the full history; it can be disabled with a dbt var. Marts are rebuilt
each run. No orchestrator, cloud or Spark.

Data: NYC Taxi & Limousine Commission Trip Record Data. TLC does not guarantee the accuracy of every record.
