# ADR 0004 - Memory-bounded loading (one file at a time)

**Status:** accepted

**Context.** The first run on the real data (56 files, 197,562,001 rows, Windows 11, 8 CPUs, 2026-10-15, four
attempts) passed the audit but `dbt build --full-refresh` stopped at `unique_stg_yellow_taxi_trip_record_key` with
`Out of Memory Error: Allocation failure`; the remaining 39 steps were skipped, so nothing past staging was verified.
Cause: whole-history operations over a 197M-row view: a uniqueness test on a 32-byte md5 key, a `not_null` test on the
same key (36 s), and, further down the DAG, the validation window (sort by file + row hash), `distinct` on the fact
table and tests re-running the views. DuckDB's default memory limit is a share of RAM that is not tuned for this.

**Decision.**
1. Profile: absolute `memory_limit` (DuckDB rejects percentages; `run_pipeline.py` sets ~60% of RAM, override with
   `DUCKDB_MEMORY_LIMIT`), `preserve_insertion_order=false`, 2 dbt threads, spill to `<db>.tmp`.
2. No data tests on views that re-run the whole window. Staging gets a cheap per-file check (`file_row_number` is
   0..n-1). `unique(trip_record_key)` on the stored tables stays as a dbt test (default, used by single-pass builds and
   CI), but a smoke test with 225M synthetic rows ran out of memory table-wide even with spilling
   (`failed to pin block ... 1.8 GiB/1.8 GiB used`; that failure is described by the author, its log is not in the repository). The file-by-file loader therefore disables it
   (`--vars full_table_unique_tests=false`) and checks uniqueness per file right after each load; keys embed the file
   name, so per-file uniqueness implies table-wide uniqueness (barring an md5 collision).
3. `fact_trips`, `quarantine_trips` and `mart_quality_reconciliation` are incremental per `source_file`
   (delete+insert plus a pre-hook that removes reprocessed files, so a re-delivered clean file leaves no stale rows).
4. `run_pipeline.py transform` loads one file at a time through those models, then builds dimensions, marts and runs the
   tests once. `dim_datetime` is derived from `fact_trips`. `--mode single-pass` keeps the old behaviour.

**Evidence.** Integration tests assert that the by-file load equals a single-pass build, row for row, on every table
and mart, and that a re-delivered clean file clears its quarantine rows. A reproducible scale smoke test is `scripts/scale_smoke_test.py`; its 2026-10-16 result (18M synthetic rows under a 2 GB limit) is in
`docs/results/synthetic-scale-smoke_2026-10-16.json`. Synthetic data shows memory behaviour, not data quality.

**Consequences.** Peak memory is bounded by one month plus the final aggregations, not by the whole history. Per-file
dbt start-up adds a few seconds per file. Duplicate detection is per file only (as before).

**Evidence for the by-file loader at scale.** `docs/results/evidence/synthetic-scale-225M_*`: 45 synthetic files, 225,000,000 rows,
DuckDB limit 2 GB, 2 CPUs / 8.4 GB RAM (Linux): full load 1,257.6 s, 358 dbt steps pass, 0 errors, all 8 reconciliation checks PASS;
benchmark (last 6 files) replay idempotent and incremental == full refresh. Synthetic data: memory and plumbing only.

**Amendment 2026-10-16.** The profile reads `DBT_DUCKDB_MEMORY_LIMIT`, `DBT_THREADS` and `DBT_DUCKDB_TEMP`, but the
first version of the loader exported a different name (`DUCKDB_MEMORY_LIMIT`), so dbt always ran with the profile defaults
(8 GB, 4 threads). `pipeline_lib.dbt_env()` now sets the names the profile reads (60% of RAM, 2 threads, temp dir next to
the database); `DUCKDB_MEMORY_LIMIT` remains the user override. A unit test fails if the two ever diverge again.
