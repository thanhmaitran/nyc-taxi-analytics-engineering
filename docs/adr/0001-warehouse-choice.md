# ADR 0001 – DuckDB as the local warehouse

**Status:** accepted

**Context.** Several years of monthly Yellow Taxi Parquet files must be analysed on a personal machine with no
cloud account and a pipeline anyone can reproduce.

**Decision.** DuckDB, with raw Parquet exposed through a view (`read_parquet`, `union_by_name`, `filename`,
`file_row_number`) and dbt (`dbt-duckdb`) for transformations.

**Consequences.**
- Zero infrastructure, reads Parquet directly, no copy of raw data.
- `union_by_name` fills missing columns with NULL but does not reconcile same-name columns of different types, so
  the Python schema audit remains mandatory.
- The raw view stores absolute file paths; it is rebuilt from the manifest on every run.
- Single-writer: dbt threads share one process; no concurrent pipelines on the same `.duckdb` file.
- Scaling past one machine would mean a different engine; models are plain SQL and mostly portable.
