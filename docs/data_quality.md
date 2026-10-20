# Data quality

## Severity policy
- **error** – pipeline must not continue for that file (schema contract breach). File is `quarantined`.
- **quarantine** – row is removed from `fact_trips` but kept in `quarantine_trips` with a reason.
- **warning** – recorded and reported; rows stay in the fact table.

## Layer A – source validation (Python, before the warehouse)
File name matches `yellow_tripdata_YYYY-MM.parquet` with a valid month; non-empty; Parquet footer readable;
every row group readable and row count matches metadata; SHA-256 recorded; duplicate months and gaps reported;
required columns present; type/column drift between consecutive periods recorded.

## Layer B – structural dbt tests
`not_null`/`unique` on keys; `relationships` from fact keys to `dim_zone`, `dim_datetime`, `dim_payment_type`
(payment type is `warn`); `accepted_values` on `quarantine_reason`; grain-uniqueness test on each mart.

## Layer C – business rules (`int_trip_validated`)
| Rule | Severity | Action |
|---|---|---|
| pickup timestamp missing / unparseable | quarantine | `missing_pickup_timestamp` |
| dropoff timestamp missing / unparseable | quarantine | `missing_dropoff_timestamp` |
| dropoff before pickup | quarantine | `dropoff_before_pickup` |
| pickup month differs from file month | quarantine | `pickup_outside_file_period` |
| exact duplicate row within a file (all business columns equal) | quarantine | first copy kept, others `exact_duplicate_row` |
| negative `trip_distance` | warning | kept; `negative_distance_rows` |
| negative `fare_amount` | warning | kept; `negative_fare_rows` (refunds/adjustments may be legitimate) |
| pickup zone NULL or not in lookup | warning | kept with zone key -1; `unmapped_pickup_rows` |

Duplicate detection can be switched off with `--vars '{dedupe_exact_duplicates: false}'` if it proves too
expensive on the full history; measure before deciding.

## Layer D – reconciliation
`mart_quality_reconciliation` has one row per file. Singular test `assert_reconciliation_balances` fails unless
`source_rows = valid_rows + quarantined_rows` and `valid_rows = fact_rows`. Two warn-level singular tests surface
unmapped zones and negative amounts.

## Evidence on synthetic fixtures
Fixtures are random data with TLC-like schema (`scripts/make_fixtures.py`), with these defects injected per file:
dropoff before pickup, negative distance, unknown zone 999, missing pickup. Verified by `tests/integration`:
the 2025 `cbd_congestion_fee` column is detected as drift; 2 of 4 defects per file are quarantined, 2 are
warnings; a file missing a required column is quarantined and never loaded; an injected exact duplicate is
quarantined; a file re-delivered under the same name with different content is reloaded without duplicates and
equals a clean full refresh; incremental equals full refresh, and replay changes nothing.
These results prove the logic works; they say nothing about the quality of real TLC data.

## Findings from the real files (56 files, 2022-01 to 2026-08, run of 2026-10-20)
Full numbers: `docs/results/real-2022-2026_2026-10-20.md`.
- No unreadable files, duplicate months or gaps. 197,562,001 rows; every layer reconciles (valid + quarantined = source).
- `airport_fee` is spelled `Airport_fee` from 2023-02 onward (43 files). DuckDB treats column names case-insensitively
  and merges them; the audit reports `renamed_case` drift and `column_case_variant` findings.
- `cbd_congestion_fee` appears from 2025-01 and is non-NULL in all 2025-2026 rows.
- `request_source` (string) appears from 2026-06 (3 files), is not in the 2025-03-18 dictionary, is reported as
  `unknown_column` and is not loaded until its meaning is confirmed.
- 2023-01 to 2023-02 changed integer widths and `passenger_count`/`RatecodeID` from double to int64; `union_by_name`
  upcasts and staging uses `try_cast`.
- Quarantine: 22,897 rows (0.0116%): 19,908 dropoff before pickup, 2,974 pickup outside the file's month, 15 exact
  duplicates. 65.9% are in 2022; spikes in 2023-11, 2024-11, 2025-11 are unexplained.
- Warnings: 4,371,395 negative fares (2.21%; 5.85% in 2025, cause unknown); no negative distances; no unmapped pickup
  zones (IDs 264 and 265 count as mapped).
