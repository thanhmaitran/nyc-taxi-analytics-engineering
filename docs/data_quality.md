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
