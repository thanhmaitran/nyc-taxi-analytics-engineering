# Metric definitions

Definitions are checked against the TLC *Data Dictionary – Yellow Taxi Trip Records* (2025-03-18).

**Population ("trips")**: rows in `fact_trips` = source rows that passed every quarantine rule
(`docs/data_quality.md`). The grain is *one retained source record*, not a verified real-world taxi trip, because
the source has no trip identifier.

| Metric | Definition | Notes |
|---|---|---|
| `trip_count` | `count(*)` over `fact_trips` rows in the grouping | quarantined rows excluded |
| `total_fare_amount` | `sum(fare_amount)` | meter time-and-distance fare |
| `total_tip_amount` | `sum(tip_amount)` | credit-card tips only; cash tips are not recorded (dictionary) |
| `total_amount_sum` | `sum(total_amount)` | taken as published, never rebuilt from components; excludes cash tips |
| `avg_trip_distance` | `avg(trip_distance)` in miles | negative distances are kept and reported, so they lower the average |
| `invalid_trip_count` | `quarantined_rows` in `mart_quality_reconciliation` | rows hit by a quarantine rule |
| `unmapped_pickup_count` | `unmapped_pickup_rows` in `mart_quality_reconciliation` | valid rows whose pickup zone is NULL or absent from the lookup |
| `quarantine_pct` | `100 * quarantined_rows / source_rows` | per file |

## Numeric precision
Monetary amounts and distance are cast to `decimal(18,2)` in staging (ADR 0003). Sums are therefore exact and
independent of row order; incremental and full-refresh marts compare equal row for row. Source values with more
than two decimals are rounded to two.

## Marts and grain
| Model | Grain |
|---|---|
| `mart_daily_operations` | pickup date × pickup zone (includes running trip count per zone) |
| `mart_hourly_demand` | pickup date × pickup hour |
| `mart_zone_performance` | pickup month × pickup zone (rank by trip count, change vs previous month) |
| `mart_quality_reconciliation` | one row per source file |

Pickup zone key `-1` means "Unmapped" and is excluded from `trip_volume_rank`.
