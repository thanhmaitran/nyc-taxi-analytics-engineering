# Data dictionary

## Source: Yellow Taxi trip records (TLC dictionary, 2025-03-18)
| Source column | Meaning | Used as |
|---|---|---|
| VendorID | TPEP provider: 1 Creative Mobile Technologies, 2 Curb Mobility, 6 Myle Technologies, 7 Helix | `vendor_id` |
| tpep_pickup_datetime | meter engaged | `pickup_datetime` (required) |
| tpep_dropoff_datetime | meter disengaged | `dropoff_datetime` (required) |
| passenger_count | passengers in vehicle | `passenger_count` |
| trip_distance | taximeter miles | `trip_distance` |
| RatecodeID | 1 Standard, 2 JFK, 3 Newark, 4 Nassau/Westchester, 5 Negotiated, 6 Group ride, 99 Null/unknown | `rate_code_id` |
| store_and_fwd_flag | Y/N store-and-forward | not carried |
| PULocationID / DOLocationID | TLC taxi zone at meter engaged / disengaged | `pickup_location_id` / `dropoff_location_id` (required) |
| payment_type | 0 Flex Fare, 1 Credit card, 2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided | `payment_type` → `dim_payment_type` |
| fare_amount | meter fare | `fare_amount` |
| extra, mta_tax, tolls_amount, improvement_surcharge | surcharges and taxes | not carried (add if a question needs them) |
| tip_amount | credit-card tips only | `tip_amount` |
| total_amount | total charged, excludes cash tips | `total_amount` |
| congestion_surcharge | NYS congestion surcharge | `congestion_surcharge` |
| airport_fee | LaGuardia / JFK pickups only | `airport_fee` |
| cbd_congestion_fee | MTA Congestion Relief Zone charge from 2025-01-05 | `cbd_congestion_fee` (NULL before 2025) |

Types after staging: ids and counts `integer`; timestamps `timestamp`; money and distance `decimal(18,2)`.

## Warehouse
**fact_trips** (grain: one retained source record)
`trip_record_key` (md5 of source file name + row number; technical key, not a TLC id) · `source_file` ·
`source_period` · `source_row_number` · `pickup_datetime_key` / `dropoff_datetime_key` (YYYYMMDDHH) ·
`pickup_zone_key` / `dropoff_zone_key` (-1 = unmapped) · `payment_type_key` (-1 = missing) · `vendor_id` ·
`passenger_count` · `trip_distance` · `fare_amount` · `tip_amount` · `total_amount` · `airport_fee` ·
`congestion_surcharge` · `cbd_congestion_fee`

**dim_zone**: `zone_key`, `location_id`, `zone_name`, `borough`, `service_zone` (from the official lookup; 265 zones
+ `Unmapped`). Lookup IDs 264 ("Unknown") and 265 ("Outside of NYC") are kept as published.
**dim_payment_type**: `payment_type_key`, `payment_type_code`, `payment_type_description` (codes 0–6 + -1).
**dim_datetime** (hour grain): `datetime_key`, `full_date`, `year`, `quarter`, `month`, `day_of_week` (ISO, 1=Mon),
`hour`, `is_weekend`.
**quarantine_trips**: key, file, row, `quarantine_reason`, and the raw fields needed to investigate.
