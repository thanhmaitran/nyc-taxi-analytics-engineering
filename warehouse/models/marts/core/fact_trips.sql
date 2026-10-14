{#
  Incremental strategy: delete+insert keyed on source_file (one monthly file = one partition).
  - A file already loaded is skipped on later runs -> replaying a month cannot duplicate rows.
  - A file listed in var `reprocess_files` (checksum changed) is deleted and re-inserted whole.
  - `--full-refresh` rebuilds everything; results must match the incremental path.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_file'
    )
}}

select
    trip_record_key,
    source_file,
    source_period,
    source_row_number,
    pickup_datetime_key,
    dropoff_datetime_key,
    pickup_zone_key,
    dropoff_zone_key,
    payment_type_key,
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    airport_fee,
    congestion_surcharge,
    cbd_congestion_fee
from {{ ref('int_trip_enriched') }}
{% if is_incremental() %}
    where
        source_file not in (select distinct t.source_file from {{ this }} as t)
        {% if var('reprocess_files', []) | length > 0 %}
            or source_file in ({{ quoted_list(var('reprocess_files')) }})
        {% endif %}
{% endif %}
