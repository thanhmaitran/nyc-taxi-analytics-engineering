{# Rows rejected by an error-level rule, kept with the reason instead of being silently dropped. #}

select
    trip_record_key,
    source_file,
    source_period,
    source_row_number,
    quarantine_reason,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    trip_distance,
    fare_amount,
    total_amount
from {{ ref('int_trip_validated') }}
where not is_valid
