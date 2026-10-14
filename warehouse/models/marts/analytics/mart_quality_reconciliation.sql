{# Per-file row reconciliation: source = valid + quarantined, valid = fact rows. #}

with validated as (

    select
        source_file,
        source_period,
        count(*) as source_rows,
        count(pickup_datetime) as parsed_pickup_rows,
        count_if(is_valid) as valid_rows,
        count_if(not is_valid) as quarantined_rows,
        count_if(is_exact_duplicate) as exact_duplicate_rows,
        count_if(is_dropoff_before_pickup) as dropoff_before_pickup_rows,
        count_if(is_pickup_outside_file_period) as pickup_outside_period_rows,
        count_if(is_valid and is_negative_distance) as negative_distance_rows,
        count_if(is_valid and is_negative_fare) as negative_fare_rows,
        count_if(is_valid and is_unmapped_pickup_zone) as unmapped_pickup_rows
    from {{ ref('int_trip_validated') }}
    group by source_file, source_period

),

fact as (

    select
        source_file,
        count(*) as fact_rows
    from {{ ref('fact_trips') }}
    group by source_file

)

select
    v.*,
    coalesce(f.fact_rows, 0) as fact_rows,
    round(100.0 * v.quarantined_rows / v.source_rows, 4) as quarantine_pct
from validated as v
left join fact as f on v.source_file = f.source_file
