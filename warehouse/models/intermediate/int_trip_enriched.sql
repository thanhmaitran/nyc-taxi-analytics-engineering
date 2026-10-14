{# Valid rows only, with surrogate keys to the dimensions. Unmapped zones map to key -1. #}

select
    trip_record_key,
    source_file,
    source_period,
    source_row_number,
    cast(strftime(pickup_datetime, '%Y%m%d%H') as integer) as pickup_datetime_key,
    cast(strftime(dropoff_datetime, '%Y%m%d%H') as integer) as dropoff_datetime_key,
    case when is_unmapped_pickup_zone then -1 else pickup_location_id end as pickup_zone_key,
    case
        when
            dropoff_location_id is null
            or dropoff_location_id not in (select z.location_id from {{ ref('taxi_zone_lookup_clean') }} as z)
            then -1
        else dropoff_location_id
    end as dropoff_zone_key,
    coalesce(payment_type, -1) as payment_type_key,
    pickup_datetime,
    dropoff_datetime,
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    airport_fee,
    congestion_surcharge,
    cbd_congestion_fee,
    is_negative_distance,
    is_negative_fare,
    is_unmapped_pickup_zone
from {{ ref('int_trip_validated') }}
where is_valid
