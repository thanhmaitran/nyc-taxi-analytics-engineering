{# Applies the published validation rules (config/data_contract.yml). Nothing is deleted here:
   every staging row appears once, flagged, so the quarantine can be audited. #}

with staged as (

    select
        *,
        md5(
            concat_ws(
                '|',
                coalesce(cast(pickup_datetime as varchar), ''),
                coalesce(cast(dropoff_datetime as varchar), ''),
                coalesce(cast(pickup_location_id as varchar), ''),
                coalesce(cast(dropoff_location_id as varchar), ''),
                coalesce(cast(vendor_id as varchar), ''),
                coalesce(cast(passenger_count as varchar), ''),
                coalesce(cast(trip_distance as varchar), ''),
                coalesce(cast(payment_type as varchar), ''),
                coalesce(cast(fare_amount as varchar), ''),
                coalesce(cast(tip_amount as varchar), ''),
                coalesce(cast(total_amount as varchar), '')
            )
        ) as business_row_hash
    from {{ ref('stg_yellow_taxi') }}

),

numbered as (

    select
        *,
        {% if var('dedupe_exact_duplicates', true) %}
            row_number() over (
                partition by source_file, business_row_hash
                order by source_row_number
            )
        {% else %}
            1
        {% endif %} as duplicate_rank
    from staged

),

flagged as (

    select
        *,
        pickup_datetime is null as is_missing_pickup,
        dropoff_datetime is null as is_missing_dropoff,
        coalesce(dropoff_datetime < pickup_datetime, false) as is_dropoff_before_pickup,
        coalesce(
            strftime(pickup_datetime, '%Y-%m') != source_period, false
        ) as is_pickup_outside_file_period,
        duplicate_rank > 1 as is_exact_duplicate,
        coalesce(trip_distance < 0, false) as is_negative_distance,
        coalesce(fare_amount < 0, false) as is_negative_fare,
        pickup_location_id is null
        or pickup_location_id not in (select z.location_id from {{ ref('taxi_zone_lookup_clean') }} as z)
            as is_unmapped_pickup_zone
    from numbered

)

select
    *,
    case
        when is_missing_pickup then 'missing_pickup_timestamp'
        when is_missing_dropoff then 'missing_dropoff_timestamp'
        when is_dropoff_before_pickup then 'dropoff_before_pickup'
        when is_pickup_outside_file_period then 'pickup_outside_file_period'
        when is_exact_duplicate then 'exact_duplicate_row'
    end as quarantine_reason,
    quarantine_reason is null as is_valid
from flagged
