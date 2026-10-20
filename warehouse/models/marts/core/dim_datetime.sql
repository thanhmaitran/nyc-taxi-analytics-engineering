{# Hour grain. Built from the hours that occur in fact_trips (pickup or dropoff). Reads the fact table, not the
   197M-row intermediate view, so it is a cheap aggregation of two integer columns. #}

with hours as (

    select distinct pickup_datetime_key as datetime_key from {{ ref('fact_trips') }}
    union
    select distinct dropoff_datetime_key as datetime_key from {{ ref('fact_trips') }}

),

parsed as (

    select
        datetime_key,
        strptime(cast(datetime_key as varchar), '%Y%m%d%H') as hour_start
    from hours

)

select
    datetime_key,
    cast(hour_start as date) as full_date,
    cast(date_part('year', hour_start) as integer) as "year",  -- noqa: RF06
    cast(date_part('quarter', hour_start) as integer) as "quarter",  -- noqa: RF06
    cast(date_part('month', hour_start) as integer) as "month",  -- noqa: RF06
    cast(date_part('isodow', hour_start) as integer) as day_of_week,
    cast(date_part('hour', hour_start) as integer) as "hour",  -- noqa: RF06
    date_part('isodow', hour_start) in (6, 7) as is_weekend
from parsed
