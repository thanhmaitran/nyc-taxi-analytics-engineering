select
    d.full_date as trip_date,
    f.pickup_zone_key,
    z.zone_name as pickup_zone,
    z.borough as pickup_borough,
    count(*) as trip_count,
    sum(f.fare_amount) as total_fare_amount,
    sum(f.tip_amount) as total_tip_amount,
    sum(f.total_amount) as total_amount_sum,
    avg(f.trip_distance) as avg_trip_distance,
    sum(count(*)) over (
        partition by f.pickup_zone_key
        order by d.full_date
    ) as running_trip_count
from {{ ref('fact_trips') }} as f
inner join {{ ref('dim_datetime') }} as d on f.pickup_datetime_key = d.datetime_key
inner join {{ ref('dim_zone') }} as z on f.pickup_zone_key = z.zone_key
group by d.full_date, f.pickup_zone_key, z.zone_name, z.borough
