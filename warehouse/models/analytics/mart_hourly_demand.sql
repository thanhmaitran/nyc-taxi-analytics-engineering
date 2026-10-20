select
    d.full_date as trip_date,
    d.hour as pickup_hour,
    d.day_of_week,
    d.is_weekend,
    count(*) as trip_count,
    sum(f.total_amount) as total_amount_sum,
    avg(f.trip_distance) as avg_trip_distance
from {{ ref('fact_trips') }} as f
inner join {{ ref('dim_datetime') }} as d on f.pickup_datetime_key = d.datetime_key
group by d.full_date, d.hour, d.day_of_week, d.is_weekend
