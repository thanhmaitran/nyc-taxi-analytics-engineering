{# Unmapped pickups stay in the table as zone_key -1 and are excluded from the ranking. #}

with monthly as (

    select
        date_trunc('month', d.full_date) as trip_month,
        f.pickup_zone_key,
        z.zone_name as pickup_zone,
        z.borough as pickup_borough,
        count(*) as trip_count,
        sum(f.total_amount) as total_amount_sum,
        avg(f.trip_distance) as avg_trip_distance
    from {{ ref('fact_trips') }} as f
    inner join {{ ref('dim_datetime') }} as d on f.pickup_datetime_key = d.datetime_key
    inner join {{ ref('dim_zone') }} as z on f.pickup_zone_key = z.zone_key
    group by date_trunc('month', d.full_date), f.pickup_zone_key, z.zone_name, z.borough

)

select
    *,
    case
        when pickup_zone_key = -1 then null
        else dense_rank() over (
            partition by trip_month, (pickup_zone_key = -1)
            order by trip_count desc
        )
    end as trip_volume_rank,
    trip_count - lag(trip_count) over (
        partition by pickup_zone_key order by trip_month
    ) as trip_count_change_vs_prev_month
from monthly
