{{ config(severity='warn') }}

{# Negative distance/fare are reported, not removed: refunds and adjustments may be legitimate. #}

select
    source_file,
    negative_distance_rows,
    negative_fare_rows
from {{ ref('mart_quality_reconciliation') }}
where negative_distance_rows > 0 or negative_fare_rows > 0
