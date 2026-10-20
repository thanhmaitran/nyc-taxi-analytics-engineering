select
    location_id as zone_key,
    location_id,
    zone_name,
    borough,
    service_zone
from {{ ref('taxi_zone_lookup_clean') }}

union all

select
    -1 as zone_key,
    -1 as location_id,
    'Unmapped' as zone_name,
    'Unmapped' as borough,
    'Unmapped' as service_zone
