{{ config(severity='warn') }}

{# Coverage monitor: warns when valid trips have a pickup zone missing from the TLC lookup. #}

select
    source_file,
    unmapped_pickup_rows
from {{ ref('mart_quality_reconciliation') }}
where unmapped_pickup_rows > 0
