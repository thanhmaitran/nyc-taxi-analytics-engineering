{# Rows rejected by a quarantine rule, kept with the reason instead of being silently dropped.
   Incremental per source file, exactly like fact_trips. #}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_file',
        pre_hook="{{ drop_reprocessed_files() }}"
    )
}}

select
    trip_record_key,
    source_file,
    source_period,
    source_row_number,
    quarantine_reason,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    trip_distance,
    fare_amount,
    total_amount
from {{ ref('int_trip_validated') }}
where
    not is_valid
    {% if is_incremental() %}
        and ({{ incremental_file_filter() }})
    {% endif %}
