{% set raw = source('raw', 'raw_yellow_taxi') %}

with source_data as (

    select
        *,
        regexp_extract(filename, '[^/\\]+$') as source_file_name
    from {{ raw }}

),

renamed as (

    select
        md5(source_file_name || ':' || cast(file_row_number as varchar)) as trip_record_key,

        source_file_name as source_file,
        regexp_extract(source_file_name, '(\d{4}-\d{2})', 1) as source_period,
        file_row_number as source_row_number,

        {{ col_or_null(raw, 'VendorID', 'integer') }} as vendor_id,
        {{ col_or_null(raw, 'tpep_pickup_datetime', 'timestamp') }} as pickup_datetime,
        {{ col_or_null(raw, 'tpep_dropoff_datetime', 'timestamp') }} as dropoff_datetime,
        {{ col_or_null(raw, 'passenger_count', 'integer') }} as passenger_count,
        {{ col_or_null(raw, 'trip_distance', 'decimal(18,2)') }} as trip_distance,
        {{ col_or_null(raw, 'RatecodeID', 'integer') }} as rate_code_id,
        {{ col_or_null(raw, 'PULocationID', 'integer') }} as pickup_location_id,
        {{ col_or_null(raw, 'DOLocationID', 'integer') }} as dropoff_location_id,
        {{ col_or_null(raw, 'payment_type', 'integer') }} as payment_type,
        {{ col_or_null(raw, 'fare_amount', 'decimal(18,2)') }} as fare_amount,
        {{ col_or_null(raw, 'tip_amount', 'decimal(18,2)') }} as tip_amount,
        {{ col_or_null(raw, 'total_amount', 'decimal(18,2)') }} as total_amount,
        {{ col_or_null(raw, 'airport_fee', 'decimal(18,2)') }} as airport_fee,
        {{ col_or_null(raw, 'congestion_surcharge', 'decimal(18,2)') }} as congestion_surcharge,
        {{ col_or_null(raw, 'cbd_congestion_fee', 'decimal(18,2)') }} as cbd_congestion_fee

    from source_data

)

select * from renamed
