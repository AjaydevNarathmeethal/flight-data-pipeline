{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='log_id',
        partition_by={
            "field": "log_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["primary_error_reason", "icao24"]
    )
}}

with raw_source as (
    select 
        -- Create a unique log hash to handle updates and deduplication
        to_hex(md5(concat(
            coalesce(cast(icao24 as string), ''), 
            coalesce(cast(time_position as string), ''), 
            coalesce(cast(_gcs_loaded_at as string), '')
        ))) as log_id,
        
        -- Use SAFE_CAST to protect the pipeline against data type corruption
        safe_cast(icao24 as string) as icao24,
        safe_cast(callsign as string) as callsign,
        safe_cast(latitude as float64) as latitude,
        safe_cast(longitude as float64) as longitude,
        safe_cast(origin_country as string) as origin_country,
        
        -- Convert epoch integers safely into timestamps
        safe.timestamp_seconds(safe_cast(time_position as int64)) as time_position,
        safe.timestamp_seconds(safe_cast(_gcs_loaded_at as int64)) as gcs_loaded_at
        
    from {{ source('bronze', 'gcs_raw_data') }}

    where 1=1
    
    {# Incremental run window filter #}
    {% if is_incremental() %}
      and safe.timestamp_seconds(safe_cast(_gcs_loaded_at as int64)) > (select max(gcs_loaded_at) from {{ this }})
    {% endif %}
),

classified_logs as (
    select 
        *,
        -- Isolate records by evaluating specific engineering rules
        case 
            when icao24 is null or trim(icao24) = '' then 'MISSING_AIRCRAFT_ID'
            when latitude is null or longitude is null then 'MISSING_GEOSPATIAL_COORDINATES'
            when gcs_loaded_at is null then 'INVALID_METADATA_TIMESTAMP'
            else 'VALID'
        end as primary_error_reason,
        
        -- Create the static target partition date field
        case 
            when gcs_loaded_at is not null then date(gcs_loaded_at)
            else current_date()
        end as log_date
        
    from raw_source
)

-- Output ONLY the filtered quarantine logs into this model
select 
    log_id,
    log_date,
    primary_error_reason,
    icao24,
    callsign,
    latitude,
    longitude,
    origin_country,
    time_position,
    gcs_loaded_at
from classified_logs
where primary_error_reason != 'VALID'
