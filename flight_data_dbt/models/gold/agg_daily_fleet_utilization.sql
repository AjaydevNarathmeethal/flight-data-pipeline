{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='aggregation_id',
        partition_by={
            "field": "flight_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with granular_data as (
    select 
        flight_date,
        loaded_at_timestamp,
        origin_country,
        icao24,
        -- Safely pull the 3-letter carrier prefix from flight codes (e.g., 'AAL' from 'AAL123')
        case 
            when callsign is not null and length(trim(callsign)) >= 3 
            then upper(substr(trim(callsign), 1, 3))
            else 'UNKNOWN_CARRIER'
        end as airline_code
    from {{ ref('fct_flight_snapshots') }}

    {% if is_incremental() %}
      where flight_date >= (select max(flight_date) from {{ this }})
    {% endif %}
),

daily_metrics as (
    select 
        flight_date,
        origin_country,
        airline_code,
        count(*) as total_telemetry_pings,
        count(distinct icao24) as unique_airframes_tracked
    from granular_data
    group by 1, 2, 3
)

select 
    -- Generate unique target record strings to prevent data duplication
    to_hex(md5(concat(cast(flight_date as string), coalesce(origin_country, ''), airline_code))) as aggregation_id,
    flight_date,
    origin_country,
    airline_code,
    total_telemetry_pings,
    unique_airframes_tracked
from daily_metrics
