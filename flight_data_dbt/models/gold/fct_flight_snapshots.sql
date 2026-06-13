{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='snapshot_id',
        partition_by={
            "field": "flight_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["airspace_name", "icao24"]
    )
}}

with silver_source as (
    select *,
        -- Primary Key Strategy: Leak-proof surrogate hash using icao24 + loaded_at_timestamp
        to_hex(md5(concat(icao24, cast(loaded_at_timestamp as string)))) as snapshot_id,
        date(loaded_at_timestamp) as flight_date
    from {{ ref('stg_flights') }}


    {% if is_incremental() %}
      -- Process incoming snapshot records incrementally based on the load timeline
      WHERE loaded_at_timestamp > (select max(loaded_at_timestamp) from {{ this }})
    {% endif %}
),

airspace_master as (
    select airspace_name, airspace_polygon from {{ ref('dim_spatial_airspaces') }}
    where airspace_polygon is not null
)

select 
    s.snapshot_id,
    s.flight_date,
    s.loaded_at_timestamp,
    s.flight_timestamp,
    s.icao24,
    s.callsign, -- Retains NULL values safely
    s.origin_country,
    s.latitude,
    s.longitude,
    s.baro_altitude,
    s.velocity,
    s.on_ground,
    -- Perform spatial analysis only if both points exist; handles NULL rows safely
    coalesce(am.airspace_name, 'International Airspace') as airspace_name

from silver_source s
left join airspace_master am 
    on safe.st_within(safe.st_geogpoint(s.longitude, s.latitude), am.airspace_polygon)

-- Enforce the 'LIMIT 1' constraint per snapshot row if multi-polygon border overlap occurs
qualify row_number() over (partition by s.snapshot_id order by am.airspace_name) = 1
