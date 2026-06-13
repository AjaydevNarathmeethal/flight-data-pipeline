{{ config(materialized='table') }}

with latest_snapshot_window as (
    select max(loaded_at_timestamp) as max_loaded_at 
    from {{ ref('fct_flight_snapshots') }}
)

select 
    snapshot_id,
    loaded_at_timestamp,
    flight_timestamp,
    icao24,
    callsign,
    origin_country,
    latitude,
    longitude,
    baro_altitude,
    velocity,
    airspace_name

from {{ ref('fct_flight_snapshots') }}
-- Isolate and serve exactly the latest telemetry payload resting in your system
where loaded_at_timestamp = (select max_loaded_at from latest_snapshot_window)
