{{
    config(
        materialized='table',
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["airspace_name"]
    )
}}

select 
    date(loaded_at_timestamp) as snapshot_date,
    -- Truncate granular time windows directly to clean hour slices
    timestamp_trunc(loaded_at_timestamp, hour) as snapshot_hour,
    -- Round spatial coordinates to 1 decimal place (~11km cells) to bundle rows
    round(latitude, 1) as rounded_latitude,
    round(longitude, 1) as rounded_longitude,
    airspace_name,
    count(*) as total_flight_records

from {{ ref('fct_flight_snapshots') }}
-- Safeguard: Skip grouping coordinates that are completely missing
where latitude is not null 
  and longitude is not null

group by 1, 2, 3, 4, 5
