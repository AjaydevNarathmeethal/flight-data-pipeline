{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key= ['icao24','loaded_at_timestamp'],
        partition_by={
            "field": "loaded_at_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['icao24', 'origin_country']
    )
}}

-- there can be problem with using 2 columns as unique_key, if there is error should use a 'surrogate_key'
-- which should be defined in the model like { {dbt_utils.generate_surrogate_key(['icao24', 'loaded_at_timestamp'])} } as surrogate_key,

with new_data as (
    select
        SAFE_CAST(icao24 AS STRING) as icao24,
        SAFE_CAST(callsign AS STRING) as callsign,
        SAFE_CAST(origin_country AS STRING) as origin_country,
        SAFE_CAST(latitude AS FLOAT64) as latitude,
        SAFE_CAST(longitude AS FLOAT64) as longitude,
        SAFE_CAST(baro_altitude AS FLOAT64) as baro_altitude,
        SAFE_CAST(on_ground AS BOOLEAN) as on_ground,
        SAFE_CAST(velocity AS FLOAT64) as velocity,
        SAFE_CAST(true_track AS FLOAT64) as true_track,
        SAFE_CAST(vertical_rate AS FLOAT64) as vertical_rate,
        SAFE_CAST(geo_altitude AS FLOAT64) as geo_altitude,
        SAFE_CAST(squawk AS STRING) as squawk,
        SAFE_CAST(spi AS BOOLEAN) as spi,
        SAFE_CAST(position_source AS INT64) as position_source,
        -- Convert Unix Epoch to BigQuery TIMESTAMP
        TIMESTAMP_SECONDS(SAFE_CAST(time_position AS INT64)) as flight_timestamp,
        TIMESTAMP_SECONDS(SAFE_CAST(last_contact AS INT64)) as last_contact_timestamp,
        TIMESTAMP_SECONDS(SAFE_CAST(_gcs_loaded_at AS INT64)) as loaded_at_timestamp
    from {{ source('bronze', 'gcs_raw_data') }}
    
    {% if is_incremental() %}
        -- Only process data loaded to GCS after the last run
        where TIMESTAMP_SECONDS(SAFE_CAST(_gcs_loaded_at AS INT64)) > (select max(loaded_at_timestamp) from {{ this }})
    {% endif %}
),


-- script to check if there are any duplicates - this is a redundancy step. the data from API are supposed to be unique for each icao24
-- however since this model has icao24 and loaded_at_timestamp 

deduplicated as (
    select *,
        row_number() over (
            partition by icao24, loaded_at_timestamp 
            order by loaded_at_timestamp desc
        ) as rn
    from new_data
)


select 
    icao24, callsign, origin_country, latitude, longitude,
    baro_altitude, on_ground, velocity, true_track, vertical_rate,
    geo_altitude, squawk, spi, position_source,
    flight_timestamp, last_contact_timestamp, loaded_at_timestamp
from deduplicated
where rn = 1    