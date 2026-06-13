{{ config(materialized='table') }}

select 
    safe_cast(airspace_id as string) as airspace_id,
    safe_cast(airspace_name as string) as airspace_name,
    -- Safely compile WKT strings into native BigQuery GEOGRAPHY geometry shapes
    safe.st_geogfromtext(wkt_geometry) as airspace_polygon
from {{ ref('seed_airspace_polygons') }}
