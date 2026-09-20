{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

/*
    Fact table for earthquake events
    Incremental load - only processes new or updated events
*/

WITH staged AS (
    SELECT s.* FROM {{ ref('stg_earthquakes') }} AS s
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS t
        WHERE t.event_id = s.event_id
          AND COALESCE(t.updated_timestamp, t.event_timestamp)
              >= COALESCE(s.updated_timestamp, s.event_timestamp)
    )
    {% endif %}
)

SELECT
    -- Keys
    event_id,
    event_date,
    
    -- Dimensions
    region,
    magnitude_category,
    depth_category,
    source_network,
    event_type,
    status,
    
    -- Facts/Measures
    magnitude,
    depth_km,
    latitude,
    longitude,
    
    -- Quality scores
    significance,
    station_count,
    travel_time_residual,
    azimuthal_gap,
    nearest_station_distance,
    
    -- Impact
    felt_reports,
    community_intensity,
    mercalli_intensity,
    alert_level,
    has_tsunami_flag,
    
    -- Time dimensions
    event_timestamp,
    event_hour,
    day_of_week,
    updated_timestamp,
    
    -- Metadata
    place,
    title,
    detail_url,
    
    -- Audit
    ingested_at,
    dbt_updated_at
    
FROM staged
