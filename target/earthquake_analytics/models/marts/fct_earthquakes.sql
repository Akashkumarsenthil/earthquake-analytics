{{
    config(
        materialized='incremental',
        unique_key='event_id',
        
        incremental_strategy='merge',
        merge_update_columns=['magnitude', 'updated_timestamp', 'status', 'felt_reports', 'community_intensity', 'alert_level', 'dbt_updated_at']
    )
}}

/*
    Fact table for earthquake events
    Incremental load - only processes new or updated events
*/

WITH staged AS (
    SELECT * FROM {{ ref('stg_earthquakes') }}
    {% if is_incremental() %}
    WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})
       OR event_id NOT IN (SELECT event_id FROM {{ this }})
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
    has_tsunami_warning,
    
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
