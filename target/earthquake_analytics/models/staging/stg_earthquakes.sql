{{
    config(
        materialized='view',
        
    )
}}

/*
    Staging model for earthquake data
    Transforms raw earthquake data with proper data types and cleaning
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_earthquakes') }}
),

cleaned AS (
    SELECT
        -- Primary key
        event_id,
        
        -- Magnitude info
        magnitude,
        mag_type,
        CASE 
            WHEN magnitude < 2.0 THEN 'micro'
            WHEN magnitude >= 2.0 AND magnitude < 4.0 THEN 'minor'
            WHEN magnitude >= 4.0 AND magnitude < 5.0 THEN 'light'
            WHEN magnitude >= 5.0 AND magnitude < 6.0 THEN 'moderate'
            WHEN magnitude >= 6.0 AND magnitude < 7.0 THEN 'strong'
            WHEN magnitude >= 7.0 AND magnitude < 8.0 THEN 'major'
            WHEN magnitude >= 8.0 THEN 'great'
            ELSE 'unknown'
        END AS magnitude_category,
        
        -- Location info
        place,
        longitude,
        latitude,
        depth_km,
        CASE 
            WHEN depth_km < 70 THEN 'shallow'
            WHEN depth_km >= 70 AND depth_km < 300 THEN 'intermediate'
            WHEN depth_km >= 300 THEN 'deep'
            ELSE 'unknown'
        END AS depth_category,
        
        -- Extract region from place (text after "of" or full place)
        CASE 
            WHEN POSITION(' of ' IN place) > 0 
            THEN TRIM(SUBSTRING(place, POSITION(' of ' IN place) + 4))
            ELSE place
        END AS region,
        
        -- Time info - convert Unix ms to timestamp
        TO_TIMESTAMP_NTZ(event_time / 1000) AS event_timestamp,
        DATE(TO_TIMESTAMP_NTZ(event_time / 1000)) AS event_date,
        HOUR(TO_TIMESTAMP_NTZ(event_time / 1000)) AS event_hour,
        DAYOFWEEK(TO_TIMESTAMP_NTZ(event_time / 1000)) AS day_of_week,
        TO_TIMESTAMP_NTZ(updated_time / 1000) AS updated_timestamp,
        
        -- Quality metrics
        status,
        significance,
        num_stations AS station_count,
        rms AS travel_time_residual,
        gap AS azimuthal_gap,
        distance AS nearest_station_distance,
        
        -- Impact metrics
        felt_reports,
        cdi AS community_intensity,
        mmi AS mercalli_intensity,
        alert_level,
        tsunami_flag,
        CASE WHEN tsunami_flag = 1 THEN TRUE ELSE FALSE END AS has_tsunami_warning,
        
        -- Metadata
        network AS source_network,
        event_type,
        event_code,
        title,
        detail_url,
        
        -- Audit fields
        ingested_at,
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM source
    WHERE event_id IS NOT NULL
      AND magnitude IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT * FROM cleaned
