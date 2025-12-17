{{
    config(
        materialized='incremental',
        unique_key='summary_date',
        
        incremental_strategy='merge'
    )
}}

/*
    Daily summary aggregation for dashboard KPIs
    Pre-computes daily metrics for fast dashboard loading
*/

WITH daily_stats AS (
    SELECT
        event_date AS summary_date,
        
        -- Counts
        COUNT(*) AS total_earthquakes,
        COUNT(CASE WHEN magnitude >= 4.0 THEN 1 END) AS significant_earthquakes,
        COUNT(CASE WHEN magnitude >= 5.0 THEN 1 END) AS strong_earthquakes,
        COUNT(CASE WHEN has_tsunami_warning THEN 1 END) AS tsunami_warnings,
        
        -- Magnitude stats
        AVG(magnitude) AS avg_magnitude,
        MAX(magnitude) AS max_magnitude,
        MIN(magnitude) AS min_magnitude,
        STDDEV(magnitude) AS stddev_magnitude,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY magnitude) AS median_magnitude,
        
        -- Depth stats
        AVG(depth_km) AS avg_depth_km,
        MAX(depth_km) AS max_depth_km,
        
        -- Impact stats
        SUM(COALESCE(felt_reports, 0)) AS total_felt_reports,
        AVG(COALESCE(significance, 0)) AS avg_significance,
        
        -- Category breakdowns
        COUNT(CASE WHEN magnitude_category = 'micro' THEN 1 END) AS micro_count,
        COUNT(CASE WHEN magnitude_category = 'minor' THEN 1 END) AS minor_count,
        COUNT(CASE WHEN magnitude_category = 'light' THEN 1 END) AS light_count,
        COUNT(CASE WHEN magnitude_category = 'moderate' THEN 1 END) AS moderate_count,
        COUNT(CASE WHEN magnitude_category = 'strong' THEN 1 END) AS strong_count,
        COUNT(CASE WHEN magnitude_category = 'major' THEN 1 END) AS major_count,
        COUNT(CASE WHEN magnitude_category = 'great' THEN 1 END) AS great_count,
        
        -- Depth category breakdown
        COUNT(CASE WHEN depth_category = 'shallow' THEN 1 END) AS shallow_count,
        COUNT(CASE WHEN depth_category = 'intermediate' THEN 1 END) AS intermediate_count,
        COUNT(CASE WHEN depth_category = 'deep' THEN 1 END) AS deep_count,
        
        -- Quality
        AVG(station_count) AS avg_station_count,
        COUNT(CASE WHEN status = 'reviewed' THEN 1 END) AS reviewed_count,
        
        -- Audit
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM {{ ref('fct_earthquakes') }}
    {% if is_incremental() %}
    WHERE event_date >= (SELECT MAX(summary_date) - INTERVAL '1 day' FROM {{ this }})
    {% endif %}
    GROUP BY event_date
)

SELECT * FROM daily_stats
