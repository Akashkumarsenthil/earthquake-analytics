{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Hourly Heatmap Aggregation
    Pre-computes earthquake patterns by hour and day of week
    Useful for temporal pattern visualization
*/

WITH hourly_patterns AS (
    SELECT
        day_of_week,
        event_hour,
        
        -- Day names for display
        CASE day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        
        -- Counts
        COUNT(*) AS earthquake_count,
        AVG(magnitude) AS avg_magnitude,
        MAX(magnitude) AS max_magnitude,
        
        -- Significant events
        COUNT(CASE WHEN magnitude >= 4.0 THEN 1 END) AS significant_count,
        
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM {{ ref('fct_earthquakes') }}
    GROUP BY day_of_week, event_hour
)

SELECT * FROM hourly_patterns
ORDER BY day_of_week, event_hour
