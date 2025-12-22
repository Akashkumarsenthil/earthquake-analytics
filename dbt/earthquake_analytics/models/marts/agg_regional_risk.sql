{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Regional Risk Analysis
    Aggregates earthquake data by region to calculate risk scores
*/

WITH regional_stats AS (
    SELECT
        region,
        
        -- Basic counts
        COUNT(*) AS total_earthquakes,
        COUNT(CASE WHEN event_date >= CURRENT_DATE - 30 THEN 1 END) AS earthquakes_last_30_days,
        COUNT(CASE WHEN event_date >= CURRENT_DATE - 7 THEN 1 END) AS earthquakes_last_7_days,
        
        -- Magnitude stats
        AVG(magnitude) AS avg_magnitude,
        MAX(magnitude) AS max_magnitude,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY magnitude) AS p95_magnitude,
        
        -- Significant event counts
        COUNT(CASE WHEN magnitude >= 4.0 THEN 1 END) AS significant_events,
        COUNT(CASE WHEN magnitude >= 5.0 THEN 1 END) AS strong_events,
        COUNT(CASE WHEN magnitude >= 6.0 THEN 1 END) AS major_events,
        
        -- Depth
        AVG(depth_km) AS avg_depth_km,
        
        -- Impact
        SUM(COALESCE(felt_reports, 0)) AS total_felt_reports,
        COUNT(CASE WHEN has_tsunami_warning THEN 1 END) AS tsunami_warnings,
        
        -- Time range
        MIN(event_date) AS first_event_date,
        MAX(event_date) AS last_event_date,
        DATEDIFF('day', MIN(event_date), MAX(event_date)) + 1 AS days_with_data,
        
        -- Representative coordinates (centroid)
        AVG(latitude) AS centroid_latitude,
        AVG(longitude) AS centroid_longitude
        
    FROM {{ ref('fct_earthquakes') }}
    WHERE region IS NOT NULL
    GROUP BY region
),

risk_scored AS (
    SELECT
        *,
        
        -- Calculate daily frequency
        total_earthquakes::FLOAT / NULLIF(days_with_data, 0) AS daily_frequency,
        
        -- Risk score calculation (weighted factors)
        (
            -- Frequency component (normalized by 365 days)
            (total_earthquakes::FLOAT / NULLIF(days_with_data, 0)) * 10 +
            -- Magnitude component
            avg_magnitude * 5 +
            -- Max magnitude impact
            CASE WHEN max_magnitude >= 6.0 THEN 20
                 WHEN max_magnitude >= 5.0 THEN 10
                 WHEN max_magnitude >= 4.0 THEN 5
                 ELSE 1 END +
            -- Recent activity boost
            earthquakes_last_30_days * 0.5 +
            -- Tsunami risk
            tsunami_warnings * 15
        ) AS raw_risk_score
        
    FROM regional_stats
),

risk_categorized AS (
    SELECT
        *,
        -- Normalize risk score to 0-100 scale
        LEAST(100, raw_risk_score) AS risk_score,
        
        -- Risk category
        CASE 
            WHEN raw_risk_score >= 50 THEN 'high'
            WHEN raw_risk_score >= 25 THEN 'moderate'
            WHEN raw_risk_score >= 10 THEN 'low'
            ELSE 'minimal'
        END AS risk_category,
        
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM risk_scored
)

SELECT * FROM risk_categorized
ORDER BY risk_score DESC
