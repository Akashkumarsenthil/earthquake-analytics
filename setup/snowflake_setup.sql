
-- Earthquake Analytics - Snowflake Setup Script
-- Run this in Snowflake Worksheet to create necessary objects


-- Use your database
USE DATABASE USER_DB_PLATYPUS;

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Switch to RAW schema for raw table creation
USE SCHEMA RAW;

-- Create the raw earthquakes table
CREATE TABLE IF NOT EXISTS raw_earthquakes (
    event_id VARCHAR(50) PRIMARY KEY,
    magnitude FLOAT,
    mag_type VARCHAR(10),
    place VARCHAR(500),
    event_time BIGINT,                    -- Unix timestamp in milliseconds
    updated_time BIGINT,
    timezone_offset INT,
    felt_reports INT,
    cdi FLOAT,                            -- Community Decimal Intensity
    mmi FLOAT,                            -- Modified Mercalli Intensity
    alert_level VARCHAR(20),
    status VARCHAR(20),
    tsunami_flag INT,
    significance INT,
    network VARCHAR(10),
    event_code VARCHAR(20),
    num_stations INT,
    distance FLOAT,                       -- Distance to nearest station
    rms FLOAT,                            -- Root mean square travel time
    gap FLOAT,                            -- Azimuthal gap
    event_type VARCHAR(50),
    title VARCHAR(500),
    longitude FLOAT,
    latitude FLOAT,
    depth_km FLOAT,
    detail_url VARCHAR(1000),
    ingested_at TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create index-like cluster key for performance (optional)
-- ALTER TABLE raw_earthquakes CLUSTER BY (event_time);

-- Grant permissions (adjust role as needed)
GRANT ALL ON SCHEMA RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON SCHEMA ANALYTICS TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE ACCOUNTADMIN;

-- Verify setup
SHOW SCHEMAS IN DATABASE USER_DB_PLATYPUS;
SHOW TABLES IN SCHEMA RAW;

-- Sample query to verify data after ingestion
/*
-- Check raw data count
SELECT COUNT(*) FROM RAW.raw_earthquakes;

-- Check recent earthquakes
SELECT 
    event_id,
    TO_TIMESTAMP_NTZ(event_time/1000) as event_time,
    magnitude,
    place,
    latitude,
    longitude
FROM RAW.raw_earthquakes
ORDER BY event_time DESC
LIMIT 10;

-- Check dbt models after running
SELECT COUNT(*) FROM ANALYTICS.fct_earthquakes;
SELECT * FROM ANALYTICS.agg_daily_summary ORDER BY summary_date DESC LIMIT 10;
SELECT * FROM ANALYTICS.agg_regional_risk ORDER BY risk_score DESC LIMIT 10;
*/
