-- ============================================================================
-- DIMENSION: DIM_TIME
-- ============================================================================
-- Purpose: Time dimension table for analytics
-- Target: blockchain_analytics.dim_time
-- 
-- This table provides a comprehensive time dimension for join-based analytics,
-- including date hierarchies, fiscal periods, and time-based attributes.
-- ============================================================================

-- Create analytics dataset if not exists
CREATE SCHEMA IF NOT EXISTS `${project_id}.blockchain_analytics`
OPTIONS(
    description = "Analytics layer with fact and dimension tables",
    location = "US"
);

-- Create time dimension table
CREATE TABLE IF NOT EXISTS `${project_id}.blockchain_analytics.dim_time`
(
    -- Primary key
    time_key INT64 NOT NULL,                      -- Surrogate key (YYYYMMDD)
    
    -- Date components
    full_date DATE NOT NULL,                      -- Full date
    year INT64 NOT NULL,                          -- Year (e.g., 2024)
    quarter INT64 NOT NULL,                       -- Quarter (1-4)
    month INT64 NOT NULL,                         -- Month (1-12)
    month_name STRING NOT NULL,                   -- Month name (e.g., "January")
    month_short STRING NOT NULL,                  -- Month short name (e.g., "Jan")
    week_of_year INT64 NOT NULL,                  -- Week of year (1-53)
    week_of_month INT64 NOT NULL,                 -- Week of month (1-5)
    day_of_year INT64 NOT NULL,                   -- Day of year (1-366)
    day_of_month INT64 NOT NULL,                  -- Day of month (1-31)
    day_of_week INT64 NOT NULL,                   -- Day of week (1-7, Sunday=1)
    day_name STRING NOT NULL,                     -- Day name (e.g., "Monday")
    day_short STRING NOT NULL,                    -- Day short name (e.g., "Mon")
    
    -- Calendar attributes
    is_weekend BOOL NOT NULL,                     -- Is Saturday or Sunday
    is_weekday BOOL NOT NULL,                     -- Is Monday-Friday
    is_month_start BOOL NOT NULL,                 -- First day of month
    is_month_end BOOL NOT NULL,                   -- Last day of month
    is_quarter_start BOOL NOT NULL,               -- First day of quarter
    is_quarter_end BOOL NOT NULL,                 -- Last day of quarter
    is_year_start BOOL NOT NULL,                  -- First day of year
    is_year_end BOOL NOT NULL,                    -- Last day of year
    
    -- Period labels
    year_month STRING NOT NULL,                   -- YYYY-MM format
    year_quarter STRING NOT NULL,                 -- YYYY-Q# format
    year_week STRING NOT NULL,                    -- YYYY-W## format
    
    -- Relative periods (updated daily in production)
    is_current_day BOOL,                          -- Is today
    is_current_week BOOL,                         -- Is current week
    is_current_month BOOL,                        -- Is current month
    is_current_quarter BOOL,                      -- Is current quarter
    is_current_year BOOL,                         -- Is current year
    
    -- Days ago calculations (updated daily in production)
    days_ago INT64,                               -- Days from today
    weeks_ago INT64,                              -- Weeks from today
    months_ago INT64,                             -- Months from today
    
    -- Record metadata
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
)
OPTIONS(
    description = "Time dimension for analytics"
);

-- ============================================================================
-- POPULATE TIME DIMENSION
-- ============================================================================
-- Generate dates from 2015 (Ethereum launch) to 2030

MERGE INTO `${project_id}.blockchain_analytics.dim_time` AS target
USING (
    WITH date_spine AS (
        -- Generate all dates from 2015-07-30 (Ethereum mainnet launch) to 2030-12-31
        SELECT date_value
        FROM UNNEST(
            GENERATE_DATE_ARRAY('2015-07-30', '2030-12-31', INTERVAL 1 DAY)
        ) AS date_value
    ),
    date_attributes AS (
        SELECT
            -- Primary key (YYYYMMDD format)
            CAST(FORMAT_DATE('%Y%m%d', date_value) AS INT64) AS time_key,
            
            -- Date components
            date_value AS full_date,
            EXTRACT(YEAR FROM date_value) AS year,
            EXTRACT(QUARTER FROM date_value) AS quarter,
            EXTRACT(MONTH FROM date_value) AS month,
            FORMAT_DATE('%B', date_value) AS month_name,
            FORMAT_DATE('%b', date_value) AS month_short,
            EXTRACT(WEEK FROM date_value) AS week_of_year,
            CAST(CEIL(EXTRACT(DAY FROM date_value) / 7.0) AS INT64) AS week_of_month,
            EXTRACT(DAYOFYEAR FROM date_value) AS day_of_year,
            EXTRACT(DAY FROM date_value) AS day_of_month,
            EXTRACT(DAYOFWEEK FROM date_value) AS day_of_week,
            FORMAT_DATE('%A', date_value) AS day_name,
            FORMAT_DATE('%a', date_value) AS day_short,
            
            -- Calendar attributes
            EXTRACT(DAYOFWEEK FROM date_value) IN (1, 7) AS is_weekend,
            EXTRACT(DAYOFWEEK FROM date_value) NOT IN (1, 7) AS is_weekday,
            EXTRACT(DAY FROM date_value) = 1 AS is_month_start,
            date_value = LAST_DAY(date_value) AS is_month_end,
            EXTRACT(MONTH FROM date_value) IN (1, 4, 7, 10) 
                AND EXTRACT(DAY FROM date_value) = 1 AS is_quarter_start,
            date_value = LAST_DAY(DATE_TRUNC(date_value, QUARTER)) AS is_quarter_end,
            EXTRACT(MONTH FROM date_value) = 1 
                AND EXTRACT(DAY FROM date_value) = 1 AS is_year_start,
            EXTRACT(MONTH FROM date_value) = 12 
                AND EXTRACT(DAY FROM date_value) = 31 AS is_year_end,
            
            -- Period labels
            FORMAT_DATE('%Y-%m', date_value) AS year_month,
            CONCAT(CAST(EXTRACT(YEAR FROM date_value) AS STRING), '-Q', 
                   CAST(EXTRACT(QUARTER FROM date_value) AS STRING)) AS year_quarter,
            CONCAT(CAST(EXTRACT(YEAR FROM date_value) AS STRING), '-W', 
                   LPAD(CAST(EXTRACT(WEEK FROM date_value) AS STRING), 2, '0')) AS year_week,
            
            -- Relative periods (based on current date)
            date_value = CURRENT_DATE() AS is_current_day,
            DATE_TRUNC(date_value, WEEK) = DATE_TRUNC(CURRENT_DATE(), WEEK) AS is_current_week,
            DATE_TRUNC(date_value, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) AS is_current_month,
            DATE_TRUNC(date_value, QUARTER) = DATE_TRUNC(CURRENT_DATE(), QUARTER) AS is_current_quarter,
            EXTRACT(YEAR FROM date_value) = EXTRACT(YEAR FROM CURRENT_DATE()) AS is_current_year,
            
            -- Days ago calculations
            DATE_DIFF(CURRENT_DATE(), date_value, DAY) AS days_ago,
            DATE_DIFF(CURRENT_DATE(), date_value, WEEK) AS weeks_ago,
            DATE_DIFF(CURRENT_DATE(), date_value, MONTH) AS months_ago,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
            
        FROM date_spine
    )
    SELECT * FROM date_attributes
) AS source
ON target.time_key = source.time_key
WHEN MATCHED THEN
    UPDATE SET
        is_current_day = source.is_current_day,
        is_current_week = source.is_current_week,
        is_current_month = source.is_current_month,
        is_current_quarter = source.is_current_quarter,
        is_current_year = source.is_current_year,
        days_ago = source.days_ago,
        weeks_ago = source.weeks_ago,
        months_ago = source.months_ago,
        updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT ROW;

