{{
    config(
        materialized='table',
        schema='analytics',
        cluster_by=['year', 'month']
    )
}}

/*
    Time dimension table
    
    This model generates a complete time dimension from Ethereum launch date
    to a future date, providing date hierarchies and time-based attributes.
*/

with date_spine as (
    -- Generate dates from Ethereum mainnet launch to 2030
    select date_value
    from unnest(
        generate_date_array('2015-07-30', '2030-12-31', interval 1 day)
    ) as date_value
),

date_attributes as (
    select
        -- Primary key (YYYYMMDD format)
        cast(format_date('%Y%m%d', date_value) as int64) as time_key,
        
        -- Date components
        date_value as full_date,
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        format_date('%B', date_value) as month_name,
        format_date('%b', date_value) as month_short,
        extract(week from date_value) as week_of_year,
        cast(ceil(extract(day from date_value) / 7.0) as int64) as week_of_month,
        extract(dayofyear from date_value) as day_of_year,
        extract(day from date_value) as day_of_month,
        extract(dayofweek from date_value) as day_of_week,
        format_date('%A', date_value) as day_name,
        format_date('%a', date_value) as day_short,
        
        -- Calendar attributes
        extract(dayofweek from date_value) in (1, 7) as is_weekend,
        extract(dayofweek from date_value) not in (1, 7) as is_weekday,
        extract(day from date_value) = 1 as is_month_start,
        date_value = last_day(date_value) as is_month_end,
        
        -- Period labels
        format_date('%Y-%m', date_value) as year_month,
        concat(cast(extract(year from date_value) as string), '-Q', 
               cast(extract(quarter from date_value) as string)) as year_quarter,
        concat(cast(extract(year from date_value) as string), '-W', 
               lpad(cast(extract(week from date_value) as string), 2, '0')) as year_week,
        
        -- Relative periods (based on current date)
        date_value = current_date() as is_current_day,
        date_trunc(date_value, week) = date_trunc(current_date(), week) as is_current_week,
        date_trunc(date_value, month) = date_trunc(current_date(), month) as is_current_month,
        extract(year from date_value) = extract(year from current_date()) as is_current_year,
        
        -- Days ago calculations
        date_diff(current_date(), date_value, day) as days_ago,
        date_diff(current_date(), date_value, week) as weeks_ago,
        date_diff(current_date(), date_value, month) as months_ago,
        
        -- Metadata
        current_timestamp() as created_at
        
    from date_spine
)

select * from date_attributes

