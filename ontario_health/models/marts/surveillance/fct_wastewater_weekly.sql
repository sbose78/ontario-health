-- DOCUMENTATION MODEL ONLY
-- The actual table is a Snowflake Dynamic Table created in sql/04_marts_schemas.sql
-- This file exists for dbt documentation and testing purposes

{{
    config(
        materialized='view',
        schema='marts_surveillance',
        tags=['surveillance', 'wastewater']
    )
}}

-- This Dynamic Table auto-refreshes with TARGET_LAG = '1 hour'
-- Managed by Snowflake, not dbt

SELECT 
    epi_year,
    epi_week,
    week_start,
    virus_code,
    virus_name,
    location,
    site,
    city,
    viral_load_avg,
    viral_load_min,
    viral_load_max,
    population_coverage

FROM {{ source('raw', 'wastewater_surveillance') }}
WHERE province = 'Ontario'

