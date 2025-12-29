{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging view for wastewater surveillance data
-- This is a documentation/testing model - actual data lives in RAW table
-- and Dynamic Tables handle materialization

SELECT 
    id,
    ingested_at,
    
    -- Time dimensions
    epi_year,
    epi_week,
    week_start,
    
    -- Location dimensions
    location,
    site,
    city,
    province,
    
    -- Virus dimensions
    virus_code,
    virus_name,
    
    -- Measurements
    viral_load_avg,
    viral_load_min,
    viral_load_max,
    population_coverage

FROM {{ source('raw', 'wastewater_surveillance') }}
WHERE province = 'Ontario'
    AND epi_year >= 2024  -- Focus on recent data

