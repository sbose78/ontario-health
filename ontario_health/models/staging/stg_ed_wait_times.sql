{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging view for ED wait times
-- Documentation model for scraped ED data

SELECT 
    id,
    ingested_at,
    scraped_at,
    source_updated,
    
    -- Hospital info
    hospital_code,
    hospital_name,
    region,
    
    -- Wait times
    wait_hours,
    wait_minutes,
    wait_total_minutes,
    
    -- Derived severity
    CASE 
        WHEN wait_total_minutes <= 60 THEN 'Low'
        WHEN wait_total_minutes <= 120 THEN 'Moderate'
        WHEN wait_total_minutes <= 240 THEN 'High'
        ELSE 'Critical'
    END as wait_severity

FROM {{ source('raw', 'ed_wait_times') }}

