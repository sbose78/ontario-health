{{
    config(
        materialized='view',
        schema='marts_surveillance',
        tags=['surveillance', 'ed', 'report']
    )
}}

-- Current ED wait times (latest scrape)

WITH latest AS (
    SELECT MAX(scraped_at) as max_scraped
    FROM {{ source('raw', 'ed_wait_times') }}
)

SELECT 
    e.hospital_name,
    e.wait_hours,
    e.wait_minutes,
    e.wait_total_minutes,
    e.source_updated,
    e.scraped_at,
    CASE 
        WHEN e.wait_total_minutes <= 60 THEN 'Low'
        WHEN e.wait_total_minutes <= 120 THEN 'Moderate'
        WHEN e.wait_total_minutes <= 240 THEN 'High'
        ELSE 'Critical'
    END as wait_severity

FROM {{ source('raw', 'ed_wait_times') }} e
JOIN latest l ON e.scraped_at = l.max_scraped

ORDER BY e.wait_total_minutes DESC

