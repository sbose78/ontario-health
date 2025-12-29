{{
    config(
        materialized='view',
        schema='marts_surveillance',
        tags=['surveillance', 'report']
    )
}}

-- Current week respiratory surveillance summary
-- Reads from Dynamic Table fct_wastewater_weekly

WITH latest_week AS (
    SELECT MAX(epi_year * 100 + epi_week) as year_week
    FROM {{ source('raw', 'wastewater_surveillance') }}
    WHERE province = 'Ontario' AND epi_year = 2025
)

SELECT 
    w.epi_year,
    w.epi_week,
    w.virus_name,
    COUNT(DISTINCT w.location) as sites_reporting,
    ROUND(AVG(w.viral_load_avg), 2) as avg_viral_load,
    ROUND(MAX(w.viral_load_avg), 2) as max_viral_load,
    ROUND(MIN(w.viral_load_avg), 2) as min_viral_load

FROM {{ source('raw', 'wastewater_surveillance') }} w
CROSS JOIN latest_week l

WHERE w.province = 'Ontario'
    AND (w.epi_year * 100 + w.epi_week) = l.year_week

GROUP BY w.epi_year, w.epi_week, w.virus_name
ORDER BY w.virus_name

