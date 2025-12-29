-- Ontario Health Data Pipeline - MARTS Schema Organization
-- Organizes analytics-ready views into domain-specific schemas

USE DATABASE ONTARIO_HEALTH;

-- ============================================================================
-- SCHEMA CREATION
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS MARTS_SURVEILLANCE 
    COMMENT = 'Current respiratory surveillance data (wastewater, etc.)';

CREATE SCHEMA IF NOT EXISTS MARTS_HISTORICAL 
    COMMENT = 'Archived reference data (2021 school cases, outbreaks)';

CREATE SCHEMA IF NOT EXISTS MARTS_OPS 
    COMMENT = 'Pipeline operations and data quality';


-- ============================================================================
-- MARTS_SURVEILLANCE - Current Respiratory Data
-- ============================================================================

-- Fact table: Weekly wastewater viral loads by location
CREATE OR REPLACE VIEW MARTS_SURVEILLANCE.fct_wastewater_weekly AS
SELECT 
    epi_year,
    epi_week,
    week_start,
    virus_code,
    virus_name,
    location,
    site,
    city,
    province,
    viral_load_avg,
    viral_load_min,
    viral_load_max,
    population_coverage,
    ingested_at
FROM RAW.WASTEWATER_SURVEILLANCE
WHERE province = 'Ontario';


-- Report: Current week summary (latest available)
CREATE OR REPLACE VIEW MARTS_SURVEILLANCE.rpt_current_week AS
WITH latest_week AS (
    SELECT MAX(epi_year * 100 + epi_week) as year_week
    FROM RAW.WASTEWATER_SURVEILLANCE 
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
FROM RAW.WASTEWATER_SURVEILLANCE w
CROSS JOIN latest_week l
WHERE w.province = 'Ontario'
    AND (w.epi_year * 100 + w.epi_week) = l.year_week
GROUP BY w.epi_year, w.epi_week, w.virus_name
ORDER BY w.virus_name;


-- ============================================================================
-- ED WAIT TIMES (Halton Healthcare - scraped)
-- ============================================================================

-- ED wait times history
CREATE OR REPLACE VIEW MARTS_SURVEILLANCE.rpt_ed_wait_times AS
SELECT 
    scraped_at,
    source_updated,
    hospital_name,
    region,
    wait_hours,
    wait_minutes,
    wait_total_minutes,
    CASE 
        WHEN wait_total_minutes <= 60 THEN 'Low'
        WHEN wait_total_minutes <= 120 THEN 'Moderate'
        WHEN wait_total_minutes <= 240 THEN 'High'
        ELSE 'Critical'
    END as wait_severity
FROM RAW.ED_WAIT_TIMES
ORDER BY scraped_at DESC, hospital_name;


-- Current ED status (latest readings)
CREATE OR REPLACE VIEW MARTS_SURVEILLANCE.rpt_ed_current AS
WITH latest AS (
    SELECT MAX(scraped_at) as max_scraped
    FROM RAW.ED_WAIT_TIMES
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
FROM RAW.ED_WAIT_TIMES e
JOIN latest l ON e.scraped_at = l.max_scraped
ORDER BY e.wait_total_minutes DESC;


-- Report: Week-over-week viral trends
CREATE OR REPLACE VIEW MARTS_SURVEILLANCE.rpt_viral_trends AS
SELECT 
    epi_year,
    epi_week,
    virus_name,
    ROUND(AVG(viral_load_avg), 2) as avg_viral_load,
    LAG(ROUND(AVG(viral_load_avg), 2)) OVER (
        PARTITION BY virus_name ORDER BY epi_year, epi_week
    ) as prev_week_avg,
    ROUND(
        (AVG(viral_load_avg) - LAG(AVG(viral_load_avg)) OVER (
            PARTITION BY virus_name ORDER BY epi_year, epi_week
        )) / NULLIF(LAG(AVG(viral_load_avg)) OVER (
            PARTITION BY virus_name ORDER BY epi_year, epi_week
        ), 0) * 100, 1
    ) as week_over_week_pct
FROM RAW.WASTEWATER_SURVEILLANCE
WHERE province = 'Ontario' AND epi_year >= 2024
GROUP BY epi_year, epi_week, virus_name
ORDER BY epi_year DESC, epi_week DESC, virus_name;


-- ============================================================================
-- MARTS_HISTORICAL - Archived School/Outbreak Data (2021)
-- ============================================================================

-- Fact table: School infection cases
CREATE OR REPLACE VIEW MARTS_HISTORICAL.fct_school_cases AS
SELECT 
    reported_date,
    school_board,
    school_name,
    school_id,
    municipality,
    confirmed_cases,
    cumulative_cases,
    ingested_at
FROM RAW.SCHOOL_CASES;


-- Report: Weekly school case summary
CREATE OR REPLACE VIEW MARTS_HISTORICAL.rpt_school_summary AS
SELECT 
    DATE_TRUNC('week', reported_date) as week_start,
    COUNT(DISTINCT school_name) as schools_affected,
    COUNT(DISTINCT school_board) as boards_affected,
    COUNT(DISTINCT municipality) as municipalities_affected,
    SUM(confirmed_cases) as total_cases
FROM RAW.SCHOOL_CASES
WHERE reported_date IS NOT NULL
GROUP BY DATE_TRUNC('week', reported_date)
ORDER BY week_start DESC;


-- Report: Outbreak summary by institution type
CREATE OR REPLACE VIEW MARTS_HISTORICAL.rpt_outbreak_summary AS
SELECT 
    institution_type,
    outbreak_status,
    COUNT(*) as outbreak_count,
    SUM(total_cases) as total_cases
FROM RAW.OUTBREAKS
GROUP BY institution_type, outbreak_status
ORDER BY outbreak_count DESC;


-- ============================================================================
-- MARTS_OPS - Pipeline Operations
-- ============================================================================

-- Report: Data freshness across all datasets
CREATE OR REPLACE VIEW MARTS_OPS.rpt_data_freshness AS
SELECT 
    'SCHOOL_CASES' as dataset,
    'historical' as category,
    MAX(reported_date) as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.SCHOOL_CASES

UNION ALL

SELECT 
    'OUTBREAKS' as dataset,
    'historical' as category,
    MAX(date_outbreak_began) as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.OUTBREAKS

UNION ALL

SELECT 
    'WASTEWATER_SURVEILLANCE' as dataset,
    'surveillance' as category,
    MAX(week_start)::DATE as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.WASTEWATER_SURVEILLANCE

UNION ALL

SELECT 
    'ED_WAIT_TIMES' as dataset,
    'surveillance' as category,
    MAX(scraped_at)::DATE as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.ED_WAIT_TIMES;


-- Report: Ingestion run history
CREATE OR REPLACE VIEW MARTS_OPS.rpt_ingestion_log AS
SELECT 
    run_id,
    dataset_name,
    started_at,
    completed_at,
    records_fetched,
    records_inserted,
    status,
    error_message
FROM RAW.INGESTION_LOG
ORDER BY started_at DESC;

