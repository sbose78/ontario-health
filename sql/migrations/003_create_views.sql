-- Ontario Health Data Pipeline - Marts (Analytics-Ready Views)
-- dbt-style marts for dashboards and reporting
-- Note: These views read directly from RAW tables. Add dbt transformations for staging layer later.

USE DATABASE ONTARIO_HEALTH;
USE SCHEMA MARTS;

-- ============================================================================
-- REPORTING VIEWS (reading from RAW for now)
-- ============================================================================

-- Daily school infection trends with 7-day moving average
CREATE OR REPLACE VIEW MARTS.RPT_SCHOOL_INFECTION_TRENDS AS
SELECT 
    reported_date,
    municipality,
    COUNT(DISTINCT school_name) as schools_with_cases,
    SUM(confirmed_cases) as daily_cases,
    AVG(SUM(confirmed_cases)) OVER (
        PARTITION BY municipality 
        ORDER BY reported_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as cases_7d_avg,
    SUM(SUM(confirmed_cases)) OVER (
        PARTITION BY municipality 
        ORDER BY reported_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as cases_7d_total
FROM RAW.SCHOOL_CASES
WHERE reported_date IS NOT NULL
GROUP BY reported_date, municipality
ORDER BY reported_date DESC, municipality;


-- School cases by school board
CREATE OR REPLACE VIEW MARTS.RPT_SCHOOL_CASES_BY_BOARD AS
SELECT 
    school_board,
    COUNT(DISTINCT school_name) as total_schools,
    SUM(confirmed_cases) as total_cases,
    MAX(reported_date) as latest_report_date,
    MIN(reported_date) as earliest_report_date
FROM RAW.SCHOOL_CASES
WHERE school_board IS NOT NULL
GROUP BY school_board
ORDER BY total_cases DESC;


-- Weekly school case summary
CREATE OR REPLACE VIEW MARTS.RPT_WEEKLY_SCHOOL_SUMMARY AS
SELECT 
    DATE_TRUNC('week', reported_date) as week_start,
    COUNT(DISTINCT school_name) as schools_affected,
    COUNT(DISTINCT school_board) as boards_affected,
    COUNT(DISTINCT municipality) as municipalities_affected,
    SUM(confirmed_cases) as total_cases,
    AVG(confirmed_cases) as avg_cases_per_report
FROM RAW.SCHOOL_CASES
WHERE reported_date IS NOT NULL
GROUP BY DATE_TRUNC('week', reported_date)
ORDER BY week_start DESC;


-- Education outbreaks summary (from aggregate outbreak data)
CREATE OR REPLACE VIEW MARTS.RPT_EDUCATION_OUTBREAKS AS
SELECT 
    outbreak_id,
    institution_name,
    institution_city,
    institution_type,
    date_outbreak_began,
    date_outbreak_declared_over,
    outbreak_status,
    total_cases,
    phu_name
FROM RAW.OUTBREAKS
WHERE institution_type LIKE '%Education%'
    OR institution_type LIKE '%3 Education%'
ORDER BY date_outbreak_began DESC NULLS LAST;


-- Top municipalities by school cases
CREATE OR REPLACE VIEW MARTS.RPT_TOP_MUNICIPALITIES AS
SELECT 
    municipality,
    COUNT(DISTINCT school_name) as schools_affected,
    SUM(confirmed_cases) as total_cases,
    ROUND(SUM(confirmed_cases) / COUNT(DISTINCT school_name), 2) as avg_cases_per_school
FROM RAW.SCHOOL_CASES
WHERE municipality IS NOT NULL
GROUP BY municipality
HAVING SUM(confirmed_cases) > 0
ORDER BY total_cases DESC
LIMIT 50;


-- Data freshness check
CREATE OR REPLACE VIEW MARTS.RPT_DATA_FRESHNESS AS
SELECT 
    'SCHOOL_CASES' as table_name,
    MAX(reported_date) as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.SCHOOL_CASES

UNION ALL

SELECT 
    'OUTBREAKS' as table_name,
    MAX(date_outbreak_began) as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.OUTBREAKS

UNION ALL

SELECT 
    'WASTEWATER_SURVEILLANCE' as table_name,
    MAX(week_start)::DATE as latest_data_date,
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_records
FROM RAW.WASTEWATER_SURVEILLANCE;


-- ============================================================================
-- WASTEWATER SURVEILLANCE VIEWS (Fall 2025 Current Data)
-- ============================================================================

-- Ontario wastewater trends with week-over-week change
CREATE OR REPLACE VIEW MARTS.RPT_WASTEWATER_TRENDS AS
SELECT 
    epi_year,
    epi_week,
    week_start,
    virus_name,
    location,
    city,
    viral_load_avg,
    viral_load_min,
    viral_load_max,
    LAG(viral_load_avg) OVER (
        PARTITION BY location, virus_name 
        ORDER BY epi_year, epi_week
    ) as prev_week_load,
    ROUND(
        (viral_load_avg - LAG(viral_load_avg) OVER (
            PARTITION BY location, virus_name 
            ORDER BY epi_year, epi_week
        )) / NULLIF(LAG(viral_load_avg) OVER (
            PARTITION BY location, virus_name 
            ORDER BY epi_year, epi_week
        ), 0) * 100, 1
    ) as week_over_week_pct_change
FROM RAW.WASTEWATER_SURVEILLANCE
WHERE province = 'Ontario'
ORDER BY epi_year DESC, epi_week DESC, virus_name, location;


-- Fall 2025 respiratory summary by week and virus
CREATE OR REPLACE VIEW MARTS.RPT_FALL_2025_RESPIRATORY AS
SELECT 
    epi_week,
    virus_name,
    COUNT(DISTINCT location) as sites_reporting,
    ROUND(AVG(viral_load_avg), 2) as avg_viral_load,
    ROUND(MAX(viral_load_avg), 2) as max_viral_load,
    LISTAGG(DISTINCT location, ', ') WITHIN GROUP (ORDER BY location) as locations
FROM RAW.WASTEWATER_SURVEILLANCE
WHERE epi_year = 2025 
    AND epi_week >= 35
    AND province = 'Ontario'
GROUP BY epi_week, virus_name
ORDER BY epi_week DESC, virus_name;
