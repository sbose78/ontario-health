-- ============================================================================
-- Migration 006: Deduplicate RAW Tables
-- Run as: ACCOUNTADMIN or ontario_health_role
-- Purpose: Remove duplicates from earlier test runs
-- Strategy: DELETE duplicates, keep most recent ingestion
-- ============================================================================

USE DATABASE ONTARIO_HEALTH;
USE SCHEMA RAW;

-- 1. Deduplicate WASTEWATER_SURVEILLANCE
-- Identify duplicates and delete older ones
DELETE FROM RAW.WASTEWATER_SURVEILLANCE
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY epi_year, epi_week, virus_code, location 
                   ORDER BY ingested_at DESC
               ) as rn
        FROM RAW.WASTEWATER_SURVEILLANCE
    )
    WHERE rn > 1
);

-- Verify
SELECT 'WASTEWATER_SURVEILLANCE' as table_name, COUNT(*) as rows_after_dedup 
FROM RAW.WASTEWATER_SURVEILLANCE;


-- 2. Deduplicate SCHOOL_CASES
DELETE FROM RAW.SCHOOL_CASES
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY school_name, reported_date 
                   ORDER BY ingested_at DESC
               ) as rn
        FROM RAW.SCHOOL_CASES
    )
    WHERE rn > 1
);

-- Verify
SELECT 'SCHOOL_CASES' as table_name, COUNT(*) as rows_after_dedup 
FROM RAW.SCHOOL_CASES;


-- 3. Deduplicate OUTBREAKS
DELETE FROM RAW.OUTBREAKS
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY outbreak_id 
                   ORDER BY ingested_at DESC
               ) as rn
        FROM RAW.OUTBREAKS
    )
    WHERE rn > 1
);

-- Verify
SELECT 'OUTBREAKS' as table_name, COUNT(*) as rows_after_dedup 
FROM RAW.OUTBREAKS;


-- Force refresh of Dynamic Tables to reflect clean data
ALTER DYNAMIC TABLE MARTS_SURVEILLANCE.fct_wastewater_weekly REFRESH;
ALTER DYNAMIC TABLE MARTS_SURVEILLANCE.rpt_viral_trends REFRESH;
ALTER DYNAMIC TABLE MARTS_SURVEILLANCE.rpt_ed_wait_times REFRESH;

-- Final verification
SELECT 'RAW.WASTEWATER_SURVEILLANCE' as table_name, COUNT(*) as row_count 
FROM RAW.WASTEWATER_SURVEILLANCE
UNION ALL
SELECT 'MARTS_SURVEILLANCE.fct_wastewater_weekly', COUNT(*) 
FROM MARTS_SURVEILLANCE.fct_wastewater_weekly;
