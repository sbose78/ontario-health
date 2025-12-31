-- ============================================================================
-- Migration 008: Extend ED_WAIT_TIMES for Multi-Network Support
-- Run as: ontario_health_svc or ACCOUNTADMIN
-- Purpose: Add columns for network, city, region to support province-wide ED tracking
-- ============================================================================

USE DATABASE ONTARIO_HEALTH;
USE SCHEMA RAW;

-- Add new columns for multi-network support
ALTER TABLE RAW.ED_WAIT_TIMES 
ADD COLUMN IF NOT EXISTS network VARCHAR(200);

ALTER TABLE RAW.ED_WAIT_TIMES 
ADD COLUMN IF NOT EXISTS city VARCHAR(200);

ALTER TABLE RAW.ED_WAIT_TIMES 
ADD COLUMN IF NOT EXISTS region_name VARCHAR(200);

-- Backfill existing Halton data
UPDATE RAW.ED_WAIT_TIMES 
SET network = 'Halton Healthcare',
    region_name = 'Halton'
WHERE network IS NULL;

-- Update cities for existing Halton hospitals
UPDATE RAW.ED_WAIT_TIMES 
SET city = 'Georgetown'
WHERE hospital_name LIKE '%Georgetown%' AND city IS NULL;

UPDATE RAW.ED_WAIT_TIMES 
SET city = 'Milton'
WHERE hospital_name LIKE '%Milton%' AND city IS NULL;

UPDATE RAW.ED_WAIT_TIMES 
SET city = 'Oakville'
WHERE hospital_name LIKE '%Oakville%' AND city IS NULL;

-- Verify
SELECT 
    network,
    city,
    COUNT(*) as readings
FROM RAW.ED_WAIT_TIMES
GROUP BY network, city
ORDER BY network, city;

