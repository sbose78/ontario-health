-- Ontario Health Data Pipeline - Database Setup
-- Run this script with ACCOUNTADMIN role to create the database infrastructure

-- Create warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create database
CREATE DATABASE IF NOT EXISTS ONTARIO_HEALTH
    COMMENT = 'Ontario public health data - respiratory infections, school outbreaks, hospital capacity';

-- Use the database
USE DATABASE ONTARIO_HEALTH;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone for raw API data from Ontario Data Catalogue';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and typed data ready for analysis';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Analytics-ready datasets (dbt marts)';

-- Grant usage (adjust role as needed)
GRANT USAGE ON DATABASE ONTARIO_HEALTH TO ROLE ACCOUNTADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ONTARIO_HEALTH TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ONTARIO_HEALTH TO ROLE ACCOUNTADMIN;

-- Verify setup
SELECT 
    CURRENT_DATABASE() as database,
    CURRENT_WAREHOUSE() as warehouse,
    CURRENT_ROLE() as role;

SHOW SCHEMAS IN DATABASE ONTARIO_HEALTH;

