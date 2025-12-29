-- ============================================================================
-- Migration 001: Create Service Account for Ontario Health Pipeline
-- Run as: ACCOUNTADMIN
-- Purpose: Set up service account with key-pair auth for dbt and automation
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create service account user
CREATE USER IF NOT EXISTS ontario_health_svc
  COMMENT = 'Service account for Ontario Health Data Pipeline (dbt, GitHub Actions)'
  DEFAULT_ROLE = ontario_health_role
  DEFAULT_WAREHOUSE = COMPUTE_WH
  MUST_CHANGE_PASSWORD = FALSE;

-- Create role for the service account
CREATE ROLE IF NOT EXISTS ontario_health_role
  COMMENT = 'Role for Ontario Health pipeline operations';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ontario_health_role;

-- Grant database access
GRANT USAGE ON DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;

-- Grant table/view/dynamic table access
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON ALL DYNAMIC TABLES IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON ALL STREAMS IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;

-- Grant future object access
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON FUTURE DYNAMIC TABLES IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;
GRANT ALL PRIVILEGES ON FUTURE STREAMS IN DATABASE ONTARIO_HEALTH TO ROLE ontario_health_role;

-- Grant role to service account
GRANT ROLE ontario_health_role TO USER ontario_health_svc;

-- Add public key to service account
-- Generated from: openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ontario_health_key.p8 -nocrypt
ALTER USER ontario_health_svc SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5Q48SyIE/HJq6A1h+vSG/jVZmRt/vZ/qyfeb7dje6wVz4EDnwewsoUsbzusPIPHjTL7U6ijWV7cOmMHNoH62B1LRl6Z1ghbZBt/DsXLMaQVzliRXwk1797qC9xyY53s74QyGXATl4Zt4GGYp6F/rl8ElKFAeet+J6zhYVQYlSahobpHdpH+C9k3gfvhPMS4NkT+qlUXyROaMQUv7OstqeSaqxdNpP5dQoJ92hIlAO152DJf/0gis8wM2+8C1tNcCv3IOmp0BQNN/T/iCHoIKq0LtZGqeNIahSrL9FdZAqEsv/AO+Fbh9pW+sHAOMozlDHPQXs4pi7MhCX4m9eWZ4HwIDAQAB';

-- Verify setup
SHOW USERS LIKE 'ontario_health_svc';
SHOW GRANTS TO USER ontario_health_svc;

-- Test query as service account
USE ROLE ontario_health_role;
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();
