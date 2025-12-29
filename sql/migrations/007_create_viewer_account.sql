-- ============================================================================
-- Migration 007: Create Read-Only Viewer Service Account
-- Run as: ACCOUNTADMIN
-- Purpose: Read-only access for public dashboard (Cloudflare Pages)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create viewer service account
CREATE USER IF NOT EXISTS ontario_health_viewer
  COMMENT = 'Read-only service account for public dashboard'
  DEFAULT_ROLE = ontario_health_viewer_role
  DEFAULT_WAREHOUSE = COMPUTE_WH
  MUST_CHANGE_PASSWORD = FALSE;

-- Create viewer role (read-only)
CREATE ROLE IF NOT EXISTS ontario_health_viewer_role
  COMMENT = 'Read-only access to MARTS schemas for public dashboard';

-- Grant warehouse usage (for queries only)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ontario_health_viewer_role;

-- Grant database usage
GRANT USAGE ON DATABASE ONTARIO_HEALTH TO ROLE ontario_health_viewer_role;

-- Set context for grants
USE DATABASE ONTARIO_HEALTH;

-- Grant SELECT only on MARTS schemas (no write access)
GRANT USAGE ON SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;
GRANT USAGE ON SCHEMA MARTS_HISTORICAL TO ROLE ontario_health_viewer_role;
GRANT USAGE ON SCHEMA MARTS_OPS TO ROLE ontario_health_viewer_role;

GRANT SELECT ON ALL TABLES IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;

GRANT SELECT ON ALL TABLES IN SCHEMA MARTS_HISTORICAL TO ROLE ontario_health_viewer_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA MARTS_HISTORICAL TO ROLE ontario_health_viewer_role;

GRANT SELECT ON ALL TABLES IN SCHEMA MARTS_OPS TO ROLE ontario_health_viewer_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA MARTS_OPS TO ROLE ontario_health_viewer_role;

-- Grant SELECT on future objects
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA MARTS_SURVEILLANCE TO ROLE ontario_health_viewer_role;

GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS_HISTORICAL TO ROLE ontario_health_viewer_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARTS_HISTORICAL TO ROLE ontario_health_viewer_role;

GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS_OPS TO ROLE ontario_health_viewer_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARTS_OPS TO ROLE ontario_health_viewer_role;

-- Explicitly deny access to RAW and STAGING
-- (default deny, but being explicit for security)

-- Grant role to viewer account
GRANT ROLE ontario_health_viewer_role TO USER ontario_health_viewer;

-- Add public key for key-pair authentication
ALTER USER ontario_health_viewer SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArW4KYqkbFREwaOfKyRhfNsQ8IXTc4Scv+qZdxhiNNPY2afufTO0TgCHTvL+/8fZVc+Pf4AmWOCx7A8crg8G1Rg4U+axpUiGsMZ35zGodl0jL212iUGzqqgN/554WAp7BEGANFAHOZNo0qUbuf52Nb6hBn3RTJci6zqBCbQLxBXeUikE10aH5aeySKAt4/VDyW7MlR5W/22wMvZV0eeuHdtJiHOkVNoNdOpX9Ca401biKLvnD6QGgGSs8Y76bghkGlrRiIUl8N4QmX4z1BV2v04uw9L3OflXOUr5JD4NEiFDtFdgi7HI/5Ptm4yG6IUjeR8Lsr+mTThQbq2GieWlxGQIDAQAB';

-- Verify setup
SHOW USERS LIKE 'ontario_health_viewer';
SHOW GRANTS TO ROLE ontario_health_viewer_role;

-- Test query as viewer
USE ROLE ontario_health_viewer_role;
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();

-- Test can read MARTS
SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week LIMIT 1;

-- Verify cannot access RAW (should fail)
-- SELECT * FROM RAW.WASTEWATER_SURVEILLANCE LIMIT 1;
