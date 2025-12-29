-- Ontario Health Data Pipeline - Table Definitions
-- Tables for storing ingested data from Ontario Data Catalogue

USE DATABASE ONTARIO_HEALTH;
USE SCHEMA RAW;

-- ============================================================================
-- RAW TABLES - Direct landing zone from API ingestion
-- ============================================================================

-- School and childcare center infection cases
CREATE TABLE IF NOT EXISTS RAW.SCHOOL_CASES (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    
    -- Raw fields from API (nullable to handle schema changes)
    reported_date DATE,
    collected_date DATE,
    school_board VARCHAR(500),
    school_name VARCHAR(500),
    school_id VARCHAR(100),
    municipality VARCHAR(200),
    school_type VARCHAR(100),  -- Elementary, Secondary, etc.
    confirmed_cases NUMBER,
    cumulative_cases NUMBER,
    
    -- Metadata
    raw_json VARIANT  -- Store full record for debugging/reprocessing
);

-- Outbreak data (schools, daycares, institutions)
CREATE TABLE IF NOT EXISTS RAW.OUTBREAKS (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    
    -- Raw fields from API
    outbreak_id VARCHAR(100),
    date_outbreak_began DATE,
    date_outbreak_declared_over DATE,
    outbreak_status VARCHAR(50),  -- Active, Resolved
    institution_name VARCHAR(500),
    institution_address VARCHAR(500),
    institution_city VARCHAR(200),
    institution_type VARCHAR(200),  -- School, Daycare, Long-term care, etc.
    outbreak_type VARCHAR(200),  -- Respiratory, Enteric, etc.
    
    -- Case counts
    resident_cases NUMBER,
    staff_cases NUMBER,
    total_cases NUMBER,
    
    -- PHU info
    phu_id VARCHAR(20),
    phu_name VARCHAR(200),
    
    -- Metadata
    raw_json VARIANT
);

-- Age breakdown of confirmed cases
CREATE TABLE IF NOT EXISTS RAW.CASES_BY_AGE (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    
    -- Raw fields from API
    reported_date DATE,
    age_group VARCHAR(50),  -- 0-4, 5-11, 12-19, 20-29, etc.
    gender VARCHAR(20),
    
    -- Case data
    case_count NUMBER,
    cumulative_cases NUMBER,
    
    -- PHU info
    phu_id VARCHAR(20),
    phu_name VARCHAR(200),
    
    -- Metadata
    raw_json VARIANT
);

-- Ingestion run tracking
CREATE TABLE IF NOT EXISTS RAW.INGESTION_LOG (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    run_id VARCHAR(50),
    started_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ,
    dataset_name VARCHAR(100),
    records_fetched NUMBER,
    records_inserted NUMBER,
    status VARCHAR(20),  -- SUCCESS, FAILED, PARTIAL
    error_message VARCHAR(4000),
    api_url VARCHAR(2000)
);


-- ============================================================================
-- STAGING TABLES - Cleaned and normalized data
-- ============================================================================

USE SCHEMA STAGING;

-- Cleaned school cases with standardized fields
CREATE TABLE IF NOT EXISTS STAGING.SCHOOL_CASES_CLEAN (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    raw_id NUMBER,  -- Reference to RAW table
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    reported_date DATE NOT NULL,
    school_board VARCHAR(500),
    school_name VARCHAR(500) NOT NULL,
    school_id VARCHAR(100),
    municipality VARCHAR(200),
    school_type VARCHAR(100),
    confirmed_cases NUMBER DEFAULT 0,
    cumulative_cases NUMBER DEFAULT 0,
    
    -- Computed fields
    is_elementary BOOLEAN,
    is_secondary BOOLEAN,
    is_childcare BOOLEAN
);

-- Cleaned outbreaks filtered to schools/daycares
CREATE TABLE IF NOT EXISTS STAGING.SCHOOL_OUTBREAKS_CLEAN (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    raw_id NUMBER,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    outbreak_id VARCHAR(100) NOT NULL,
    date_outbreak_began DATE NOT NULL,
    date_outbreak_declared_over DATE,
    outbreak_status VARCHAR(50),
    duration_days NUMBER,  -- Computed: days active
    
    institution_name VARCHAR(500),
    institution_city VARCHAR(200),
    institution_type VARCHAR(200),
    
    total_cases NUMBER DEFAULT 0,
    
    phu_id VARCHAR(20),
    phu_name VARCHAR(200),
    is_halton BOOLEAN
);

-- Pediatric case aggregates by age group and date
CREATE TABLE IF NOT EXISTS STAGING.PEDIATRIC_CASES (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    reported_date DATE NOT NULL,
    age_group VARCHAR(50) NOT NULL,
    
    daily_cases NUMBER DEFAULT 0,
    cumulative_cases NUMBER DEFAULT 0,
    
    phu_id VARCHAR(20),
    phu_name VARCHAR(200),
    is_halton BOOLEAN,
    
    -- 7-day rolling metrics (computed)
    cases_7d_avg FLOAT,
    cases_7d_total NUMBER
);

-- Create indexes for common queries
CREATE OR REPLACE INDEX idx_school_cases_date ON STAGING.SCHOOL_CASES_CLEAN(reported_date);
CREATE OR REPLACE INDEX idx_outbreaks_date ON STAGING.SCHOOL_OUTBREAKS_CLEAN(date_outbreak_began);
CREATE OR REPLACE INDEX idx_pediatric_date_age ON STAGING.PEDIATRIC_CASES(reported_date, age_group);

