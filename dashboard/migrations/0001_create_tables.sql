-- D1 Cache Schema for Ontario Health Dashboard
-- Synced from Snowflake MARTS every 30 minutes

-- Current week respiratory surveillance
CREATE TABLE IF NOT EXISTS current_week (
    virus_name TEXT PRIMARY KEY,
    epi_year INTEGER NOT NULL,
    epi_week INTEGER NOT NULL,
    sites_reporting INTEGER,
    avg_viral_load REAL,
    max_viral_load REAL,
    min_viral_load REAL,
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ED wait times (current)
CREATE TABLE IF NOT EXISTS ed_current (
    hospital_name TEXT PRIMARY KEY,
    wait_hours INTEGER,
    wait_minutes INTEGER,
    wait_total_minutes INTEGER,
    source_updated TEXT,
    scraped_at TEXT,
    wait_severity TEXT,
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Viral trends (last 4 weeks)
CREATE TABLE IF NOT EXISTS viral_trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    epi_year INTEGER,
    epi_week INTEGER,
    virus_name TEXT,
    avg_viral_load REAL,
    prev_week_avg REAL,
    week_over_week_pct REAL,
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Data freshness
CREATE TABLE IF NOT EXISTS data_freshness (
    dataset TEXT PRIMARY KEY,
    category TEXT,
    latest_data_date TEXT,
    total_records INTEGER,
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_trends_week ON viral_trends(epi_year, epi_week);
CREATE INDEX IF NOT EXISTS idx_trends_virus ON viral_trends(virus_name);

