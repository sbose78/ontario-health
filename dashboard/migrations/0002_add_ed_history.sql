-- Add table for ED wait times history (last 24 hours)

CREATE TABLE IF NOT EXISTS ed_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hospital_name TEXT NOT NULL,
    wait_hours INTEGER,
    wait_minutes INTEGER,
    wait_total_minutes INTEGER,
    scraped_at TEXT NOT NULL,
    wait_severity TEXT,
    synced_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Index for queries
CREATE INDEX IF NOT EXISTS idx_ed_history_time ON ed_history(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_ed_history_hospital ON ed_history(hospital_name);

