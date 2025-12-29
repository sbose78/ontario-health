# Ontario Health Data Pipeline - Technical Specification

**Version**: 1.0  
**Last Updated**: December 28, 2025

---

## System Architecture

```
Health Canada / Ontario Data Catalogue
           │
           ▼
    Python Ingestors (GitHub Actions)
           │
           ▼
    ┌─────────────────────────────────┐
    │         Snowflake               │
    │  RAW → Dynamic Tables → MARTS   │
    │  (auto-refresh on data arrival) │
    └─────────────────────────────────┘
```

**Key Design**: Snowflake Dynamic Tables handle materialization (not dbt). Auto-refresh when upstream data changes.

---

## Data Sources & Refresh Schedule

| Dataset | Source | Frequency | Latest | Volume |
|---------|--------|-----------|--------|--------|
| **Wastewater** | Health Canada CSV | Weekly (Wed 6am) | Week 51, 2025 | 6,248 rows |
| **ED Wait Times** | Halton Healthcare (scraped) | Every 3 hours | Dec 28, 2025 | 3 rows/run |
| School Cases | Ontario Data Catalogue | One-time (2021) | Dec 23, 2021 | 50,299 rows |
| Outbreaks | Ontario Data Catalogue | One-time (2021) | 2021 | 2,147 rows |

---

## Schema Definitions

### RAW.WASTEWATER_SURVEILLANCE

**Purpose**: Viral RNA measurements from wastewater treatment plants

**Key Columns**:
```
epi_year NUMBER          -- Epidemiological year (2020-2025)
epi_week NUMBER          -- Week number (1-53)
week_start DATE          -- Sunday of the week
virus_code VARCHAR(50)   -- covN2, fluA, fluB, rsv
virus_name VARCHAR(100)  -- COVID-19, Influenza A, Influenza B, RSV
location VARCHAR(200)    -- Treatment site (e.g., "Peel G.E. Booth")
viral_load_avg FLOAT     -- Population-weighted viral load (copies/mL)
```

**Quality Rules**:
- `province` = 'Ontario'
- `viral_load_avg` >= 0
- `epi_week` BETWEEN 1 AND 53

### RAW.ED_WAIT_TIMES

**Purpose**: Emergency department wait times (scraped)

**Key Columns**:
```
scraped_at TIMESTAMP_NTZ     -- When we scraped the data
hospital_code VARCHAR(50)    -- georgetown, milton, oakville
hospital_name VARCHAR(200)   -- Full hospital name
wait_total_minutes NUMBER    -- Physician wait time
```

**Quality Rules**:
- `wait_total_minutes` <= 720 (12 hours max, sanity check)
- `hospital_code` IN ('georgetown', 'milton', 'oakville')

**Severity Classification**:
- Low: ≤60 min
- Moderate: 61-120 min
- High: 121-240 min
- Critical: >240 min

### RAW.SCHOOL_CASES

**Purpose**: Historical school infection cases (2021)

**Key Columns**:
```
reported_date DATE         -- Report date
school_name VARCHAR(500)   -- School name
municipality VARCHAR(200)  -- City/town
confirmed_cases NUMBER     -- Daily cases
```

**Coverage**: September - December 2021 only (archived)

---

## MARTS Layer

### MARTS_SURVEILLANCE (Current Data)

**Schema Purpose**: Actionable current surveillance data

| Object | Type | Refresh | Description |
|--------|------|---------|-------------|
| `fct_wastewater_weekly` | Dynamic Table | 1 hour | Viral loads by location/week |
| `rpt_viral_trends` | Dynamic Table | 1 hour | Week-over-week trends |
| `rpt_ed_wait_times` | Dynamic Table | 30 min | ED wait history with severity |
| `rpt_current_week` | View | Instant | Latest week viral summary |
| `rpt_ed_current` | View | Instant | Current ED wait times |

### MARTS_HISTORICAL (Reference Data)

**Schema Purpose**: Archived data for comparison

| View | Description |
|------|-------------|
| `fct_school_cases` | 2021 school infection records |
| `rpt_school_summary` | Weekly school case aggregates |

### MARTS_OPS (Operations)

**Schema Purpose**: Pipeline health monitoring

| View | Description |
|------|-------------|
| `rpt_data_freshness` | Latest ingestion timestamps |
| `rpt_ingestion_log` | Run history with errors |

---

## Dynamic Tables (Auto-Refresh)

### fct_wastewater_weekly

**Materialization**: Dynamic Table (not dbt)

```sql
CREATE DYNAMIC TABLE MARTS_SURVEILLANCE.fct_wastewater_weekly
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
  SELECT epi_year, epi_week, virus_name, location, viral_load_avg
  FROM RAW.WASTEWATER_SURVEILLANCE
  WHERE province = 'Ontario';
```

**Behavior**: Automatically refreshes within 1 hour when new data arrives in RAW.WASTEWATER_SURVEILLANCE

### rpt_ed_wait_times

**Materialization**: Dynamic Table

**Target Lag**: 30 minutes

**Behavior**: Refreshes when ED_WAIT_TIMES gets new scrapes

---

## Data Quality SLAs

| Dataset | Completeness | Timeliness | Action if Breached |
|---------|--------------|------------|-------------------|
| Wastewater | ≥95% of weeks | ≤7 days lag | Check Health Canada source |
| ED Wait Times | ≥90% uptime | ≤6 hours lag | Check scraper |
| School Cases | 100% (static) | N/A | N/A |

**Monitoring**: Query `MARTS_OPS.rpt_data_freshness` daily

---

## API Endpoints

### Health Canada Wastewater

**URL**: `https://health-infobase.canada.ca/src/data/wastewater/wastewater_aggregate.csv`

**Format**: CSV (direct download)

**Columns**: Location, site, city, province, EpiYear, EpiWeek, weekstart, measureid, w_avg, min, max

**Viruses**: covN2 (COVID-19), fluA (Influenza A), fluB (Influenza B), rsv (RSV)

**Update Frequency**: Weekly (Mon/Tue)

### Halton Healthcare ED

**URL**: `https://www.haltonhealthcare.on.ca/emergency-department`

**Format**: HTML (scraped)

**Pattern**: `{hospital}(\d+)\s*Hour\(s\)\s*and\s*(\d+)\s*Minute\(s\)`

**Update Frequency**: ~30 minutes on source

**Risk**: Scraper breaks if HTML structure changes

---

## File Organization

```
pipeline/
├── config.py              # Snowflake connection, constants
├── base_ingestor.py       # Reusable CKAN API class
├── ingest_wastewater.py   # Health Canada ingestor
├── ingest_ed_wait_times.py # Halton Healthcare scraper
├── run_ingestion.py       # Unified entry point
├── test_snowflake.py      # Connection tester
└── tests/                 # Python unit tests
    ├── test_config.py
    └── test_ingestors.py

sql/
└── migrations/            # All SQL migrations (run in order)
    ├── 001_setup_database.sql
    ├── 002_create_tables.sql
    ├── 003_create_views.sql
    ├── 004_marts_schemas.sql
    └── 005_create_service_account.sql

ontario_health/            # dbt project
├── models/staging/        # Source definitions + tests
├── models/marts/          # Documentation models
└── profiles.yml           # Service account connection
```

---

## Automation (GitHub Actions)

**Workflow**: `.github/workflows/weekly-ingest.yml`

**Schedule**:
```yaml
- cron: '0 11 * * 3'      # Wed 6am EST - wastewater
- cron: '0 */3 * * *'     # Every 3 hours - ED
```

**Secrets Required**:
- `SNOWFLAKE_PAT_TOKEN`
- `SNOWFLAKE_ACCOUNT` = `BMWIVTO-JF10661`
- `SNOWFLAKE_USER` = `SBOSE78`

---

## Monitoring Queries

### Check Data Freshness
```sql
SELECT * FROM MARTS_OPS.rpt_data_freshness;
```

### Current Respiratory Surveillance
```sql
SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week;
```

### Current ED Wait Times
```sql
SELECT * FROM MARTS_SURVEILLANCE.rpt_ed_current;
```

### Dynamic Table Refresh Status
```sql
SHOW DYNAMIC TABLES IN DATABASE ONTARIO_HEALTH;

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name = 'FCT_WASTEWATER_WEEKLY'
ORDER BY refresh_start_time DESC
LIMIT 5;
```

---

## Performance & Cost

**Warehouse**: COMPUTE_WH (X-SMALL, 60s auto-suspend)

**Expected Monthly Cost**:
- Dynamic Table refreshes: ~$2-5 (only when data changes)
- GitHub Actions: Free (360 min/month, under 2,000 limit)

**Query Performance**:
- MARTS queries: <1 second (pre-aggregated)
- RAW queries: 1-3 seconds (6K-50K rows)

---

## Setup Repeatability

All setup is 100% reproducible via versioned SQL migrations:

1. **SQL Migrations**: `sql/migrations/001_*.sql` through `005_*.sql` (run in order)
2. **Python**: `make setup` creates venv from requirements.txt
3. **Keys**: Generate once with `make setup`, stored in `~/.snowflake/`

To set up a new environment:
```bash
make setup      # Python env + generate keys
make migrate    # Lists SQL files to run
# Run each migration in Snowflake Web UI in order
make test       # Verify Python connection
make test-dbt   # Verify service account connection
```

## Adding New Data Sources

1. Create `pipeline/ingest_*.py` (inherit from `BaseIngestor` if CKAN)
2. Add RAW table in a new migration file `sql/migrations/00X_*.sql`
3. Add to `pipeline/run_ingestion.py` choices
4. Create MARTS view or Dynamic Table in a new migration file
5. Update `MARTS_OPS.rpt_data_freshness`
6. Add Python tests in `pipeline/tests/`
7. Document in AGENTS.md
8. Add to Makefile and GitHub Actions

---

## Known Issues

### ED Scraper Fragility

**Issue**: HTML parsing breaks if website structure changes

**Detection**: Zero records in RAW.ED_WAIT_TIMES or freshness >6 hours

**Fix**: Update regex in `ingest_ed_wait_times.py`

### Wastewater Coverage Varies

**Issue**: Some weeks show 2 sites instead of 11

**Reason**: Upstream data quality (Health Canada)

**Not a bug**: This is how the source data is

---

## Change Log

### v1.0 (2025-12-28)
- Initial implementation
- Wastewater surveillance (Fall 2025)
- ED wait times (Halton Healthcare)
- Historical school data (2021)
- Dynamic Tables for auto-refresh
- GitHub Actions automation

