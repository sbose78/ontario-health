# Ontario Health Data Pipeline

Automated Snowflake pipeline for Ontario respiratory surveillance data, focusing on pediatric/school populations and hospital capacity.

---

## Quick Start

```bash
# 1. Initial setup
make setup              # Install dependencies
make test               # Test Snowflake connection
make db-setup           # Create database objects

# 2. Run migrations (in Snowflake Web UI)
make migrate            # Shows list of SQL files to run

# 3. Load data
make ingest-wastewater  # Load Fall 2025 data
make ingest-ed          # Load current ED wait times
make status             # View current data
```

**Setup Requirements:**
1. Snowflake PAT token in `~/.snowflake/ontario_health_token` (for Python)
2. Service account keys in `~/.snowflake/ontario_health_key.p8` (for dbt)
3. Python 3.11+
4. Network policy allows your IP

**Keys Already Generated**: Run `make migrate` to see all SQL migrations.

---

## What's Inside

**Current Data (Fall 2025)**:
- **Wastewater surveillance** - COVID-19, Flu A/B, RSV through Week 51, 2025
- **ED wait times** - Halton Healthcare capacity every 3 hours

**Historical Data (2021)**:
- School cases: 50,299 records
- Education outbreaks: 2,147 records

**Auto-Refresh**: Snowflake Dynamic Tables update within 1 hour when new data arrives (no manual refresh needed).

---

## Current Surveillance Snapshot

Run `make status` to see:

```
--- Respiratory (Week 51, 2025) ---
Influenza A     | Load:  62.40 | 2 sites  ← Very High
COVID-19        | Load:  21.51 | 2 sites
RSV             | Load:   4.13 | 2 sites
Influenza B     | Load:   0.63 | 2 sites

--- ED Wait Times (Current) ---
Oakville Hospital | 244min | Critical
Milton Hospital   | 157min | High
Georgetown        | 108min | Moderate
```

---

## Commands

```bash
# Data Access
make status         # Show current surveillance snapshot
make verify         # Check data freshness

# Data Refresh (manual)
make ingest-wastewater  # Fetch latest wastewater data
make ingest-ed          # Fetch current ED wait times
```

---

## Key Snowflake Objects

### MARTS_SURVEILLANCE (Current - Auto-Refresh)

| Object | Type | Refresh | Rows |
|--------|------|---------|------|
| `fct_wastewater_weekly` | Dynamic Table | 1 hour | 6,248 |
| `rpt_viral_trends` | Dynamic Table | 1 hour | 412 |
| `rpt_ed_wait_times` | Dynamic Table | 30 min | 3 |
| `rpt_current_week` | View | Instant | 4 |
| `rpt_ed_current` | View | Instant | 3 |

### MARTS_HISTORICAL (Reference)

| View | Records | Period |
|------|---------|--------|
| `fct_school_cases` | 50,299 | Sep-Dec 2021 |
| `rpt_school_summary` | Weekly | 2021 |

### MARTS_OPS (Monitoring)

| View | Purpose |
|------|---------|
| `rpt_data_freshness` | Data recency check |
| `rpt_ingestion_log` | Pipeline run history |

---

## Automation

**GitHub Actions** (`.github/workflows/weekly-ingest.yml`):
- Wastewater: Weekly (Wed 6am EST)
- ED Wait Times: Every 3 hours

**To Enable**:
1. Push to GitHub
2. Add secrets: `SNOWFLAKE_PAT_TOKEN`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`

---

## SQL Queries

### Current Week Respiratory Surveillance
```sql
SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week;
```

### Current ED Wait Times
```sql
SELECT * FROM MARTS_SURVEILLANCE.rpt_ed_current
ORDER BY wait_total_minutes DESC;
```

### 4-Week Viral Trends
```sql
SELECT 
    epi_week,
    virus_name,
    avg_viral_load,
    prev_week_avg,
    week_over_week_pct
FROM MARTS_SURVEILLANCE.rpt_viral_trends
WHERE epi_year = 2025 AND epi_week >= 48
ORDER BY epi_week DESC, virus_name;
```

---

## Architecture

### Data Flow
```
Health Canada CSV → Python → RAW tables → Dynamic Tables → MARTS
                                    ↓
                             Streams (change tracking)
```

### Snowflake Schemas

| Schema | Purpose | Objects |
|--------|---------|---------|
| `RAW` | Landing zone | Tables + Streams |
| `STAGING` | Transformations | (Reserved for dbt) |
| `MARTS_SURVEILLANCE` | Current data | Dynamic Tables + Views |
| `MARTS_HISTORICAL` | 2021 reference | Views |
| `MARTS_OPS` | Monitoring | Views |

---

## Project Structure

```
ontariohealth/
├── Makefile                    # All commands
├── README.md                   # This file (user guide)
├── AGENTS.md                   # AI assistant guide
├── SPEC.md                     # Technical specifications
├── pipeline/                   # Python ingestion
│   ├── config.py
│   ├── ingest_wastewater.py
│   ├── ingest_ed_wait_times.py
│   └── run_ingestion.py
├── sql/migrations/             # All SQL (run in order)
│   ├── 001_setup_database.sql
│   ├── 002_create_tables.sql
│   ├── 003_create_views.sql
│   ├── 004_marts_schemas.sql   # Dynamic Tables
│   └── 005_create_service_account.sql
├── ontario_health/             # dbt project
│   └── models/                 # Documentation models
└── .github/workflows/          # Automation
```

---

## Documentation

| File | Audience | Purpose |
|------|----------|---------|
| **README.md** | Users/analysts | Setup, usage, queries |
| **AGENTS.md** | AI assistants | Architecture, operations, troubleshooting |
| **SPEC.md** | Engineers | Schemas, APIs, contracts |

---

## Troubleshooting

### Connection Errors

**Error**: `Network policy is required`

**Fix**: Add your IP in Snowflake: Admin → Security → Network Policies

### Stale Data

**Check**: `make verify`

**Expected**:
- Wastewater: Updated within 7 days
- ED Wait Times: Updated within 6 hours

### Scraper Failures

**Check**: `SELECT * FROM MARTS_OPS.rpt_ingestion_log ORDER BY started_at DESC`

**Common Cause**: Halton Healthcare website HTML changed

**Fix**: Update regex in `pipeline/ingest_ed_wait_times.py`

---

## Need Help?

- Architecture questions → `AGENTS.md`
- Schema questions → `SPEC.md`
- Data quality → `make verify`
- Pipeline health → `MARTS_OPS.rpt_ingestion_log`

---

## Developer Reference

### Setup Commands

```bash
# First-time setup
make setup          # Create venv, install packages
make test           # Test Snowflake connection
make db-setup       # Create Snowflake objects

# All commands
make help           # Show all available commands
```

### dbt Commands

```bash
make dbt-compile    # Validate SQL
make dbt-test       # Run data quality tests
make dbt-docs       # Generate documentation
```

### Testing

```bash
# Python unit tests (fast, validates ingestion logic)
make test-python

# dbt tests (validates data in Snowflake)
make test-dbt

# Run all tests
make test-all
```

### Maintenance

```bash
make clean          # Clean temp files and caches
```
