# Ontario Health Data Pipeline - AI Agent Guide

**Last Updated**: December 28, 2025

This document is for AI assistants (like Cursor, GitHub Copilot) working on this codebase. It captures architectural decisions, operational procedures, and important context that may not be obvious from the code alone.

---

## Project Overview

**Purpose**: Automated data pipeline for Ontario respiratory surveillance data, focusing on pediatric/school-related infections.

**Data Sources**:
- Health Canada Wastewater Surveillance (Fall 2025 data)
- Halton Healthcare ED Wait Times (web scraping)
- Ontario historical school cases (2021, archived)

**Tech Stack**: Python ingestion → Snowflake → Dynamic Tables (auto-refresh) → dbt (DDL + tests)

---

## Architecture Decisions

### Why Snowflake Dynamic Tables (not dbt materialized tables)?

**Decision**: Use Snowflake Dynamic Tables for MARTS, with dbt only for DDL generation and testing.

**Reasoning**:
- Dynamic Tables auto-refresh when upstream data changes (no orchestrator needed)
- Cost-efficient: Snowflake only computes when data arrives
- Simpler than running dbt on cron
- dbt still useful for SQL organization, tests, and documentation

**Implementation**:
```sql
-- dbt generates this, Snowflake maintains it
CREATE DYNAMIC TABLE MARTS_SURVEILLANCE.fct_wastewater_weekly
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS SELECT ...
```

### Why MARTS_SURVEILLANCE / MARTS_HISTORICAL / MARTS_OPS?

**Decision**: Split MARTS into three schemas by domain.

**Reasoning**:
- **MARTS_SURVEILLANCE**: Current actionable data (wastewater, ED wait times)
- **MARTS_HISTORICAL**: Reference data from 2021 (school cases, outbreaks)
- **MARTS_OPS**: Pipeline health and data quality monitoring

This separation makes it clear which data is current vs archived.

### Why Web Scraping for ED Wait Times?

**Decision**: Scrape Halton Healthcare website every 3 hours.

**Reasoning**:
- No official API exists
- Website updates every 30 minutes
- Provides current hospital capacity context alongside viral surveillance

**Risk**: Scraper is fragile—website changes will break it. Monitor `MARTS_OPS.rpt_data_freshness` for stale data.

---

## Data Source Details

### Wastewater Surveillance

**Source**: `https://health-infobase.canada.ca/src/data/wastewater/wastewater_aggregate.csv`

**Refresh**: Weekly (Mon/Tue), our ingestion runs Wednesdays 6am EST

**Coverage**: Ontario sites (Toronto, Peel, London, Kingston, etc.)

**Viruses**: COVID-19 (covN2), Influenza A (fluA), Influenza B (fluB), RSV (rsv)

**Key Field**: `viral_load_avg` - higher = more community transmission

**Note**: This is a leading indicator—wastewater signals precede clinical cases by 4-7 days.

### ED Wait Times

**Source**: `https://www.haltonhealthcare.on.ca/emergency-department` (scraped)

**Refresh**: Every 3 hours via GitHub Actions

**Coverage**: 3 Halton Healthcare hospitals
- Georgetown Hospital
- Milton District Hospital
- Oakville Trafalgar Memorial Hospital

**Key Field**: `wait_total_minutes` - physician wait time

**Severity Classification**:
- Low: ≤60 min
- Moderate: 61-120 min
- High: 121-240 min
- Critical: >240 min

**Expansion Limitations**:
Most Ontario hospitals use JavaScript-rendered dashboards or don't publish live wait times.
Of 163 EDs in Ontario, only ~10-15 have scrapeable static HTML.
Framework supports multi-network ingestion (see `pipeline/hospital_scrapers/`),
but expanding beyond Halton requires Selenium/Playwright for JavaScript sites.

**Risk**: HTML structure changes break the scraper. Check for zero records in `RAW.ED_WAIT_TIMES`.

### Historical School Cases (2021)

**Source**: Ontario Data Catalogue (archived)

**Coverage**: Sep-Dec 2021 only

**Note**: Ontario stopped publishing this data after COVID emergency ended. Kept for historical reference.

---

## Schema Conventions

### Naming

- `fct_*`: Fact tables (granular, event-level)
- `dim_*`: Dimension tables (lookup/reference)
- `rpt_*`: Pre-aggregated reports
- `*_current`: Latest snapshot only
- `*_trends`: Time-series with week-over-week calculations

### Materialization Strategy

| Layer | Method | Target Lag | Use Case |
|-------|--------|------------|----------|
| RAW → STAGING | Dynamic Table | 15 min | Type casting, NULL handling |
| STAGING → MARTS | Dynamic Table | 30-60 min | Business logic, aggregations |
| Simple aggregations | Materialized View | Instant | SUM/AVG/COUNT only |
| Complex/ad-hoc | View | On-demand | Rarely queried reports |

---

## Deployment Procedures

### Initial Setup (100% Repeatable)

1. **Python environment**: `make setup` (creates venv + generates RSA key pair)
2. **Snowflake migrations**: `make migrate` (lists SQL files)
   - Run all migrations in `sql/migrations/` in order (001-006)
   - **Critical**: Run `005_create_service_account.sql` to create the user
3. **Test connection**: `make test`
4. **Load data**: `make ingest-all`

**Authentication**: Service account (`ontario_health_svc`) with key-pair auth for everything.

All SQL is versioned in `sql/migrations/` - no manual setup needed beyond running the migration scripts in order.

### Data Ingestion (Manual)

```bash
# One-time historical data (already loaded)
python pipeline/run_ingestion.py school_cases
python pipeline/run_ingestion.py outbreaks

# Current surveillance data
python pipeline/run_ingestion.py wastewater      # Weekly
python pipeline/run_ingestion.py ed_wait_times   # Every 3h

# All current datasets
python pipeline/run_ingestion.py all
```

### Automated Ingestion (GitHub Actions)

**File**: `.github/workflows/weekly-ingest.yml`

**Schedule**:
- Wastewater: Wednesdays 6am EST
- ED Wait Times: Every 3 hours
- On merge to main: All ingestors

**Secrets Required**:
- `SNOWFLAKE_ACCOUNT` - `BMWIVTO-JF10661`
- `SNOWFLAKE_USER` - `ontario_health_svc`
- `SNOWFLAKE_PRIVATE_KEY` - Contents of `~/.snowflake/ontario_health_key.p8`

**Authentication**: Service account with key-pair (same for local + CI/CD)

**Manual Trigger**: GitHub → Actions → "Ontario Health Data Ingestion" → Run workflow

### Adding a New Data Source

1. Create ingestor in `pipeline/ingest_*.py` (inherit from `BaseIngestor` if CKAN)
2. Add to `pipeline/run_ingestion.py` choices and logic
3. Create RAW table DDL
4. Add to `MARTS_OPS.rpt_data_freshness` view
5. Create MARTS views/Dynamic Tables
6. Update `.github/workflows/weekly-ingest.yml` if automated
7. Document in this file (AGENTS.md) and `SPEC.md`

---

## Known Limitations

### ED Wait Times Scraping

**Issue**: Website scraping is fragile.

**Detection**: Check `MARTS_OPS.rpt_data_freshness` - if latest_ingestion is >6 hours old, scraper likely broke.

**Fix**: Inspect HTML at `https://www.haltonhealthcare.on.ca/emergency-department`, update regex in `pipeline/ingest_ed_wait_times.py`.

### Wastewater Data Gaps

**Issue**: Some weeks have incomplete Ontario site coverage (only 2 sites vs 11).

**Reason**: Sites occasionally miss reporting to Health Canada.

**Impact**: `rpt_current_week` may show fewer sites than usual. This is upstream data quality, not our bug.

### Historical Data is Archived

**Issue**: School cases and outbreaks are from 2021 only.

**Reason**: Ontario stopped publishing after COVID emergency.

**Future**: If Ontario resumes publishing, add to weekly ingestion schedule.

---

## Troubleshooting

### Connection Errors

**Symptom**: `JWT token is invalid` or `User does not exist`

**Fix**: 
1. Verify service account exists: Run `sql/migrations/005_create_service_account.sql`
2. Check private key exists: `ls ~/.snowflake/ontario_health_key.p8`
3. Verify network policy includes your IP: Snowflake Admin → Security → Network Policies
4. Test: `make test`

### Stale MARTS Data

**Symptom**: MARTS views don't reflect new RAW data

**Fix**:
1. Check if Dynamic Tables exist: `SHOW DYNAMIC TABLES IN SCHEMA MARTS_SURVEILLANCE`
2. If only views exist, they're not auto-refreshing. Convert to Dynamic Tables (see `sql/04_marts_schemas.sql`)
3. Check Dynamic Table refresh status: `SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())`

### GitHub Actions Failures

**Symptom**: Workflow runs fail

**Common causes**:
1. PAT token expired → Regenerate and update secret
2. Website structure changed (ED scraper) → Check logs, update regex
3. Snowflake IP restriction → Add GitHub Actions IP range

---

## Performance Notes

### Cost Optimization

- Dynamic Tables only compute when upstream changes (cost-efficient)
- Warehouse `COMPUTE_WH` is X-SMALL, auto-suspend 60 seconds
- GitHub Actions: ~8 ED runs/day + 1 wastewater/week = ~360 min/month (well under 2,000 free limit)

### Query Performance

- MARTS views are fast (pre-aggregated)
- For large date ranges on RAW tables, filter by `ingested_at` first
- `fct_wastewater_weekly` has ~6K rows (Ontario only), very fast

---

## Testing Strategy

### Why Tests Matter in a Data Pipeline

**Tests are the only way to know if your data is trustworthy.** Unlike application code where bugs are obvious (crashes, errors), data quality issues are silent—bad data flows through the pipeline unnoticed until someone makes a bad decision based on it.

**Types of Tests**:

1. **Python Unit Tests** (`pipeline/tests/`)
   - Validate ingestion logic (parsing, transformations)
   - Catch bugs before data reaches Snowflake
   - Fast feedback (run in <1 second)

2. **dbt Data Quality Tests** (`ontario_health/models/`)
   - Validate data in Snowflake (nulls, ranges, relationships)
   - Catch upstream source issues (API changes, bad data)
   - Run against actual data, not mocks

### Test Coverage

**Python Tests** (13 tests):
```bash
make test-python    # Runs in ~0.5 seconds
```

Coverage:
- Configuration validation (account, schemas, token)
- Wastewater transformation logic (virus mapping, date handling)
- ED scraper HTML parsing (regex patterns)
- Data quality rules (non-negative viral loads, wait time sanity)

**dbt Tests** (defined in `models/staging/staging_tests.yml`):
```bash
make test-dbt       # Queries actual Snowflake data
```

Coverage:
- Primary key uniqueness
- Required fields not null
- Value constraints (epi_week 1-53, viral_load >= 0)
- Referential integrity (virus codes match list)
- Data recency (wastewater <14 days, ED <6 hours)

### When Tests Fail

**Python test failure** = Code bug → Fix before deploying

**dbt test failure** = Data quality issue → Investigate source

Examples:
- `viral_load < 0` → Health Canada CSV corrupted
- `wait_time > 720 min` → ED scraper parsing wrong field
- `epi_week = 54` → Upstream data error
- `recency > 14 days` → GitHub Actions not running

### Running Tests

```bash
# Quick validation (unit tests)
make test-python

# Full data quality check (queries Snowflake)
make test-dbt

# Both
make test-all
```

**Best Practice**: Run `make test-python` before committing code changes.

---

## Future Enhancements

### Potential Additions

1. **PHU-level testing data** - If Ontario resumes publishing
2. **Hospital capacity metrics** - If API becomes available
3. **Alerts** - Slack/email when viral load spikes or ED wait times critical
4. **Dashboards** - Preset/Tableau integration
5. **More dbt tests** - Cross-dataset validation (e.g., viral load correlates with ED volume)

---

## Contact / Questions

For questions about architecture or operational issues, refer to:
- This file (AGENTS.md)
- `SPEC.md` for schema specifications and data contracts
- `README.md` for user-facing documentation
- GitHub Issues for bugs/feature requests

