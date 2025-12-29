# Ontario Health dbt Project

**Note**: This dbt project uses Snowflake Dynamic Tables for materialization instead of dbt's built-in materializations.

## Why This Approach?

Traditional dbt materializes tables by running `dbt run` on a schedule. We use Snowflake Dynamic Tables which automatically refresh when upstream data changes, eliminating the need for external orchestration.

**dbt's role here:**
- SQL organization and templating
- Data quality tests
- Documentation and lineage graphs
- Source definitions

**Snowflake's role:**
- Dynamic Tables handle materialization
- Auto-refresh on data arrival
- Cost-efficient (only computes when needed)

## Setup

### 1. Authentication

dbt with PAT tokens requires a helper script:

```bash
cd ontario_health
export SNOWFLAKE_TOKEN=$(cat ~/.snowflake/ontario_health_token)
dbt debug --profiles-dir .
```

Or use the Python wrapper:

```bash
python run_dbt.py test
python run_dbt.py docs generate
```

### 2. Run dbt Commands

```bash
# Run tests
dbt test --profiles-dir .

# Generate documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .

# Compile SQL (doesn't execute)
dbt compile --profiles-dir .
```

## Project Structure

```
models/
├── staging/          # Source definitions and light transformations
│   ├── sources.yml   # Defines RAW tables
│   └── stg_*.sql     # Staging models (documentation)
├── marts/
│   ├── surveillance/ # Current respiratory data (Dynamic Tables)
│   ├── historical/   # Archived reference data
│   └── ops/          # Operations monitoring
└── schema.yml        # Model documentation and tests
```

## Dynamic Tables (Already Created)

The following are managed as Snowflake Dynamic Tables (not dbt materializations):

| Table | Schema | Target Lag | Purpose |
|-------|--------|------------|---------|
| fct_wastewater_weekly | MARTS_SURVEILLANCE | 1 hour | Viral loads by location/week |
| rpt_ed_wait_times | MARTS_SURVEILLANCE | 30 min | ED wait times with severity |
| rpt_viral_trends | MARTS_SURVEILLANCE | 1 hour | Week-over-week trends |

These auto-refresh when RAW data changes—no dbt run needed.

## Testing

dbt tests validate data quality without managing materialization:

```bash
# Run all tests
dbt test --profiles-dir .

# Test specific model
dbt test --select stg_wastewater --profiles-dir .
```

## Documentation

```bash
# Generate and serve docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Visit http://localhost:8080 to browse the lineage graph.

## For More Information

- **AGENTS.md**: Architecture decisions and operations
- **SPEC.md**: Schema definitions and data contracts
- **../.cursorrules**: Coding conventions
