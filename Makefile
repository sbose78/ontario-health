# Ontario Health Data Pipeline - Makefile
# All operational commands in one place

.PHONY: help setup test db-setup ingest-wastewater ingest-ed ingest-all verify clean

# Python environment
VENV = .venv
PYTHON = $(shell pwd)/$(VENV)/bin/python
PIP = $(shell pwd)/$(VENV)/bin/pip

# Directories
PIPELINE = pipeline
SQL = sql
DBT = ontario_health

help:
	@echo "Ontario Health Data Pipeline - Available Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup          Create venv and install dependencies"
	@echo "  make test           Test Snowflake connection"
	@echo "  make db-setup       Create Snowflake database objects"
	@echo "  make migrate        List migration scripts to run in Snowflake"
	@echo ""
	@echo "Data Ingestion:"
	@echo "  make ingest-wastewater   Fetch latest wastewater surveillance data"
	@echo "  make ingest-ed           Fetch current ED wait times"
	@echo "  make ingest-all          Run all ingestors"
	@echo ""
	@echo "Monitoring:"
	@echo "  make verify         Check data freshness and pipeline health"
	@echo "  make status         Show current surveillance snapshot"
	@echo ""
	@echo "Testing:"
	@echo "  make test-python    Run Python unit tests"
	@echo "  make test-dbt       Run dbt data quality tests"
	@echo "  make test-all       Run all tests"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-compile    Compile dbt models (validate SQL)"
	@echo "  make dbt-docs       Generate and serve dbt documentation"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean          Clean up temp files and caches"

# Setup and initialization
setup:
	@echo "Setting up Python environment..."
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r $(PIPELINE)/requirements.txt
	@echo "✓ Setup complete. Activate with: source $(VENV)/bin/activate"

test:
	@echo "Testing Snowflake connection..."
	@$(PYTHON) $(PIPELINE)/test_snowflake.py

db-setup:
	@echo "Creating Snowflake database objects..."
	@$(PYTHON) $(PIPELINE)/test_snowflake.py --setup
	@echo ""
	@echo "⚠️  Manual step required:"
	@echo "   Run: sql/migrations/001_create_service_account.sql"
	@echo "   in Snowflake Web UI to set up service account for dbt"

migrate:
	@echo "Migrations must be run in Snowflake Web UI:"
	@echo ""
	@ls -1 $(SQL)/migrations/*.sql 2>/dev/null | while read f; do \
		echo "  $$f"; \
	done
	@echo ""
	@echo "Run these SQL files in order as ACCOUNTADMIN"

# Data ingestion
ingest-wastewater:
	@echo "Ingesting wastewater surveillance data..."
	@$(PYTHON) $(PIPELINE)/run_ingestion.py wastewater

ingest-ed:
	@echo "Ingesting ED wait times..."
	@$(PYTHON) $(PIPELINE)/run_ingestion.py ed_wait_times

ingest-all:
	@echo "Running all ingestors..."
	@$(PYTHON) $(PIPELINE)/run_ingestion.py all

# Monitoring and verification
verify:
	@echo "Checking data freshness..."
	@$(PYTHON) $(PIPELINE)/show_freshness.py

status:
	@echo "Current Surveillance Status:"
	@$(PYTHON) $(PIPELINE)/show_status.py

# Testing
test-python:
	@echo "Running Python unit tests..."
	@$(PYTHON) -m pytest $(PIPELINE)/tests/ -v

test-dbt:
	@echo "Running dbt data quality tests..."
	@cd $(DBT) && export SNOWFLAKE_TOKEN=$$(cat ~/.snowflake/ontario_health_token) && \
		../$(VENV)/bin/dbt test --profiles-dir .

test-all: test-python
	@echo ""
	@echo "✓ All Python tests passed"
	@echo ""
	@echo "Note: dbt tests require password/SSO auth (see ontario_health/NOTE_DBT_AUTH.md)"

# dbt commands
dbt-compile:
	@echo "Compiling dbt models..."
	@cd $(DBT) && export SNOWFLAKE_TOKEN=$$(cat ~/.snowflake/ontario_health_token) && \
		../$(VENV)/bin/dbt compile --profiles-dir .

dbt-test:
	@echo "Running dbt tests..."
	@cd $(DBT) && export SNOWFLAKE_TOKEN=$$(cat ~/.snowflake/ontario_health_token) && \
		../$(VENV)/bin/dbt test --profiles-dir .

dbt-docs:
	@echo "Generating dbt documentation..."
	@cd $(DBT) && export SNOWFLAKE_TOKEN=$$(cat ~/.snowflake/ontario_health_token) && \
		../$(VENV)/bin/dbt docs generate --profiles-dir . && \
		../$(VENV)/bin/dbt docs serve --profiles-dir .

# Maintenance
clean:
	@echo "Cleaning up..."
	rm -rf $(DBT)/target
	rm -rf $(DBT)/dbt_packages
	rm -rf $(DBT)/logs
	rm -rf logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✓ Cleanup complete"

# Quick reference
.DEFAULT_GOAL := help

