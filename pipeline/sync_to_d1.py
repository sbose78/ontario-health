#!/usr/bin/env python3
"""
Sync Snowflake MARTS data to Cloudflare D1 cache.

This script:
1. Queries Snowflake MARTS (using service account)
2. Pushes data to Cloudflare D1 via API
3. Runs every 30 min via GitHub Actions

D1 acts as a fast cache layer for the public dashboard.
"""
import json
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import get_snowflake_connection

D1_DATABASE_ID = "1b818f56-47aa-4c11-b1fa-4c9e98009d0e"
D1_DATABASE_NAME = "ontario-health-cache"


def query_snowflake(sql: str) -> list[dict]:
    """Query Snowflake and return results as list of dicts."""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(sql)
        
        # Get column names
        columns = [col[0] for col in cur.description]
        
        # Fetch all rows
        rows = cur.fetchall()
        
        # Convert to list of dicts
        return [dict(zip(columns, row)) for row in rows]
        
    finally:
        cur.close()
        conn.close()


def sync_current_week():
    """Sync current week respiratory data."""
    print("Syncing current week data...")
    
    rows = query_snowflake("SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week")
    
    if not rows:
        print("  No data returned")
        return
    
    # Clear and insert
    sql = "DELETE FROM current_week;\n"
    
    for row in rows:
        sql += f"""
INSERT INTO current_week (virus_name, epi_year, epi_week, sites_reporting, avg_viral_load, max_viral_load, min_viral_load)
VALUES ('{row['VIRUS_NAME']}', {row['EPI_YEAR']}, {row['EPI_WEEK']}, {row['SITES_REPORTING']}, 
        {row['AVG_VIRAL_LOAD']}, {row['MAX_VIRAL_LOAD']}, {row['MIN_VIRAL_LOAD']});
"""
    
    execute_d1_sql(sql)
    print(f"  ✓ Synced {len(rows)} viruses")


def sync_ed_status():
    """Sync ED wait times."""
    print("Syncing ED wait times...")
    
    rows = query_snowflake("SELECT * FROM MARTS_SURVEILLANCE.rpt_ed_current")
    
    if not rows:
        print("  No data returned")
        return
    
    sql = "DELETE FROM ed_current;\n"
    
    for row in rows:
        scraped_at = row['SCRAPED_AT'].isoformat() if row['SCRAPED_AT'] else ''
        sql += f"""
INSERT INTO ed_current (hospital_name, wait_hours, wait_minutes, wait_total_minutes, source_updated, scraped_at, wait_severity)
VALUES ('{row['HOSPITAL_NAME']}', {row['WAIT_HOURS']}, {row['WAIT_MINUTES']}, {row['WAIT_TOTAL_MINUTES']}, 
        '{row['SOURCE_UPDATED'] or ''}', '{scraped_at}', '{row['WAIT_SEVERITY']}');
"""
    
    execute_d1_sql(sql)
    print(f"  ✓ Synced {len(rows)} hospitals")


def sync_viral_trends():
    """Sync 4-week viral trends."""
    print("Syncing viral trends...")
    
    rows = query_snowflake("""
        SELECT * FROM MARTS_SURVEILLANCE.rpt_viral_trends
        WHERE epi_year = 2025 AND epi_week >= 48
        ORDER BY epi_week DESC, virus_name
    """)
    
    sql = "DELETE FROM viral_trends;\n"
    
    for row in rows:
        prev_week = row['PREV_WEEK_AVG'] if row['PREV_WEEK_AVG'] is not None else 'NULL'
        week_pct = row['WEEK_OVER_WEEK_PCT'] if row['WEEK_OVER_WEEK_PCT'] is not None else 'NULL'
        
        sql += f"""
INSERT INTO viral_trends (epi_year, epi_week, virus_name, avg_viral_load, prev_week_avg, week_over_week_pct)
VALUES ({row['EPI_YEAR']}, {row['EPI_WEEK']}, '{row['VIRUS_NAME']}', {row['AVG_VIRAL_LOAD']}, {prev_week}, {week_pct});
"""
    
    execute_d1_sql(sql)
    print(f"  ✓ Synced {len(rows)} trend rows")


def sync_data_freshness():
    """Sync data freshness metadata."""
    print("Syncing data freshness...")
    
    rows = query_snowflake("""
        SELECT * FROM MARTS_OPS.rpt_data_freshness 
        WHERE category = 'surveillance'
    """)
    
    sql = "DELETE FROM data_freshness;\n"
    
    for row in rows:
        latest_date = row['LATEST_DATA_DATE'].isoformat() if row['LATEST_DATA_DATE'] else ''
        sql += f"""
INSERT INTO data_freshness (dataset, category, latest_data_date, total_records)
VALUES ('{row['DATASET']}', '{row['CATEGORY']}', '{latest_date}', {row['TOTAL_RECORDS']});
"""
    
    execute_d1_sql(sql)
    print(f"  ✓ Synced {len(rows)} datasets")


def execute_d1_sql(sql: str):
    """Execute SQL on D1 via wrangler."""
    # Write SQL to temp file
    sql_file = Path("/tmp/d1_sync.sql")
    sql_file.write_text(sql)
    
    # Execute via wrangler
    result = subprocess.run(
        ["npx", "wrangler", "d1", "execute", D1_DATABASE_NAME, 
         "--remote", "--file", str(sql_file)],
        cwd=Path(__file__).parent.parent / "dashboard",
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"  Error: {result.stderr}")
        raise RuntimeError(f"D1 sync failed: {result.stderr}")


def main():
    print("="*60)
    print(f"Snowflake → D1 Sync")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    print()
    
    try:
        sync_current_week()
        sync_ed_status()
        sync_viral_trends()
        sync_data_freshness()
        
        print()
        print("="*60)
        print("✓ Sync complete!")
        print("="*60)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

