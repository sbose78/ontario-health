#!/usr/bin/env python3
"""
Test Snowflake connection and optionally run database setup.

Usage:
    python test_snowflake.py              # Test connection only
    python test_snowflake.py --setup      # Run SQL setup scripts
"""
import argparse
import sys
from pathlib import Path


def test_connection():
    """Test Snowflake connection using PAT authentication."""
    from config import get_snowflake_connection, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER
    
    print(f"Testing connection to Snowflake...")
    print(f"  Account: {SNOWFLAKE_ACCOUNT}")
    print(f"  User: {SNOWFLAKE_USER}")
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
        result = cursor.fetchone()
        
        print(f"\n✓ Connection successful!")
        print(f"  User: {result[0]}")
        print(f"  Role: {result[1]}")
        print(f"  Warehouse: {result[2]}")
        print(f"  Database: {result[3]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        
        if "Network policy is required" in str(e):
            print("\n  Your Snowflake account has network policy restrictions.")
            print("  Add your current IP to: Admin → Security → Network Policies")
        
        return False


def run_setup_scripts():
    """Run SQL migration scripts to create database objects."""
    from config import get_snowflake_connection
    
    sql_dir = Path(__file__).parent.parent / "sql" / "migrations"
    scripts = sorted(sql_dir.glob("*.sql"))
    
    if not scripts:
        print(f"No SQL scripts found in {sql_dir}")
        return False
    
    print(f"\nFound {len(scripts)} SQL scripts to run:")
    for s in scripts:
        print(f"  - {s.name}")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        for script_path in scripts:
            print(f"\nRunning {script_path.name}...")
            
            sql_content = script_path.read_text()
            
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in sql_content.split(";") if s.strip()]
            
            for i, stmt in enumerate(statements):
                # Skip comments-only statements
                if not any(line.strip() and not line.strip().startswith("--") 
                          for line in stmt.split("\n")):
                    continue
                
                try:
                    cursor.execute(stmt)
                    # Fetch results if SELECT
                    if stmt.strip().upper().startswith("SELECT") or stmt.strip().upper().startswith("SHOW"):
                        results = cursor.fetchall()
                        for row in results[:5]:  # Show first 5 rows
                            print(f"    {row}")
                except Exception as e:
                    print(f"  Warning: Statement {i+1} failed: {e}")
            
            print(f"  ✓ {script_path.name} completed")
        
        print("\n✓ All setup scripts completed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Test Snowflake connection")
    parser.add_argument("--setup", action="store_true", help="Run SQL setup scripts")
    args = parser.parse_args()
    
    # Always test connection first
    if not test_connection():
        return 1
    
    if args.setup:
        if not run_setup_scripts():
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

