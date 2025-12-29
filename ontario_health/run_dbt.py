#!/usr/bin/env python3
"""
Helper script to run dbt with Snowflake PAT authentication.

Usage:
    python run_dbt.py compile
    python run_dbt.py test
    python run_dbt.py docs generate
"""
import os
import sys
import subprocess
from pathlib import Path


def main():
    # Load token from file
    token_file = Path.home() / ".snowflake" / "ontario_health_token"
    if not token_file.exists():
        print(f"Error: Token file not found at {token_file}")
        return 1
    
    token = token_file.read_text().strip()
    
    # Set environment variable
    env = os.environ.copy()
    env['SNOWFLAKE_TOKEN'] = token
    
    # Run dbt with remaining args
    dbt_args = sys.argv[1:] if len(sys.argv) > 1 else ['debug']
    cmd = ['dbt'] + dbt_args + ['--profiles-dir', '.']
    
    print(f"Running: {' '.join(cmd)}")
    print(f"Token loaded: {token[:20]}...\n")
    
    result = subprocess.run(cmd, env=env)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())

