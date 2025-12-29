#!/usr/bin/env python3
"""Show current surveillance status."""
from config import get_snowflake_connection

conn = get_snowflake_connection()
cur = conn.cursor()

print('\n--- Respiratory (Week 51, 2025) ---')
cur.execute('SELECT virus_name, avg_viral_load, sites_reporting FROM MARTS_SURVEILLANCE.rpt_current_week ORDER BY avg_viral_load DESC')
for row in cur.fetchall():
    print(f'{row[0]:15} | Load: {row[1]:6.2f} | {row[2]} sites')

print('\n--- ED Wait Times (Current) ---')
cur.execute('SELECT hospital_name, wait_total_minutes, wait_severity FROM MARTS_SURVEILLANCE.rpt_ed_current ORDER BY wait_total_minutes DESC')
for row in cur.fetchall():
    print(f'{row[0]:35} | {row[1]:3}min | {row[2]}')

cur.close()
conn.close()

