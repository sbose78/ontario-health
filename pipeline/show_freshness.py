#!/usr/bin/env python3
"""Show data freshness across all datasets."""
from config import get_snowflake_connection

conn = get_snowflake_connection()
cur = conn.cursor()

cur.execute('SELECT * FROM MARTS_OPS.rpt_data_freshness ORDER BY category, dataset')

print('\nData Freshness:')
print('Dataset                    | Latest Data | Ingestion    | Records')
print('-' * 75)

for row in cur.fetchall():
    print(f'{row[0]:26} | {str(row[2])[:10]:11} | {str(row[3])[:12]:12} | {row[4]:,}')

cur.close()
conn.close()

