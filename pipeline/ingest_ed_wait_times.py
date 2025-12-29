"""
Ingestor for Halton Healthcare Emergency Department Wait Times.

Data source: Halton Healthcare Website
URL: https://www.haltonhealthcare.on.ca/patients-visitors/emergency-care

Scrapes current ED wait times for Georgetown, Milton, and Oakville hospitals.
Updated approximately every 30 minutes on the source website.

NOTE: This is web scraping - may break if website structure changes.
"""
import json
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import (
    get_snowflake_connection,
    SNOWFLAKE_DATABASE,
    SCHEMA_RAW
)


class EDWaitTimesIngestor:
    """Scrape ED wait times from Halton Healthcare website."""
    
    URL = "https://www.haltonhealthcare.on.ca/emergency-department"
    
    # Hospital name mapping
    HOSPITALS = {
        "georgetown": "Georgetown Hospital",
        "milton": "Milton District Hospital", 
        "oakville": "Oakville Trafalgar Memorial Hospital"
    }
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OntarioHealthPipeline/1.0"
        })
    
    def fetch_and_parse(self) -> list[dict]:
        """Fetch page and parse ED wait times."""
        print(f"Fetching ED wait times from Halton Healthcare...")
        
        response = self.session.get(self.URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the last updated timestamp
        last_updated = None
        update_match = re.search(r'Last Updated:\s*([^<]+)', response.text)
        if update_match:
            last_updated = update_match.group(1).strip()
            print(f"  Source last updated: {last_updated}")
        
        # Parse wait times from the page
        # The format is: "georgetown01 Hour(s) and 48 Minute(s)"
        records = []
        
        # Find all text that contains wait times
        text = soup.get_text()
        
        for hospital_key, hospital_name in self.HOSPITALS.items():
            # Pattern: hospitalname + hours + minutes
            pattern = rf'{hospital_key}(\d+)\s*Hour\(s\)\s*and\s*(\d+)\s*Minute\(s\)'
            match = re.search(pattern, text, re.IGNORECASE)
            
            if match:
                hours = int(match.group(1))
                minutes = int(match.group(2))
                total_minutes = hours * 60 + minutes
                
                records.append({
                    "hospital_code": hospital_key,
                    "hospital_name": hospital_name,
                    "wait_hours": hours,
                    "wait_minutes": minutes,
                    "wait_total_minutes": total_minutes,
                    "source_updated": last_updated
                })
                
                print(f"  {hospital_name}: {hours}h {minutes}m")
        
        if not records:
            print("  WARNING: No wait times found - page structure may have changed")
        
        return records
    
    def transform(self, records: list[dict]) -> pd.DataFrame:
        """Transform scraped data for Snowflake loading."""
        now = datetime.now()
        
        transformed = []
        for rec in records:
            transformed.append({
                "SOURCE_FILE": f"halton_ed_{self.run_id}",
                "SCRAPED_AT": now.strftime("%Y-%m-%d %H:%M:%S"),
                "SOURCE_UPDATED": rec["source_updated"],
                "HOSPITAL_CODE": rec["hospital_code"],
                "HOSPITAL_NAME": rec["hospital_name"],
                "REGION": "Halton",
                "WAIT_HOURS": rec["wait_hours"],
                "WAIT_MINUTES": rec["wait_minutes"],
                "WAIT_TOTAL_MINUTES": rec["wait_total_minutes"],
                "RAW_JSON": json.dumps(rec, default=str)
            })
        
        return pd.DataFrame(transformed)
    
    def load_to_snowflake(self, df: pd.DataFrame) -> int:
        """Load DataFrame to Snowflake."""
        if df.empty:
            print("No data to load")
            return 0
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            from snowflake.connector.pandas_tools import write_pandas
            
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
            
            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS RAW.ED_WAIT_TIMES (
                    id NUMBER AUTOINCREMENT PRIMARY KEY,
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    source_file VARCHAR(500),
                    scraped_at TIMESTAMP_NTZ,
                    source_updated VARCHAR(100),
                    hospital_code VARCHAR(50),
                    hospital_name VARCHAR(200),
                    region VARCHAR(100),
                    wait_hours NUMBER,
                    wait_minutes NUMBER,
                    wait_total_minutes NUMBER,
                    raw_json VARIANT
                )
            """)
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="ED_WAIT_TIMES",
                database=SNOWFLAKE_DATABASE,
                schema=SCHEMA_RAW,
                auto_create_table=False,
                overwrite=False
            )
            
            print(f"Loaded {nrows} rows to RAW.ED_WAIT_TIMES")
            return nrows
            
        finally:
            cursor.close()
            conn.close()
    
    def run(self) -> dict:
        """Execute full ingestion pipeline."""
        result = {
            "dataset": "ed_wait_times",
            "run_id": self.run_id,
            "status": "FAILED",
            "records_fetched": 0,
            "records_inserted": 0,
            "error": None
        }
        
        try:
            # Fetch and parse
            records = self.fetch_and_parse()
            result["records_fetched"] = len(records)
            
            if not records:
                result["status"] = "SUCCESS"
                result["error"] = "No records found - check page structure"
                return result
            
            # Transform
            df = self.transform(records)
            
            # Load
            rows_inserted = self.load_to_snowflake(df)
            result["records_inserted"] = rows_inserted
            result["status"] = "SUCCESS"
            
        except Exception as e:
            result["error"] = str(e)
            raise
        
        return result


def main():
    """Run ED wait times ingestion."""
    print("\n" + "="*60)
    print("ED Wait Times Ingestion (Halton Healthcare)")
    print("="*60)
    
    ingestor = EDWaitTimesIngestor()
    
    try:
        result = ingestor.run()
        print(f"\nIngestion complete:")
        print(f"  Status: {result['status']}")
        print(f"  Records fetched: {result['records_fetched']}")
        print(f"  Records inserted: {result['records_inserted']}")
        if result.get("error"):
            print(f"  Note: {result['error']}")
    except Exception as e:
        print(f"Ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main()

