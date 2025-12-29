"""
Ingestor for Ontario School Cases dataset.

Data source: Ontario Data Catalogue
Dataset: Summary of Cases in Schools
URL: https://data.ontario.ca/dataset/summary-of-cases-in-schools
"""
import json
from datetime import datetime, date

import pandas as pd

from base_ingestor import BaseIngestor


class SchoolCasesIngestor(BaseIngestor):
    """Ingest school infection case data from Ontario Data Catalogue."""
    
    def __init__(self):
        super().__init__("school_cases")
        # Resource ID for the school cases dataset
        # This may need to be updated if the dataset structure changes
        self._resource_id = None
    
    def check_already_loaded(self) -> bool:
        """Check if school cases data already exists in Snowflake."""
        try:
            from config import get_snowflake_connection, SNOWFLAKE_DATABASE, SCHEMA_RAW
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
            cursor.execute("SELECT COUNT(*) FROM SCHOOL_CASES")
            
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            return count > 0
            
        except Exception:
            return False
    
    @property
    def target_table(self) -> str:
        return "SCHOOL_CASES"
    
    @property
    def resource_id(self) -> str | None:
        if self._resource_id is None:
            # Try to discover the resource ID
            self._resource_id = self.discover_resource_id("summary-of-cases-in-schools")
        return self._resource_id
    
    def transform_records(self, records: list[dict]) -> pd.DataFrame:
        """Transform raw school case records to DataFrame."""
        transformed = []
        
        for record in records:
            # Parse dates safely
            reported_date = self._parse_date(record.get("reported_date"))
            collected_date = self._parse_date(record.get("collected_date"))
            
            # Convert datetime to date for Snowflake DATE columns
            reported_date_str = reported_date.strftime("%Y-%m-%d") if reported_date else None
            collected_date_str = collected_date.strftime("%Y-%m-%d") if collected_date else None
            
            # Sanitize record for JSON - convert any non-serializable types
            sanitized_record = {}
            for k, v in record.items():
                if isinstance(v, (datetime,)):
                    sanitized_record[k] = v.isoformat()
                else:
                    sanitized_record[k] = v
            
            transformed.append({
                "SOURCE_FILE": f"ckan_school_cases_{datetime.now().strftime('%Y%m%d')}",
                "REPORTED_DATE": reported_date_str,
                "COLLECTED_DATE": collected_date_str,
                "SCHOOL_BOARD": record.get("school_board"),
                "SCHOOL_NAME": record.get("school"),
                "SCHOOL_ID": str(record.get("school_id", "")),
                "MUNICIPALITY": record.get("municipality"),
                "SCHOOL_TYPE": record.get("school_type"),
                "CONFIRMED_CASES": self._safe_int(record.get("confirmed_student_cases", 0)) + 
                                   self._safe_int(record.get("confirmed_staff_cases", 0)),
                "CUMULATIVE_CASES": self._safe_int(record.get("total_confirmed_cases")),
                "RAW_JSON": json.dumps(sanitized_record, default=str)
            })
        
        return pd.DataFrame(transformed)
    
    def _parse_date(self, date_val) -> datetime | None:
        """Parse various date formats from the API, including Unix timestamps."""
        if date_val is None or date_val == "":
            return None
        
        # Handle Unix timestamp in nanoseconds (common in CKAN)
        if isinstance(date_val, (int, float)):
            try:
                # If it's in nanoseconds, convert to seconds
                if date_val > 1e15:
                    date_val = date_val / 1e9
                elif date_val > 1e12:
                    date_val = date_val / 1e3
                return datetime.fromtimestamp(date_val)
            except (ValueError, OSError):
                return None
        
        date_str = str(date_val)
        
        # Try parsing as Unix timestamp string
        try:
            ts = float(date_str)
            if ts > 1e15:
                ts = ts / 1e9
            elif ts > 1e12:
                ts = ts / 1e3
            return datetime.fromtimestamp(ts)
        except (ValueError, TypeError):
            pass
        
        # Try various date string formats
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%d/%m/%Y",
            "%m/%d/%Y"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:10], fmt[:len(date_str[:10])])
            except (ValueError, TypeError):
                continue
        
        return None
    
    def _safe_int(self, value) -> int:
        """Safely convert value to int."""
        if value is None or value == "":
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0


def main():
    """Run school cases ingestion."""
    ingestor = SchoolCasesIngestor()
    
    # Skip if already loaded (static historical data)
    if ingestor.check_already_loaded():
        print(f"\n✓ School cases already loaded - skipping")
        print(f"  (This is historical 2021 data, no updates expected)")
        return
    
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

