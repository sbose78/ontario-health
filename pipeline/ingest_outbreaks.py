"""
Ingestor for Ontario Outbreaks dataset.

Data source: Ontario Data Catalogue
Dataset: Ontario COVID-19 outbreaks data (includes all institution types)
URL: https://data.ontario.ca/dataset/ontario-covid-19-outbreaks-data

This ingestor fetches all outbreaks and filters to school/daycare during transform.
"""
import json
from datetime import datetime, date

import pandas as pd

from base_ingestor import BaseIngestor
from config import HALTON_PHU_CODES, HALTON_PHU_NAMES


class OutbreaksIngestor(BaseIngestor):
    """Ingest outbreak data focusing on schools and daycares."""
    
    # Institution types we care about (case-insensitive matching)
    SCHOOL_DAYCARE_TYPES = [
        "school",
        "elementary",
        "secondary",
        "daycare",
        "child care",
        "childcare",
        "before and after school program",
        "licensed child care",
        "education",  # Ontario outbreak data uses "3 Education" category
        "3 education"
    ]
    
    def __init__(self, filter_to_schools: bool = True):
        super().__init__("outbreaks")
        self._resource_id = None
        self.filter_to_schools = filter_to_schools
    
    @property
    def target_table(self) -> str:
        return "OUTBREAKS"
    
    @property
    def resource_id(self) -> str | None:
        if self._resource_id is None:
            # Try known resource ID first, then discover
            # This is the "ongoing outbreaks" resource
            self._resource_id = self.discover_resource_id("ontario-covid-19-outbreaks-data")
        return self._resource_id
    
    def _is_school_or_daycare(self, institution_type: str | None) -> bool:
        """Check if institution type is school/daycare related."""
        if not institution_type:
            return False
        
        inst_lower = institution_type.lower()
        return any(t in inst_lower for t in self.SCHOOL_DAYCARE_TYPES)
    
    def _is_halton(self, phu_id: str | None, phu_name: str | None) -> bool:
        """Check if this outbreak is in Halton region."""
        if phu_id and phu_id in HALTON_PHU_CODES:
            return True
        if phu_name:
            return any(h.lower() in phu_name.lower() for h in HALTON_PHU_NAMES)
        return False
    
    def transform_records(self, records: list[dict]) -> pd.DataFrame:
        """Transform outbreak records to DataFrame."""
        transformed = []
        
        for record in records:
            institution_type = record.get("outbreak_group") or record.get("institution_type")
            
            # Filter to schools/daycares if enabled
            if self.filter_to_schools and not self._is_school_or_daycare(institution_type):
                continue
            
            date_began = self._parse_date(record.get("date_outbreak_began"))
            date_over = self._parse_date(record.get("date_outbreak_declared_over"))
            
            # Convert to date strings for Snowflake DATE columns
            date_began_str = date_began.strftime("%Y-%m-%d") if date_began else None
            date_over_str = date_over.strftime("%Y-%m-%d") if date_over else None
            
            phu_id = str(record.get("phu_num", "")) or record.get("phu_id")
            phu_name = record.get("phu_name") or record.get("reporting_phu")
            
            # Sanitize record for JSON
            sanitized_record = {}
            for k, v in record.items():
                if isinstance(v, (datetime,)):
                    sanitized_record[k] = v.isoformat()
                else:
                    sanitized_record[k] = v
            
            transformed.append({
                "SOURCE_FILE": f"ckan_outbreaks_{datetime.now().strftime('%Y%m%d')}",
                "OUTBREAK_ID": str(record.get("outbreak_id") or record.get("_id", "")),
                "DATE_OUTBREAK_BEGAN": date_began_str,
                "DATE_OUTBREAK_DECLARED_OVER": date_over_str,
                "OUTBREAK_STATUS": "Resolved" if date_over else "Active",
                "INSTITUTION_NAME": record.get("outbreak_setting") or record.get("institution_name"),
                "INSTITUTION_ADDRESS": record.get("institution_address"),
                "INSTITUTION_CITY": record.get("institution_city"),
                "INSTITUTION_TYPE": institution_type,
                "OUTBREAK_TYPE": record.get("outbreak_type") or record.get("causative_agent"),
                "RESIDENT_CASES": self._safe_int(record.get("resident_cases")),
                "STAFF_CASES": self._safe_int(record.get("staff_cases")),
                "TOTAL_CASES": self._safe_int(record.get("cases_total")) or 
                              (self._safe_int(record.get("resident_cases")) + 
                               self._safe_int(record.get("staff_cases"))),
                "PHU_ID": phu_id,
                "PHU_NAME": phu_name,
                "RAW_JSON": json.dumps(sanitized_record, default=str)
            })
        
        df = pd.DataFrame(transformed)
        print(f"Filtered to {len(df)} school/daycare outbreaks")
        return df
    
    def _parse_date(self, date_val) -> datetime | None:
        """Parse various date formats from the API, including Unix timestamps."""
        if date_val is None or date_val == "":
            return None
        
        # Handle Unix timestamp in nanoseconds (common in CKAN)
        if isinstance(date_val, (int, float)):
            try:
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
    """Run outbreaks ingestion."""
    ingestor = OutbreaksIngestor(filter_to_schools=True)
    
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

