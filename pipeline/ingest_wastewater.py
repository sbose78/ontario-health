"""
Ingestor for Health Canada Wastewater Surveillance Data.

Data source: Health Infobase Canada
URL: https://health-infobase.canada.ca/wastewater/
Data: https://health-infobase.canada.ca/src/data/wastewater/wastewater_aggregate.csv

Tracks respiratory virus levels (COVID-19, Influenza A/B, RSV) in wastewater.
Updated weekly with current 2025 data.
"""
import json
from datetime import datetime

import pandas as pd
import requests

from config import (
    get_snowflake_connection,
    SNOWFLAKE_DATABASE,
    SCHEMA_RAW
)


class WastewaterIngestor:
    """Ingest wastewater surveillance data from Health Canada."""
    
    DATA_URL = "https://health-infobase.canada.ca/src/data/wastewater/wastewater_aggregate.csv"
    TREND_URL = "https://health-infobase.canada.ca/src/data/wastewater/wastewater_trend.csv"
    
    # Virus code mapping
    VIRUS_NAMES = {
        "covN2": "COVID-19",
        "fluA": "Influenza A", 
        "fluB": "Influenza B",
        "rsv": "RSV"
    }
    
    def __init__(self, province_filter: str | None = "Ontario"):
        """
        Initialize ingestor.
        
        Args:
            province_filter: Filter to specific province (default: Ontario).
                            Set to None to ingest all provinces.
        """
        self.province_filter = province_filter
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def fetch_data(self) -> pd.DataFrame:
        """Fetch wastewater data from Health Canada."""
        print(f"Fetching wastewater data from Health Canada...")
        
        response = requests.get(self.DATA_URL, timeout=120)
        response.raise_for_status()
        
        df = pd.read_csv(pd.io.common.StringIO(response.text))
        print(f"  Total records: {len(df):,}")
        
        # Filter by province if specified
        if self.province_filter:
            df = df[df["province"] == self.province_filter]
            print(f"  Filtered to {self.province_filter}: {len(df):,} records")
        
        return df
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data for Snowflake loading."""
        transformed = []
        
        for _, row in df.iterrows():
            # Calculate week end date from EpiYear and EpiWeek
            try:
                # EpiWeek starts on Sunday, ends on Saturday
                year = int(row["EpiYear"])
                week = int(row["EpiWeek"])
                # Use weekstart if available, otherwise calculate
                week_start = row.get("weekstart")
                if pd.isna(week_start):
                    week_start = None
            except (ValueError, TypeError):
                week_start = None
            
            virus_code = row.get("measureid", "")
            virus_name = self.VIRUS_NAMES.get(virus_code, virus_code)
            
            transformed.append({
                "SOURCE_FILE": f"wastewater_{self.run_id}",
                "LOCATION": row.get("Location"),
                "SITE": row.get("site"),
                "CITY": row.get("city"),
                "PROVINCE": row.get("province"),
                "COUNTRY": row.get("country", "Canada"),
                "EPI_YEAR": int(row["EpiYear"]) if pd.notna(row.get("EpiYear")) else None,
                "EPI_WEEK": int(row["EpiWeek"]) if pd.notna(row.get("EpiWeek")) else None,
                "WEEK_START": week_start,
                "VIRUS_CODE": virus_code,
                "VIRUS_NAME": virus_name,
                "VIRAL_LOAD_AVG": float(row["w_avg"]) if pd.notna(row.get("w_avg")) else None,
                "VIRAL_LOAD_MIN": float(row["min"]) if pd.notna(row.get("min")) else None,
                "VIRAL_LOAD_MAX": float(row["max"]) if pd.notna(row.get("max")) else None,
                "POPULATION_COVERAGE": float(row["populationcoverage"]) if pd.notna(row.get("populationcoverage")) else None,
                "RAW_JSON": json.dumps(row.to_dict(), default=str)
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
                CREATE TABLE IF NOT EXISTS RAW.WASTEWATER_SURVEILLANCE (
                    id NUMBER AUTOINCREMENT PRIMARY KEY,
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    source_file VARCHAR(500),
                    location VARCHAR(200),
                    site VARCHAR(200),
                    city VARCHAR(200),
                    province VARCHAR(100),
                    country VARCHAR(100),
                    epi_year NUMBER,
                    epi_week NUMBER,
                    week_start DATE,
                    virus_code VARCHAR(50),
                    virus_name VARCHAR(100),
                    viral_load_avg FLOAT,
                    viral_load_min FLOAT,
                    viral_load_max FLOAT,
                    population_coverage FLOAT,
                    raw_json VARIANT
                )
            """)
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="WASTEWATER_SURVEILLANCE",
                database=SNOWFLAKE_DATABASE,
                schema=SCHEMA_RAW,
                auto_create_table=False,
                overwrite=False
            )
            
            print(f"Loaded {nrows:,} rows to RAW.WASTEWATER_SURVEILLANCE")
            return nrows
            
        finally:
            cursor.close()
            conn.close()
    
    def run(self) -> dict:
        """Execute full ingestion pipeline."""
        result = {
            "dataset": "wastewater_surveillance",
            "run_id": self.run_id,
            "status": "FAILED",
            "records_fetched": 0,
            "records_inserted": 0,
            "error": None
        }
        
        try:
            # Fetch
            df = self.fetch_data()
            result["records_fetched"] = len(df)
            
            if df.empty:
                result["status"] = "SUCCESS"
                result["error"] = "No records returned"
                return result
            
            # Transform
            transformed_df = self.transform(df)
            
            # Load
            rows_inserted = self.load_to_snowflake(transformed_df)
            result["records_inserted"] = rows_inserted
            result["status"] = "SUCCESS"
            
        except Exception as e:
            result["error"] = str(e)
            raise
        
        return result


def main():
    """Run wastewater ingestion."""
    print("\n" + "="*60)
    print("Wastewater Surveillance Ingestion (Ontario)")
    print("="*60)
    
    ingestor = WastewaterIngestor(province_filter="Ontario")
    
    try:
        result = ingestor.run()
        print(f"\nIngestion complete:")
        print(f"  Status: {result['status']}")
        print(f"  Records fetched: {result['records_fetched']:,}")
        print(f"  Records inserted: {result['records_inserted']:,}")
        if result.get("error"):
            print(f"  Note: {result['error']}")
    except Exception as e:
        print(f"Ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main()

