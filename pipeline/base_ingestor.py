"""
Base ingestor class for Ontario Data Catalogue (CKAN) API.

This provides reusable functionality for:
- Fetching data from CKAN API with pagination
- Loading data into Snowflake RAW tables
- Tracking ingestion runs
"""
import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from config import (
    CKAN_BASE_URL,
    get_snowflake_connection,
    SNOWFLAKE_DATABASE,
    SCHEMA_RAW
)


class BaseIngestor(ABC):
    """Base class for CKAN data ingestion into Snowflake."""
    
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.run_id = str(uuid.uuid4())[:8]
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OntarioHealthPipeline/1.0"
        })
    
    @property
    @abstractmethod
    def target_table(self) -> str:
        """Target RAW table name (without schema)."""
        pass
    
    @property
    @abstractmethod
    def resource_id(self) -> str | None:
        """CKAN resource ID for this dataset."""
        pass
    
    @abstractmethod
    def transform_records(self, records: list[dict]) -> pd.DataFrame:
        """Transform raw API records to DataFrame matching target table schema."""
        pass
    
    def discover_resource_id(self, dataset_slug: str) -> str | None:
        """Discover the CSV/JSON resource ID for a dataset by its slug."""
        url = f"{CKAN_BASE_URL}/package_show"
        try:
            response = self.session.get(url, params={"id": dataset_slug}, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                resources = data["result"].get("resources", [])
                # Prefer CSV, then JSON
                for fmt in ["CSV", "JSON"]:
                    for resource in resources:
                        if resource.get("format", "").upper() == fmt:
                            print(f"Found {fmt} resource: {resource['id']}")
                            return resource["id"]
            
            print(f"No CSV/JSON resource found for dataset: {dataset_slug}")
            return None
            
        except Exception as e:
            print(f"Error discovering resource: {e}")
            return None
    
    def fetch_from_api(self, limit: int = 10000) -> list[dict]:
        """Fetch all records from CKAN datastore API with pagination."""
        if not self.resource_id:
            raise ValueError(f"No resource_id configured for {self.dataset_name}")
        
        url = f"{CKAN_BASE_URL}/datastore_search"
        all_records = []
        offset = 0
        
        print(f"Fetching data from {self.dataset_name}...")
        
        while True:
            params = {
                "resource_id": self.resource_id,
                "limit": limit,
                "offset": offset
            }
            
            try:
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if not data.get("success"):
                    raise ValueError(f"API returned error: {data.get('error', 'Unknown')}")
                
                records = data["result"].get("records", [])
                if not records:
                    break
                
                all_records.extend(records)
                print(f"  Fetched {len(all_records)} records...")
                
                # Check if more records exist
                total = data["result"].get("total", 0)
                if len(all_records) >= total:
                    break
                
                offset += limit
                
            except requests.RequestException as e:
                print(f"API request failed: {e}")
                raise
        
        print(f"Total records fetched: {len(all_records)}")
        return all_records
    
    def load_to_snowflake(self, df: pd.DataFrame) -> int:
        """Load DataFrame to Snowflake RAW table."""
        if df.empty:
            print("No data to load")
            return 0
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            # Use Snowflake's write_pandas for efficient loading
            from snowflake.connector.pandas_tools import write_pandas
            
            # Ensure we're in the right database/schema
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
            
            # Write data
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=self.target_table,
                database=SNOWFLAKE_DATABASE,
                schema=SCHEMA_RAW,
                auto_create_table=False,  # Table should exist from DDL
                overwrite=False  # Append mode
            )
            
            print(f"Loaded {nrows} rows to {SCHEMA_RAW}.{self.target_table}")
            return nrows
            
        except Exception as e:
            print(f"Error loading to Snowflake: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def log_ingestion(self, records_fetched: int, records_inserted: int, 
                      status: str, error_message: str | None = None,
                      api_url: str | None = None):
        """Log ingestion run to tracking table."""
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
            
            cursor.execute("""
                INSERT INTO INGESTION_LOG 
                (run_id, started_at, completed_at, dataset_name, 
                 records_fetched, records_inserted, status, error_message, api_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.run_id,
                self._start_time.isoformat() if hasattr(self, '_start_time') else None,
                datetime.utcnow().isoformat(),
                self.dataset_name,
                records_fetched,
                records_inserted,
                status,
                error_message,
                api_url
            ))
            
        except Exception as e:
            print(f"Warning: Could not log ingestion: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def run(self) -> dict[str, Any]:
        """Execute the full ingestion pipeline."""
        self._start_time = datetime.utcnow()
        result = {
            "dataset": self.dataset_name,
            "run_id": self.run_id,
            "status": "FAILED",
            "records_fetched": 0,
            "records_inserted": 0,
            "error": None
        }
        
        try:
            # Fetch data
            records = self.fetch_from_api()
            result["records_fetched"] = len(records)
            
            if not records:
                result["status"] = "SUCCESS"
                result["error"] = "No records returned from API"
                return result
            
            # Transform
            df = self.transform_records(records)
            
            # Load
            rows_inserted = self.load_to_snowflake(df)
            result["records_inserted"] = rows_inserted
            result["status"] = "SUCCESS"
            
            # Log success
            self.log_ingestion(
                records_fetched=len(records),
                records_inserted=rows_inserted,
                status="SUCCESS",
                api_url=f"{CKAN_BASE_URL}/datastore_search?resource_id={self.resource_id}"
            )
            
        except Exception as e:
            result["error"] = str(e)
            self.log_ingestion(
                records_fetched=result["records_fetched"],
                records_inserted=0,
                status="FAILED",
                error_message=str(e)
            )
            raise
        
        return result


class CKANDatasetExplorer:
    """Utility class to explore available CKAN datasets."""
    
    def __init__(self):
        self.session = requests.Session()
    
    def search_datasets(self, query: str, rows: int = 10) -> list[dict]:
        """Search for datasets matching a query."""
        url = f"{CKAN_BASE_URL}/package_search"
        params = {"q": query, "rows": rows}
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success"):
            results = []
            for pkg in data["result"]["results"]:
                results.append({
                    "name": pkg["name"],
                    "title": pkg.get("title", ""),
                    "resources": [
                        {"id": r["id"], "format": r.get("format", ""), "name": r.get("name", "")}
                        for r in pkg.get("resources", [])
                    ]
                })
            return results
        return []
    
    def get_resource_fields(self, resource_id: str) -> list[dict]:
        """Get field definitions for a resource."""
        url = f"{CKAN_BASE_URL}/datastore_search"
        params = {"resource_id": resource_id, "limit": 0}
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success"):
            return data["result"].get("fields", [])
        return []
    
    def preview_resource(self, resource_id: str, limit: int = 5) -> list[dict]:
        """Preview first few records from a resource."""
        url = f"{CKAN_BASE_URL}/datastore_search"
        params = {"resource_id": resource_id, "limit": limit}
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success"):
            return data["result"].get("records", [])
        return []


if __name__ == "__main__":
    # Example: Explore available school/outbreak datasets
    explorer = CKANDatasetExplorer()
    
    print("Searching for school-related datasets...")
    results = explorer.search_datasets("school cases outbreak")
    
    for r in results:
        print(f"\n{r['title']} ({r['name']})")
        for res in r['resources']:
            print(f"  - {res['format']}: {res['id']}")

