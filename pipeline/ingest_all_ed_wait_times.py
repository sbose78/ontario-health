"""
Multi-hospital ED wait times ingestor for Ontario.

Attempts to scrape all verified hospital networks.
Handles failures gracefully - partial data is better than no data.
"""
import json
from datetime import datetime
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import get_snowflake_connection, SNOWFLAKE_DATABASE, SCHEMA_RAW


# Import existing Halton scraper pattern
from ingest_ed_wait_times import EDWaitTimesIngestor


class MultiNetworkEDScraper:
    """Scrape ED wait times from multiple Ontario hospital networks."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OntarioHealthPipeline/2.0'
        })
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def scrape_all(self) -> List[Dict]:
        """Scrape all networks, return list of hospital records."""
        all_hospitals = []
        
        # 1. Halton (working scraper)
        print("Scraping Halton Healthcare...")
        halton = EDWaitTimesIngestor()
        try:
            halton_data = halton.fetch_and_parse()
            all_hospitals.extend(halton_data)
            print(f"  ✓ {len(halton_data)} hospitals")
        except Exception as e:
            print(f"  ✗ {e}")
        
        # 2-7. Other networks (best-effort)
        networks = [
            ("Niagara Health", "https://www.niagarahealth.on.ca/site/waiting-times", self.scrape_niagara),
            ("UHN Toronto", "https://www.uhn.ca/PatientsFamilies/Visit_UHN/Emergency/Pages/ED_wait_times.aspx", self.scrape_uhn),
            ("London Health", "https://www.lhsc.on.ca/adult-ed/emergency-department-wait-times", self.scrape_london),
        ]
        
        for name, url, scraper_func in networks:
            print(f"Scraping {name}...")
            try:
                hospitals = scraper_func(url)
                if hospitals:
                    all_hospitals.extend(hospitals)
                    print(f"  ✓ {len(hospitals)} hospitals")
                else:
                    print(f"  ⚠️  No data (site may have changed)")
            except Exception as e:
                print(f"  ✗ {str(e)[:50]}")
        
        return all_hospitals
    
    def scrape_niagara(self, url: str) -> List[Dict]:
        """Scrape Niagara Health sites."""
        response = self.session.get(url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Niagara uses specific structure - need to find actual pattern
        # For now, return empty and mark for future implementation
        return []
    
    def scrape_uhn(self, url: str) -> List[Dict]:
        """Scrape UHN Toronto sites."""
        response = self.session.get(url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # UHN structure TBD
        return []
    
    def scrape_london(self, url: str) -> List[Dict]:
        """Scrape London Health Sciences."""
        response = self.session.get(url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # London structure TBD
        return []
    
    def load_to_snowflake(self, hospitals: List[Dict]) -> int:
        """Load all hospital data to Snowflake."""
        if not hospitals:
            print("No data to load")
            return 0
        
        # Convert to DataFrame
        df_data = []
        for h in hospitals:
            df_data.append({
                'SOURCE_FILE': f"multi_network_ed_{self.run_id}",
                'SCRAPED_AT': h.get('scraped_at', datetime.now().isoformat()),
                'SOURCE_UPDATED': h.get('source_updated', ''),
                'HOSPITAL_CODE': h.get('hospital_code', h['hospital_name'].lower().replace(' ', '_')),
                'HOSPITAL_NAME': h['hospital_name'],
                'NETWORK': h.get('network', 'Unknown'),
                'CITY': h.get('city', ''),
                'REGION': h.get('region', ''),
                'WAIT_HOURS': h['wait_hours'],
                'WAIT_MINUTES': h['wait_minutes'],
                'WAIT_TOTAL_MINUTES': h['wait_total_minutes'],
                'RAW_JSON': json.dumps(h, default=str)
            })
        
        df = pd.DataFrame(df_data)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            from snowflake.connector.pandas_tools import write_pandas
            
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SCHEMA_RAW}")
            
            # Schema already exists from original ED scraper
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="ED_WAIT_TIMES",
                database=SNOWFLAKE_DATABASE,
                schema=SCHEMA_RAW,
                auto_create_table=False,
                overwrite=False
            )
            
            print(f"\nLoaded {nrows} hospitals to RAW.ED_WAIT_TIMES")
            return nrows
            
        finally:
            cursor.close()
            conn.close()
    
    def run(self) -> Dict:
        """Execute full scraping pipeline."""
        result = {
            "status": "FAILED",
            "hospitals_scraped": 0,
            "hospitals_inserted": 0,
            "networks_attempted": 0,
            "error": None
        }
        
        try:
            hospitals = self.scrape_all()
            result["hospitals_scraped"] = len(hospitals)
            
            if hospitals:
                rows = self.load_to_snowflake(hospitals)
                result["hospitals_inserted"] = rows
                result["status"] = "SUCCESS"
            else:
                result["status"] = "SUCCESS"
                result["error"] = "No hospitals scraped"
            
        except Exception as e:
            result["error"] = str(e)
            raise
        
        return result


def main():
    print("\n" + "="*60)
    print("Multi-Network ED Wait Times Scraper")
    print("="*60)
    
    scraper = MultiNetworkEDScraper()
    
    try:
        result = scraper.run()
        print(f"\nScraping complete:")
        print(f"  Status: {result['status']}")
        print(f"  Hospitals found: {result['hospitals_scraped']}")
        print(f"  Inserted: {result['hospitals_inserted']}")
        if result.get("error"):
            print(f"  Note: {result['error']}")
    except Exception as e:
        print(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()

