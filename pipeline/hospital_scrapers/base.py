"""Base class for hospital ED wait time scrapers."""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict
import requests
from bs4 import BeautifulSoup


class BaseHospitalScraper(ABC):
    """Base scraper for hospital ED wait times."""
    
    def __init__(self, network_name: str):
        self.network_name = network_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OntarioHealthPipeline/1.0'
        })
    
    @property
    @abstractmethod
    def url(self) -> str:
        """URL to scrape."""
        pass
    
    @abstractmethod
    def parse(self, response: requests.Response) -> List[Dict]:
        """
        Parse response and return list of hospital data.
        
        Each dict should have:
        - hospital_name: str
        - wait_hours: int
        - wait_minutes: int
        - wait_total_minutes: int
        - network: str
        - city: str (optional)
        """
        pass
    
    def fetch_and_parse(self) -> List[Dict]:
        """Fetch URL and parse data."""
        try:
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
            
            hospitals = self.parse(response)
            
            # Add metadata
            for h in hospitals:
                h['network'] = self.network_name
                h['scraped_at'] = datetime.now().isoformat()
            
            return hospitals
            
        except Exception as e:
            print(f"  Error scraping {self.network_name}: {e}")
            return []
    
    def extract_time(self, text: str) -> tuple[int, int]:
        """
        Extract hours and minutes from text like '2 hours 30 minutes' or '2h 30m'.
        Returns: (hours, minutes)
        """
        import re
        
        # Try various patterns
        patterns = [
            r'(\d+)\s*(?:hour|hr|h)(?:s)?\s+(\d+)\s*(?:minute|min|m)',
            r'(\d+)\s*(?:hour|hr|h)(?:s)?',
            r'(\d+)\s*(?:minute|min|m)(?:s)?'
        ]
        
        text_lower = text.lower()
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    return int(groups[0]), int(groups[1])
                elif 'hour' in pattern or 'hr' in pattern or pattern.endswith('h'):
                    return int(groups[0]), 0
                else:
                    return 0, int(groups[0])
        
        return 0, 0

