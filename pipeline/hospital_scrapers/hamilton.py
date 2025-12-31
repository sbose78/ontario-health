"""Hamilton Health Hub scraper - JSON API."""
from .base import BaseHospitalScraper
import json


class HamiltonHealthScraper(BaseHospitalScraper):
    """Scraper for Hamilton Emergency Wait Times (JSON backend)."""
    
    def __init__(self):
        super().__init__("Hamilton Health Hub")
    
    @property
    def url(self) -> str:
        # Try to find the JSON API endpoint
        return "https://www.hamiltonemergencywaittimes.ca/"
    
    def parse(self, response):
        results = []
        
        # Try to find JSON data in the page
        # Look for embedded JSON or API endpoint
        text = response.text
        
        # Pattern 1: Embedded JSON
        if 'application/json' in text or '{' in text:
            # Try to extract JSON
            import re
            json_matches = re.findall(r'\{[^{}]*"hospital"[^{}]*\}', text)
            
            for match in json_matches:
                try:
                    data = json.loads(match)
                    if 'wait' in str(data).lower():
                        hours, minutes = self._parse_wait_time(str(data))
                        results.append({
                            'hospital_name': data.get('hospital', 'Hamilton Hospital'),
                            'wait_hours': hours,
                            'wait_minutes': minutes,
                            'wait_total_minutes': hours * 60 + minutes,
                            'city': 'Hamilton'
                        })
                except:
                    continue
        
        # Pattern 2: HTML parsing
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(text, 'html.parser')
        
        hospitals = ['Hamilton General', 'Juravinski', 'McMaster', "St. Joseph's"]
        for hospital in hospitals:
            # Find hospital name in page
            for elem in soup.find_all(text=re.compile(hospital, re.I)):
                parent = elem.find_parent()
                if parent:
                    parent_text = parent.get_text()
                    hours, minutes = self.extract_time(parent_text)
                    if hours > 0 or minutes > 0:
                        results.append({
                            'hospital_name': f'{hospital} Hospital',
                            'wait_hours': hours,
                            'wait_minutes': minutes,
                            'wait_total_minutes': hours * 60 + minutes,
                            'city': 'Hamilton'
                        })
                        break
        
        return results
    
    def _parse_wait_time(self, text: str) -> tuple[int, int]:
        """Parse wait time from text."""
        import re
        match = re.search(r'(\d+)\s*h.*?(\d+)\s*m', text.lower())
        if match:
            return int(match.group(1)), int(match.group(2))
        return 0, 0

