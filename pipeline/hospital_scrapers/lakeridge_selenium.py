"""Lakeridge Health scraper using Selenium."""
from .selenium_base import SeleniumHospitalScraper
from bs4 import BeautifulSoup
import re


class LakeridgeSeleniumScraper(SeleniumHospitalScraper):
    """Scraper for Lakeridge Health using Selenium (JS-rendered)."""
    
    def __init__(self):
        super().__init__("Lakeridge Health")
    
    @property
    def url(self) -> str:
        return "https://edwt.lh.ca/"
    
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        results = []
        
        hospitals = [
            ('Ajax Pickering Hospital', 'Ajax'),
            ('Oshawa Hospital', 'Oshawa'),
            ('Port Perry Hospital', 'Port Perry'),
            ('Whitby Hospital', 'Whitby')
        ]
        
        # After JavaScript loads, look for hospital names and wait times
        for hospital_name, city in hospitals:
            # Try to find hospital in text
            text = soup.get_text()
            
            # Look for hospital name
            if city.lower() in text.lower() or hospital_name.lower() in text.lower():
                # Extract time near the hospital mention
                pattern = rf'({city}|{hospital_name}).*?(\d+)\s*(hour|hr).*?(\d+)?\s*(minute|min)?'
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                
                if match:
                    hours = int(match.group(2))
                    minutes = int(match.group(4)) if match.group(4) else 0
                    
                    results.append({
                        'hospital_name': hospital_name,
                        'wait_hours': hours,
                        'wait_minutes': minutes,
                        'wait_total_minutes': hours * 60 + minutes,
                        'city': city
                    })
        
        return results

