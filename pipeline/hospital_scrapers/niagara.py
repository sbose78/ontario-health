"""Niagara Health scraper - 4 hospitals."""
from .base import BaseHospitalScraper
from bs4 import BeautifulSoup
import re


class NiagaraHealthScraper(BaseHospitalScraper):
    """Scraper for Niagara Health network."""
    
    def __init__(self):
        super().__init__("Niagara Health")
        self.hospitals = [
            "Greater Niagara General",
            "St. Catharines Site", 
            "Welland Hospital",
            "Fort Erie Site",
            "Niagara Falls Site"
        ]
    
    @property
    def url(self) -> str:
        return "https://www.niagarahealth.on.ca/site/waiting-times"
    
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        results = []
        
        # Find all sections with wait time data
        # Pattern: look for hospital names and nearby time elements
        text = soup.get_text()
        
        for hospital in self.hospitals:
            # Find hospital name in text
            if hospital.lower() in text.lower():
                # Look for time pattern near the hospital name
                # This is a simplified parser - may need adjustment based on actual HTML
                pattern = rf'{re.escape(hospital)}.*?(\d+)\s*(?:hour|hr).*?(\d+)\s*(?:minute|min)'
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                
                if match:
                    hours, minutes = int(match.group(1)), int(match.group(2))
                else:
                    # Try just minutes
                    pattern = rf'{re.escape(hospital)}.*?(\d+)\s*(?:minute|min)'
                    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                    if match:
                        hours, minutes = 0, int(match.group(1))
                    else:
                        continue
                
                results.append({
                    'hospital_name': hospital,
                    'wait_hours': hours,
                    'wait_minutes': minutes,
                    'wait_total_minutes': hours * 60 + minutes,
                    'city': self._get_city(hospital)
                })
        
        return results
    
    def _get_city(self, hospital_name: str) -> str:
        if 'St. Catharines' in hospital_name or 'General' in hospital_name:
            return 'St. Catharines'
        elif 'Welland' in hospital_name:
            return 'Welland'
        elif 'Fort Erie' in hospital_name:
            return 'Fort Erie'
        elif 'Niagara Falls' in hospital_name:
            return 'Niagara Falls'
        return 'Niagara Region'

