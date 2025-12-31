"""Lakeridge Health scraper - 4 hospitals."""
from .base import BaseHospitalScraper
from bs4 import BeautifulSoup
import re


class LakeridgeHealthScraper(BaseHospitalScraper):
    """Scraper for Lakeridge Health network."""
    
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
            ('Whitby Hospital', 'Whitby'),
            ('Bowmanville Hospital', 'Bowmanville')
        ]
        
        for hospital_name, city in hospitals:
            # Look for hospital name
            search_name = hospital_name.replace(' Hospital', '')
            elements = soup.find_all(text=re.compile(search_name, re.I))
            
            if elements:
                for elem in elements:
                    parent = elem.find_parent()
                    if parent:
                        # Get all text in parent and siblings
                        context = parent.get_text()
                        # Also check next few siblings
                        for sib in list(parent.find_next_siblings())[:3]:
                            context += ' ' + sib.get_text()
                        
                        hours, minutes = self.extract_time(context)
                        
                        if hours > 0 or minutes > 0:
                            results.append({
                                'hospital_name': hospital_name,
                                'wait_hours': hours,
                                'wait_minutes': minutes,
                                'wait_total_minutes': hours * 60 + minutes,
                                'city': city
                            })
                            break
        
        return results

