"""London Health Sciences scraper - 2 EDs."""
from .base import BaseHospitalScraper
from bs4 import BeautifulSoup
import re


class LondonHealthScraper(BaseHospitalScraper):
    """Scraper for London Health Sciences Centre."""
    
    def __init__(self):
        super().__init__("London Health Sciences")
    
    @property
    def url(self) -> str:
        return "https://www.lhsc.on.ca/adult-ed/emergency-department-wait-times"
    
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        results = []
        
        hospitals = [
            ('Victoria Hospital', 'London'),
            ('University Hospital', 'London')
        ]
        
        for hospital_name, city in hospitals:
            # Find hospital and extract wait time
            search_terms = [hospital_name, hospital_name.replace(' Hospital', '')]
            
            for term in search_terms:
                elements = soup.find_all(text=re.compile(term, re.I))
                if elements:
                    for elem in elements:
                        context = elem.find_parent().get_text() if elem.find_parent() else ''
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
                    break
        
        return results

