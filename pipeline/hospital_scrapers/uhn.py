"""UHN Toronto scraper - 2 hospitals."""
from .base import BaseHospitalScraper
from bs4 import BeautifulSoup
import re


class UHNScraper(BaseHospitalScraper):
    """Scraper for University Health Network Toronto."""
    
    def __init__(self):
        super().__init__("UHN Toronto")
    
    @property
    def url(self) -> str:
        return "https://www.uhn.ca/PatientsFamilies/Visit_UHN/Emergency/Pages/ED_wait_times.aspx"
    
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        results = []
        
        hospitals = [
            ('Toronto General Hospital', 'Toronto'),
            ('Toronto Western Hospital', 'Toronto')
        ]
        
        for hospital_name, city in hospitals:
            # Look for the hospital name and nearby wait time
            for elem in soup.find_all(text=re.compile(hospital_name, re.I)):
                # Get surrounding text
                parent = elem.find_parent()
                if parent:
                    # Look in parent and siblings for time
                    context = parent.get_text() + ' '.join(
                        s.get_text() for s in parent.find_next_siblings()[:3]
                    )
                    
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

