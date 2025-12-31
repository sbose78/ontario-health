"""Base scraper using Selenium for JavaScript-rendered sites."""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from typing import List, Dict
from .base import BaseHospitalScraper


class SeleniumHospitalScraper(BaseHospitalScraper):
    """Base scraper for JavaScript-rendered sites."""
    
    def get_driver(self):
        """Create headless Chrome driver."""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)')
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)
    
    def fetch_with_selenium(self, wait_seconds: int = 3) -> str:
        """Fetch page with Selenium, wait for JavaScript to load."""
        driver = self.get_driver()
        
        try:
            driver.get(self.url)
            time.sleep(wait_seconds)  # Wait for JS to execute
            
            html = driver.page_source
            return html
            
        finally:
            driver.quit()
    
    def fetch_and_parse(self) -> List[Dict]:
        """Override to use Selenium instead of requests."""
        try:
            html = self.fetch_with_selenium()
            
            # Create mock response object
            class MockResponse:
                def __init__(self, text):
                    self.text = text
            
            hospitals = self.parse(MockResponse(html))
            
            # Add metadata
            for h in hospitals:
                h['network'] = self.network_name
                from datetime import datetime
                h['scraped_at'] = datetime.now().isoformat()
            
            return hospitals
            
        except Exception as e:
            print(f"  Error with Selenium for {self.network_name}: {e}")
            return []

