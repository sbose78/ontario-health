"""
Unit tests for data ingestors.

Run with: pytest pipeline/tests/
"""
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd

# Test imports work
from pipeline.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER
from pipeline.ingest_wastewater import WastewaterIngestor
from pipeline.ingest_ed_wait_times import EDWaitTimesIngestor


class TestWastewaterIngestor(unittest.TestCase):
    """Test wastewater surveillance ingestor."""
    
    def setUp(self):
        self.ingestor = WastewaterIngestor(province_filter="Ontario")
    
    def test_virus_name_mapping(self):
        """Test virus code to name mapping."""
        self.assertEqual(self.ingestor.VIRUS_NAMES["covN2"], "COVID-19")
        self.assertEqual(self.ingestor.VIRUS_NAMES["fluA"], "Influenza A")
        self.assertEqual(self.ingestor.VIRUS_NAMES["fluB"], "Influenza B")
        self.assertEqual(self.ingestor.VIRUS_NAMES["rsv"], "RSV")
    
    def test_transform_valid_data(self):
        """Test transformation of valid wastewater data."""
        mock_data = pd.DataFrame([
            {
                "Location": "Toronto",
                "site": "Ashbridges Bay",
                "city": "Toronto",
                "province": "Ontario",
                "country": "Canada",
                "EpiYear": 2025,
                "EpiWeek": 51,
                "weekstart": "2025-12-14",
                "measureid": "fluA",
                "w_avg": 62.4,
                "min": 50.0,
                "max": 75.0,
                "populationcoverage": 2800000.0,
                "pruid": 35
            }
        ])
        
        result = self.ingestor.transform(mock_data)
        
        # Verify columns
        self.assertIn("EPI_YEAR", result.columns)
        self.assertIn("VIRUS_NAME", result.columns)
        self.assertIn("VIRAL_LOAD_AVG", result.columns)
        
        # Verify values
        self.assertEqual(result.iloc[0]["EPI_YEAR"], 2025)
        self.assertEqual(result.iloc[0]["VIRUS_NAME"], "Influenza A")
        self.assertEqual(result.iloc[0]["VIRAL_LOAD_AVG"], 62.4)
    
    def test_transform_filters_ontario(self):
        """Test that non-Ontario data is included in transform."""
        mock_data = pd.DataFrame([
            {"province": "Ontario", "EpiYear": 2025, "EpiWeek": 51, "measureid": "fluA", "w_avg": 50.0,
             "Location": "Toronto", "site": "A", "city": "Toronto", "country": "Canada", 
             "weekstart": "2025-12-14", "min": 0, "max": 100, "populationcoverage": 1000000, "pruid": 35},
            {"province": "Quebec", "EpiYear": 2025, "EpiWeek": 51, "measureid": "fluA", "w_avg": 40.0,
             "Location": "Montreal", "site": "B", "city": "Montreal", "country": "Canada",
             "weekstart": "2025-12-14", "min": 0, "max": 100, "populationcoverage": 1000000, "pruid": 24}
        ])
        
        # Province filtering happens at ingestion, not transform
        result = self.ingestor.transform(mock_data)
        self.assertEqual(len(result), 2)  # Both rows transformed


class TestEDWaitTimesIngestor(unittest.TestCase):
    """Test ED wait times scraper."""
    
    def setUp(self):
        self.ingestor = EDWaitTimesIngestor()
    
    def test_hospital_mapping(self):
        """Test hospital code to name mapping."""
        self.assertEqual(
            self.ingestor.HOSPITALS["georgetown"],
            "Georgetown Hospital"
        )
        self.assertEqual(
            self.ingestor.HOSPITALS["oakville"],
            "Oakville Trafalgar Memorial Hospital"
        )
    
    def test_transform_valid_scrape(self):
        """Test transformation of scraped ED data."""
        mock_records = [
            {
                "hospital_code": "georgetown",
                "hospital_name": "Georgetown Hospital",
                "wait_hours": 1,
                "wait_minutes": 48,
                "wait_total_minutes": 108,
                "source_updated": "Dec 28, 10:30 AM"
            },
            {
                "hospital_code": "oakville",
                "hospital_name": "Oakville Trafalgar Memorial Hospital",
                "wait_hours": 4,
                "wait_minutes": 4,
                "wait_total_minutes": 244,
                "source_updated": "Dec 28, 10:30 AM"
            }
        ]
        
        result = self.ingestor.transform(mock_records)
        
        # Verify structure
        self.assertEqual(len(result), 2)
        self.assertIn("HOSPITAL_NAME", result.columns)
        self.assertIn("WAIT_TOTAL_MINUTES", result.columns)
        
        # Verify calculations
        self.assertEqual(result.iloc[0]["WAIT_TOTAL_MINUTES"], 108)
        self.assertEqual(result.iloc[1]["WAIT_TOTAL_MINUTES"], 244)
    
    @patch('requests.Session.get')
    def test_parse_html(self, mock_get):
        """Test HTML parsing logic."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """
        <html>
        Last Updated: Dec 28, 10:30 AM
        georgetown01 Hour(s) and 48 Minute(s)
        milton02 Hour(s) and 37 Minute(s)
        oakville04 Hour(s) and 04 Minute(s)
        </html>
        """
        mock_get.return_value = mock_response
        
        records = self.ingestor.fetch_and_parse()
        
        # Should find 3 hospitals
        self.assertEqual(len(records), 3)
        
        # Verify Georgetown
        georgetown = next(r for r in records if r["hospital_code"] == "georgetown")
        self.assertEqual(georgetown["wait_hours"], 1)
        self.assertEqual(georgetown["wait_minutes"], 48)
        self.assertEqual(georgetown["wait_total_minutes"], 108)


class TestDataQuality(unittest.TestCase):
    """Test data quality rules."""
    
    def test_viral_load_non_negative(self):
        """Viral loads must be non-negative."""
        ingestor = WastewaterIngestor()
        
        # Valid data
        valid_df = pd.DataFrame([{"w_avg": 50.0, "EpiYear": 2025, "EpiWeek": 51, "measureid": "fluA",
                                  "province": "Ontario", "Location": "Toronto", "site": "A", 
                                  "city": "Toronto", "country": "Canada", "weekstart": "2025-12-14",
                                  "min": 0, "max": 100, "populationcoverage": 1000000, "pruid": 35}])
        result = ingestor.transform(valid_df)
        self.assertGreaterEqual(result.iloc[0]["VIRAL_LOAD_AVG"], 0)
        
        # Negative viral load should be caught in validation
        invalid_df = pd.DataFrame([{"w_avg": -5.0, "EpiYear": 2025, "EpiWeek": 51, "measureid": "fluA",
                                   "province": "Ontario", "Location": "Toronto", "site": "A",
                                   "city": "Toronto", "country": "Canada", "weekstart": "2025-12-14",
                                   "min": 0, "max": 100, "populationcoverage": 1000000, "pruid": 35}])
        result = ingestor.transform(invalid_df)
        # Should still transform but will fail dbt tests
        self.assertEqual(result.iloc[0]["VIRAL_LOAD_AVG"], -5.0)
    
    def test_ed_wait_time_sanity(self):
        """ED wait times should be reasonable (<12 hours)."""
        ingestor = EDWaitTimesIngestor()
        
        valid_records = [{
            "hospital_code": "georgetown",
            "hospital_name": "Georgetown Hospital",
            "wait_hours": 2,
            "wait_minutes": 30,
            "wait_total_minutes": 150,
            "source_updated": "Dec 28, 10:30 AM"
        }]
        
        result = ingestor.transform(valid_records)
        self.assertLess(result.iloc[0]["WAIT_TOTAL_MINUTES"], 720)  # 12 hours


if __name__ == "__main__":
    unittest.main()

