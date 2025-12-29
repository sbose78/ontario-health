"""
Test configuration and connection.

Run with: pytest pipeline/tests/test_config.py
"""
import unittest
from pathlib import Path

from pipeline.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SCHEMA_RAW,
    SCHEMA_MARTS_SURVEILLANCE,
    get_private_key_bytes,
    PRIVATE_KEY_FILE
)


class TestConfig(unittest.TestCase):
    """Test configuration values."""
    
    def test_snowflake_account(self):
        """Verify Snowflake account is set."""
        self.assertEqual(SNOWFLAKE_ACCOUNT, "BMWIVTO-JF10661")
    
    def test_snowflake_user(self):
        """Verify Snowflake user is service account."""
        self.assertEqual(SNOWFLAKE_USER, "ontario_health_svc")
    
    def test_schema_names(self):
        """Verify schema naming conventions."""
        self.assertEqual(SCHEMA_RAW, "RAW")
        self.assertEqual(SCHEMA_MARTS_SURVEILLANCE, "MARTS_SURVEILLANCE")
    
    def test_private_key_file_exists(self):
        """Check if private key file exists."""
        from config import PRIVATE_KEY_FILE
        self.assertTrue(PRIVATE_KEY_FILE.exists(), 
                       f"Private key file not found at {PRIVATE_KEY_FILE}")
    
    def test_private_key_readable(self):
        """Verify private key file can be read and is valid."""
        from config import get_private_key_bytes
        key_bytes = get_private_key_bytes()
        self.assertIsInstance(key_bytes, bytes)
        self.assertGreater(len(key_bytes), 0)


if __name__ == "__main__":
    unittest.main()

