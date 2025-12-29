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
    get_snowflake_token,
    TOKEN_FILE
)


class TestConfig(unittest.TestCase):
    """Test configuration values."""
    
    def test_snowflake_account(self):
        """Verify Snowflake account is set."""
        self.assertEqual(SNOWFLAKE_ACCOUNT, "BMWIVTO-JF10661")
    
    def test_snowflake_user(self):
        """Verify Snowflake user is set."""
        self.assertEqual(SNOWFLAKE_USER, "SBOSE78")
    
    def test_schema_names(self):
        """Verify schema naming conventions."""
        self.assertEqual(SCHEMA_RAW, "RAW")
        self.assertEqual(SCHEMA_MARTS_SURVEILLANCE, "MARTS_SURVEILLANCE")
    
    def test_token_file_exists(self):
        """Check if token file exists."""
        self.assertTrue(TOKEN_FILE.exists(), 
                       f"Token file not found at {TOKEN_FILE}")
    
    def test_token_readable(self):
        """Verify token file can be read."""
        token = get_snowflake_token()
        self.assertIsInstance(token, str)
        self.assertGreater(len(token), 0)
        # PAT tokens are JWTs, should have 3 parts
        self.assertEqual(len(token.split('.')), 3)


if __name__ == "__main__":
    unittest.main()

