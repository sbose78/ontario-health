"""
Configuration for Ontario Health Data Pipeline.

Snowflake connection uses PAT (Programmatic Access Token) authentication.
Token should be stored in ~/.snowflake/ontario_health_token
"""
import os
from pathlib import Path

# Snowflake Configuration (Service Account)
SNOWFLAKE_ACCOUNT = "BMWIVTO-JF10661"
SNOWFLAKE_USER = "ontario_health_svc"  # Service account
SNOWFLAKE_ROLE = "ontario_health_role"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "ONTARIO_HEALTH"

# Schemas (dbt-style naming)
SCHEMA_RAW = "RAW"
SCHEMA_STAGING = "STAGING"

# MARTS schemas (organized by domain)
SCHEMA_MARTS_SURVEILLANCE = "MARTS_SURVEILLANCE"  # Current respiratory data
SCHEMA_MARTS_HISTORICAL = "MARTS_HISTORICAL"      # Archived reference data
SCHEMA_MARTS_OPS = "MARTS_OPS"                    # Pipeline operations

# Private key file path (for service account)
PRIVATE_KEY_FILE = Path.home() / ".snowflake" / "ontario_health_key.p8"


def get_private_key_bytes() -> bytes:
    """Read private key from file."""
    if not PRIVATE_KEY_FILE.exists():
        raise FileNotFoundError(
            f"Private key file not found at {PRIVATE_KEY_FILE}. "
            "Generate with: openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out {PRIVATE_KEY_FILE} -nocrypt"
        )
    
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    
    with open(PRIVATE_KEY_FILE, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def get_snowflake_connection():
    """Create Snowflake connection using key-pair authentication (service account)."""
    import snowflake.connector
    
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=get_private_key_bytes(),
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE
    )


# Ontario Data Catalogue (CKAN) Configuration
CKAN_BASE_URL = "https://data.ontario.ca/api/3/action"

# Dataset Resource IDs - these are the actual data files within datasets
# Note: Resource IDs may change; verify at data.ontario.ca if ingestion fails
DATASETS = {
    "school_cases": {
        "name": "Summary of Cases in Schools",
        "dataset_url": "https://data.ontario.ca/dataset/summary-of-cases-in-schools",
        "resource_id": None,  # Will be discovered via API
        "refresh": "daily"
    },
    "outbreaks": {
        "name": "Ongoing Outbreaks",
        "dataset_url": "https://data.ontario.ca/dataset/ontario-covid-19-outbreaks-data", 
        "resource_id": None,  # Will be discovered via API
        "refresh": "daily"
    },
    "confirmed_positive_cases": {
        "name": "Confirmed Positive Cases of COVID-19 in Ontario",
        "dataset_url": "https://data.ontario.ca/dataset/confirmed-positive-cases-of-covid-19-in-ontario",
        "resource_id": None,  # Will be discovered via API
        "refresh": "daily"
    }
}

# Age groups of interest (pediatric focus)
PEDIATRIC_AGE_GROUPS = [
    "0-4",
    "5-11", 
    "12-19",
    "<20",
    "Under 20"
]

# PHU codes for Halton Region
HALTON_PHU_CODES = ["2236"]
HALTON_PHU_NAMES = ["Halton Region Health Department", "Halton Region"]

