#!/usr/bin/env python3
"""Test JWT generation for Snowflake key-pair auth."""
import sys
import time
import json
import base64
import hashlib
from pathlib import Path
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

def generate_jwt():
    # Config
    account_locator = "BMWIVTO-BG45124"  # Org-Account for JWT
    account_full = "BMWIVTO-JF10661"     # Full identifier for API URL
    user = "ontario_health_viewer"
    
    # Load private key
    key_file = Path.home() / '.snowflake' / 'ontario_health_viewer_key.p8'
    with open(key_file, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    
    # Get public key fingerprint (base64 format like Snowflake stores it)
    public_key = private_key.public_key()
    public_key_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    fingerprint_bytes = hashlib.sha256(public_key_der).digest()
    fingerprint = base64.b64encode(fingerprint_bytes).decode('utf-8')
    
    print(f"Account (API): {account_full}")
    print(f"Account (JWT): {account_locator}")
    print(f"User: {user}")
    print(f"Public Key Fingerprint: SHA256:{fingerprint}")
    
    # Create JWT
    header = {"alg": "RS256", "typ": "JWT"}
    
    now = int(time.time())
    qualified_user = f"{account_locator.upper()}.{user.upper()}"
    
    payload = {
        "iss": f"{qualified_user}.{fingerprint}",  # fingerprint without SHA256: prefix
        "sub": qualified_user,
        "iat": now,
        "exp": now + 3600
    }
    
    print(f"\nJWT Payload:")
    print(json.dumps(payload, indent=2))
    
    # Encode
    def b64url_encode(data):
        if isinstance(data, dict):
            data = json.dumps(data, separators=(',', ':')).encode()  # Compact JSON
        return base64.urlsafe_b64encode(data).decode().rstrip('=')
    
    header_enc = b64url_encode(header)
    payload_enc = b64url_encode(payload)
    message = f"{header_enc}.{payload_enc}".encode()
    
    # Sign
    signature = private_key.sign(
        message,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    signature_enc = b64url_encode(signature)
    
    jwt = f"{header_enc}.{payload_enc}.{signature_enc}"
    
    print(f"\nJWT Token: {jwt[:50]}...")
    print(f"Length: {len(jwt)}")
    
    return jwt

if __name__ == "__main__":
    jwt = generate_jwt()

