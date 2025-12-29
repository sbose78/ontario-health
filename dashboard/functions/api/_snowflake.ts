// Shared Snowflake REST API helper for Cloudflare Pages Functions
// Uses Snowflake SQL API (REST) instead of Node SDK

interface SnowflakeConfig {
  account: string;           // Full account identifier for API URL (BMWIVTO-JF10661)
  accountLocator: string;    // Org-account for JWT (BMWIVTO-BG45124)
  user: string;
  privateKey: string;
  publicKeyFP: string;       // SHA256 fingerprint of public key
  warehouse: string;
  database: string;
  schema: string;
}

export async function executeQuery(config: SnowflakeConfig, sql: string): Promise<any[]> {
  // Generate JWT token for authentication
  const jwt = await generateJWT(config.accountLocator, config.user, config.privateKey, config.publicKeyFP);
  
  // Snowflake SQL API endpoint
  const url = `https://${config.account}.snowflakecomputing.com/api/v2/statements`;
  
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${jwt}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'OntarioHealthDashboard/1.0',
      'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT'
    },
    body: JSON.stringify({
      statement: sql,
      timeout: 60,
      warehouse: config.warehouse,
      database: config.database,
      schema: config.schema
    })
  });
  
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Snowflake query failed: ${response.status} - ${error}`);
  }
  
  const result = await response.json();
  
  // Convert Snowflake result format to array of objects
  if (result.data && result.resultSetMetaData) {
    const columns = result.resultSetMetaData.rowType.map((col: any) => col.name);
    return result.data.map((row: any[]) => {
      const obj: any = {};
      columns.forEach((col: string, i: number) => {
        obj[col] = row[i];
      });
      return obj;
    });
  }
  
  return [];
}

async function generateJWT(account: string, user: string, privateKeyPEM: string, publicKeyFP: string): Promise<string> {
  // Parse account to get qualified name
  const accountName = account.toUpperCase();
  const userName = user.toUpperCase();
  
  // JWT header
  const header = {
    alg: 'RS256',
    typ: 'JWT'
  };
  
  // JWT payload
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: `${accountName}.${userName}.SHA256:${publicKeyFP}`,
    sub: `${accountName}.${userName}`,
    iat: now,
    exp: now + 3600 // 1 hour expiry
  };
  
  // Create JWT
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signatureInput = `${encodedHeader}.${encodedPayload}`;
  
  // Sign with private key
  const signature = await signRS256(signatureInput, privateKeyPEM);
  
  return `${signatureInput}.${signature}`;
}

async function importPrivateKey(pem: string): Promise<CryptoKey> {
  // Remove PEM headers and decode base64
  const pemContents = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\s/g, '');
  
  const binaryDer = Uint8Array.from(atob(pemContents), c => c.charCodeAt(0));
  
  return await crypto.subtle.importKey(
    'pkcs8',
    binaryDer,
    {
      name: 'RSASSA-PKCS1-v1_5',
      hash: 'SHA-256'
    },
    false,
    ['sign']
  );
}

async function signRS256(data: string, privateKeyPEM: string): Promise<string> {
  const privateKey = await importPrivateKey(privateKeyPEM);
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    privateKey,
    new TextEncoder().encode(data)
  );
  return base64UrlEncode(signature);
}

function base64UrlEncode(data: string | ArrayBuffer): string {
  let base64;
  
  if (typeof data === 'string') {
    base64 = btoa(data);
  } else {
    base64 = btoa(String.fromCharCode(...new Uint8Array(data)));
  }
  
  return base64
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
}


