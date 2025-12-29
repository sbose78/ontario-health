/**
 * Snowflake REST API helper for Cloudflare Workers
 * Uses SQL API with JWT key-pair authentication
 */

interface SnowflakeQuery {
  account: string;      // BMWIVTO-JF10661
  accountLocator: string;  // BMWIVTO-BG45124  
  user: string;
  privateKey: string;
  publicKeyFP: string;
  warehouse: string;
  database: string;
  schema: string;
  sql: string;
}

export async function querySnowflake(config: SnowflakeQuery): Promise<any[]> {
  // Generate JWT
  const jwt = await generateJWT(
    config.accountLocator,
    config.user,
    config.privateKey,
    config.publicKeyFP
  );
  
  // Query Snowflake SQL API
  const url = `https://${config.account}.snowflakecomputing.com/api/v2/statements`;
  
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${jwt}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'OntarioHealthWorker/1.0',
      'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT'
    },
    body: JSON.stringify({
      statement: config.sql,
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
  
  // Convert Snowflake format to array of objects
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

async function generateJWT(
  accountLocator: string,
  user: string,
  privateKeyPEM: string,
  publicKeyFP: string
): Promise<string> {
  const header = { alg: 'RS256', typ: 'JWT' };
  
  const now = Math.floor(Date.now() / 1000);
  const qualifiedUser = `${accountLocator.toUpperCase()}.${user.toUpperCase()}`;
  
  const payload = {
    iss: `${qualifiedUser}.${publicKeyFP}`,  // Use Snowflake's base64 fingerprint
    sub: qualifiedUser,
    iat: now,
    exp: now + 3600
  };
  
  // Encode header and payload
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signatureInput = `${encodedHeader}.${encodedPayload}`;
  
  // Sign with private key
  const signature = await signRS256(signatureInput, privateKeyPEM);
  
  return `${signatureInput}.${signature}`;
}

async function signRS256(data: string, privateKeyPEM: string): Promise<string> {
  // Import private key
  const pemContents = privateKeyPEM
    .replace(/-----BEGIN PRIVATE KEY-----/, '')
    .replace(/-----END PRIVATE KEY-----/, '')
    .replace(/\s/g, '');
  
  const binaryDer = Uint8Array.from(atob(pemContents), c => c.charCodeAt(0));
  
  const privateKey = await crypto.subtle.importKey(
    'pkcs8',
    binaryDer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign']
  );
  
  // Sign
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    privateKey,
    new TextEncoder().encode(data)
  );
  
  return base64UrlEncode(signature);
}

function base64UrlEncode(data: string | ArrayBuffer): string {
  let base64: string;
  
  if (typeof data === 'string') {
    base64 = btoa(data);
  } else {
    base64 = btoa(String.fromCharCode(...new Uint8Array(data)));
  }
  
  return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

