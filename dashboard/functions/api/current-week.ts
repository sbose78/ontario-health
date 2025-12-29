// Cloudflare Pages Function - Current Week Respiratory Surveillance
// Returns latest week viral load data from Snowflake

interface Env {
  SNOWFLAKE_ACCOUNT: string;
  SNOWFLAKE_USER: string;
  SNOWFLAKE_PRIVATE_KEY: string;
  SNOWFLAKE_WAREHOUSE: string;
  SNOWFLAKE_DATABASE: string;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  // Import snowflake connector
  const snowflake = await import('snowflake-sdk');
  
  try {
    // Create connection using viewer service account
    const connection = snowflake.createConnection({
      account: env.SNOWFLAKE_ACCOUNT || 'BMWIVTO-JF10661',
      username: env.SNOWFLAKE_USER || 'ontario_health_viewer',
      privateKey: env.SNOWFLAKE_PRIVATE_KEY,
      warehouse: env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database: env.SNOWFLAKE_DATABASE || 'ONTARIO_HEALTH',
      schema: 'MARTS_SURVEILLANCE',
      authenticator: 'SNOWFLAKE_JWT'
    });
    
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve(undefined);
      });
    });
    
    // Query current week data
    const sql = 'SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week ORDER BY virus_name';
    
    const rows = await new Promise<any[]>((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        }
      });
    });
    
    connection.destroy();
    
    // Return JSON
    return new Response(JSON.stringify(rows), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=900' // Cache 15 min
      }
    });
    
  } catch (error: any) {
    console.error('Error querying Snowflake:', error);
    
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch data', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

