// Cloudflare Pages Function - Data Freshness Check

interface Env {
  SNOWFLAKE_ACCOUNT: string;
  SNOWFLAKE_USER: string;
  SNOWFLAKE_PRIVATE_KEY: string;
  SNOWFLAKE_WAREHOUSE: string;
  SNOWFLAKE_DATABASE: string;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  const snowflake = await import('snowflake-sdk');
  
  try {
    const connection = snowflake.createConnection({
      account: env.SNOWFLAKE_ACCOUNT || 'BMWIVTO-JF10661',
      username: env.SNOWFLAKE_USER || 'ontario_health_viewer',
      privateKey: env.SNOWFLAKE_PRIVATE_KEY,
      warehouse: env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database: env.SNOWFLAKE_DATABASE || 'ONTARIO_HEALTH',
      schema: 'MARTS_OPS',
      authenticator: 'SNOWFLAKE_JWT'
    });
    
    await new Promise((resolve, reject) => {
      connection.connect((err) => err ? reject(err) : resolve(undefined));
    });
    
    const sql = `
      SELECT 
        dataset,
        category,
        latest_data_date,
        total_records
      FROM MARTS_OPS.rpt_data_freshness
      WHERE category = 'surveillance'
      ORDER BY dataset
    `;
    
    const rows = await new Promise<any[]>((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => err ? reject(err) : resolve(rows || [])
      });
    });
    
    connection.destroy();
    
    return new Response(JSON.stringify(rows), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=600' // Cache 10 min
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ error: 'Failed to fetch freshness data', message: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

