// Cloudflare Pages Function - Viral Trends (4 weeks)

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
      schema: 'MARTS_SURVEILLANCE',
      authenticator: 'SNOWFLAKE_JWT'
    });
    
    await new Promise((resolve, reject) => {
      connection.connect((err) => err ? reject(err) : resolve(undefined));
    });
    
    const sql = `
      SELECT 
        epi_year,
        epi_week,
        virus_name,
        avg_viral_load,
        prev_week_avg,
        week_over_week_pct
      FROM MARTS_SURVEILLANCE.rpt_viral_trends
      WHERE epi_year = 2025 AND epi_week >= 48
      ORDER BY epi_week DESC, virus_name
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
        'Cache-Control': 'public, max-age=3600' // Cache 1 hour
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ error: 'Failed to fetch trends', message: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

