// Cloudflare Pages Function - Current Week Respiratory Surveillance

import { executeQuery } from './_snowflake';

interface Env {
  SNOWFLAKE_ACCOUNT: string;
  SNOWFLAKE_USER: string;
  SNOWFLAKE_PRIVATE_KEY: string;
  SNOWFLAKE_WAREHOUSE: string;
  SNOWFLAKE_DATABASE: string;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    const config = {
      account: env.SNOWFLAKE_ACCOUNT || 'BMWIVTO-JF10661',
      user: env.SNOWFLAKE_USER || 'ontario_health_viewer',
      privateKey: env.SNOWFLAKE_PRIVATE_KEY,
      warehouse: env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database: env.SNOWFLAKE_DATABASE || 'ONTARIO_HEALTH',
      schema: 'MARTS_SURVEILLANCE'
    };
    
    const sql = 'SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week ORDER BY virus_name';
    const rows = await executeQuery(config, sql);
    
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

