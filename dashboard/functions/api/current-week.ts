// Cloudflare Pages Function - Current Week (from D1 cache)

interface Env {
  DB: D1Database;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    // Query D1 cache (no Snowflake auth needed!)
    const result = await env.DB.prepare(`
      SELECT 
        virus_name,
        epi_year,
        epi_week,
        sites_reporting,
        avg_viral_load,
        max_viral_load,
        min_viral_load
      FROM current_week
      ORDER BY virus_name
    `).all();
    
    return new Response(JSON.stringify(result.results), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=900' // Cache 15 min
      }
    });
    
  } catch (error: any) {
    console.error('Error querying D1:', error);
    
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch data', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
