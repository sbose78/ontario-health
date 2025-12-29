// Cloudflare Pages Function - ED Wait Times (from D1 cache)

interface Env {
  DB: D1Database;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    const result = await env.DB.prepare(`
      SELECT 
        hospital_name,
        wait_hours,
        wait_minutes,
        wait_total_minutes,
        source_updated,
        scraped_at,
        wait_severity
      FROM ed_current
      ORDER BY wait_total_minutes DESC
    `).all();
    
    return new Response(JSON.stringify(result.results), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=180' // Cache 3 min
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch ED data', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
