// Cloudflare Pages Function - ED Wait Times History (24 hours)

interface Env {
  DB: D1Database;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    const result = await env.DB.prepare(`
      SELECT 
        hospital_name,
        wait_total_minutes,
        scraped_at,
        wait_severity
      FROM ed_history
      WHERE scraped_at >= datetime('now', '-24 hours')
      ORDER BY scraped_at ASC
    `).all();
    
    return new Response(JSON.stringify(result.results), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=300' // Cache 5 min
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch ED history', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

