// Cloudflare Pages Function - Viral Trends (from D1 cache)

interface Env {
  DB: D1Database;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    const result = await env.DB.prepare(`
      SELECT 
        epi_year,
        epi_week,
        virus_name,
        avg_viral_load,
        prev_week_avg,
        week_over_week_pct
      FROM viral_trends
      ORDER BY epi_week DESC, virus_name
    `).all();
    
    return new Response(JSON.stringify(result.results), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=3600' // Cache 1 hour
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch trends', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
