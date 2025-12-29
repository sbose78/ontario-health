// Cloudflare Pages Function - Data Freshness (from D1 cache)

interface Env {
  DB: D1Database;
}

export async function onRequest(context: { env: Env }): Promise<Response> {
  const { env } = context;
  
  try {
    const result = await env.DB.prepare(`
      SELECT 
        dataset,
        category,
        latest_data_date,
        total_records
      FROM data_freshness
      ORDER BY dataset
    `).all();
    
    return new Response(JSON.stringify(result.results), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=600' // Cache 10 min
      }
    });
    
  } catch (error: any) {
    return new Response(JSON.stringify({ 
      error: 'Failed to fetch freshness data', 
      message: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
