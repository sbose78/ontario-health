/**
 * Cloudflare Worker - Snowflake to D1 Sync
 * 
 * Runs every 30 minutes via cron trigger.
 * Queries Snowflake MARTS and syncs to D1 cache for public dashboard.
 * 
 * Security: All credentials stay in Cloudflare (never in GitHub).
 */

interface Env {
  DB: D1Database;
  SNOWFLAKE_ACCOUNT: string;
  SNOWFLAKE_USER: string;
  SNOWFLAKE_PRIVATE_KEY: string;
}

export default {
  // Cron trigger handler
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    console.log('Sync triggered at:', new Date().toISOString());
    
    try {
      await syncSnowflakeToD1(env);
      console.log('✓ Sync complete');
    } catch (error: any) {
      console.error('✗ Sync failed:', error.message);
      throw error;
    }
  },
  
  // HTTP handler (for manual triggers)
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.url.includes('/trigger-sync')) {
      try {
        await syncSnowflakeToD1(env);
        return new Response(JSON.stringify({ status: 'success', message: 'Sync completed' }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (error: any) {
        return new Response(JSON.stringify({ status: 'error', message: error.message }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }
    
    return new Response('Ontario Health Sync Worker\n\nEndpoints:\n  POST /trigger-sync - Manual sync trigger', {
      headers: { 'Content-Type': 'text/plain' }
    });
  }
};

async function syncSnowflakeToD1(env: Env): Promise<void> {
  console.log('Starting Snowflake → D1 sync...');
  
  // Use Python SDK approach - Snowflake connector via SDK
  // Since JWT is complex, we'll use simple approach: query and sync
  
  // For now, use HTTP API to query Snowflake
  const account = env.SNOWFLAKE_ACCOUNT || 'BMWIVTO-JF10661';
  
  // Sync each dataset
  await syncCurrentWeek(env, account);
  await syncEDStatus(env, account);
  await syncViralTrends(env, account);
  await syncDataFreshness(env, account);
  
  console.log('All datasets synced');
}

async function querySnowflake(env: Env, sql: string): Promise<any[]> {
  // For production, implement Snowflake SQL API with JWT
  // For now, this is a placeholder - will be implemented
  
  // In production, this would:
  // 1. Generate JWT using private key
  // 2. Call Snowflake SQL API
  // 3. Return results
  
  // Temporary: Return empty to deploy Worker structure
  console.log('Querying:', sql.substring(0, 50) + '...');
  return [];
}

async function syncCurrentWeek(env: Env, account: string): Promise<void> {
  const rows = await querySnowflake(env, 'SELECT * FROM MARTS_SURVEILLANCE.rpt_current_week');
  
  if (rows.length === 0) {
    console.log('  current_week: No data (placeholder mode)');
    return;
  }
  
  // Clear and insert
  await env.DB.prepare('DELETE FROM current_week').run();
  
  for (const row of rows) {
    await env.DB.prepare(`
      INSERT INTO current_week (virus_name, epi_year, epi_week, sites_reporting, avg_viral_load, max_viral_load, min_viral_load)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
      row.VIRUS_NAME,
      row.EPI_YEAR,
      row.EPI_WEEK,
      row.SITES_REPORTING,
      row.AVG_VIRAL_LOAD,
      row.MAX_VIRAL_LOAD,
      row.MIN_VIRAL_LOAD
    ).run();
  }
  
  console.log(`  ✓ current_week: ${rows.length} rows`);
}

async function syncEDStatus(env: Env, account: string): Promise<void> {
  const rows = await querySnowflake(env, 'SELECT * FROM MARTS_SURVEILLANCE.rpt_ed_current');
  
  if (rows.length === 0) {
    console.log('  ed_current: No data (placeholder mode)');
    return;
  }
  
  await env.DB.prepare('DELETE FROM ed_current').run();
  
  for (const row of rows) {
    await env.DB.prepare(`
      INSERT INTO ed_current (hospital_name, wait_hours, wait_minutes, wait_total_minutes, source_updated, scraped_at, wait_severity)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).bind(
      row.HOSPITAL_NAME,
      row.WAIT_HOURS,
      row.WAIT_MINUTES,
      row.WAIT_TOTAL_MINUTES,
      row.SOURCE_UPDATED,
      row.SCRAPED_AT,
      row.WAIT_SEVERITY
    ).run();
  }
  
  console.log(`  ✓ ed_current: ${rows.length} rows`);
}

async function syncViralTrends(env: Env, account: string): Promise<void> {
  const rows = await querySnowflake(env, 
    'SELECT * FROM MARTS_SURVEILLANCE.rpt_viral_trends WHERE epi_year = 2025 AND epi_week >= 48'
  );
  
  if (rows.length === 0) {
    console.log('  viral_trends: No data (placeholder mode)');
    return;
  }
  
  await env.DB.prepare('DELETE FROM viral_trends').run();
  
  for (const row of rows) {
    await env.DB.prepare(`
      INSERT INTO viral_trends (epi_year, epi_week, virus_name, avg_viral_load, prev_week_avg, week_over_week_pct)
      VALUES (?, ?, ?, ?, ?, ?)
    `).bind(
      row.EPI_YEAR,
      row.EPI_WEEK,
      row.VIRUS_NAME,
      row.AVG_VIRAL_LOAD,
      row.PREV_WEEK_AVG,
      row.WEEK_OVER_WEEK_PCT
    ).run();
  }
  
  console.log(`  ✓ viral_trends: ${rows.length} rows`);
}

async function syncDataFreshness(env: Env, account: string): Promise<void> {
  const rows = await querySnowflake(env, 
    "SELECT * FROM MARTS_OPS.rpt_data_freshness WHERE category = 'surveillance'"
  );
  
  if (rows.length === 0) {
    console.log('  data_freshness: No data (placeholder mode)');
    return;
  }
  
  await env.DB.prepare('DELETE FROM data_freshness').run();
  
  for (const row of rows) {
    await env.DB.prepare(`
      INSERT INTO data_freshness (dataset, category, latest_data_date, total_records)
      VALUES (?, ?, ?, ?)
    `).bind(
      row.DATASET,
      row.CATEGORY,
      row.LATEST_DATA_DATE,
      row.TOTAL_RECORDS
    ).run();
  }
  
  console.log(`  ✓ data_freshness: ${rows.length} rows`);
}

