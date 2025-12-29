# Ontario Health Sync Worker

Cloudflare Worker that syncs Snowflake MARTS data to D1 cache.

**Schedule**: Every 30 minutes (cron)

## Architecture

```
Cloudflare Worker (cron)
    ↓
Queries Snowflake MARTS (REST API + JWT)
    ↓
Writes to D1 (binding, no auth)
    ↓
Dashboard reads from D1
```

## Security Benefits

✅ **All secrets in Cloudflare** - Snowflake key never touches GitHub  
✅ **D1 binding** - No API tokens needed  
✅ **Single platform** - Smaller attack surface  

## Deployment

### 1. Set Secrets

```bash
cd workers

# Set secrets (one-time)
wrangler secret put SNOWFLAKE_ACCOUNT
# Enter: BMWIVTO-JF10661

wrangler secret put SNOWFLAKE_USER
# Enter: ontario_health_viewer

wrangler secret put SNOWFLAKE_PRIVATE_KEY
# Paste contents of ~/.snowflake/ontario_health_viewer_key.p8
```

### 2. Deploy

```bash
npm install
npm run deploy
```

Worker will run every 30 minutes automatically.

### 3. Manual Trigger (Testing)

```bash
curl -X POST https://ontario-health-sync.<your-subdomain>.workers.dev/trigger-sync
```

## Status: In Progress

⚠️ **Current state**: Worker structure created, Snowflake JWT auth being implemented.

**Alternative**: Keep using GitHub Actions sync (already working) until Worker JWT is fully tested.

