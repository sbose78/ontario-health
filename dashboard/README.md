# Ontario Health Public Dashboard

Public-facing dashboard for Ontario respiratory surveillance data.

**Live URL**: https://ontariohealth.sbose.ca (after deployment)

## Architecture

```
Public Users
     ↓
Cloudflare Pages (ontariohealth.sbose.ca)
     ↓
Pages Functions (/api/*)
     ↓
Snowflake MARTS_SURVEILLANCE (read-only service account)
```

## Local Development

### 1. Install Dependencies

```bash
cd dashboard
npm install
```

### 2. Set Environment Variables (Local Testing)

```bash
export SNOWFLAKE_ACCOUNT=BMWIVTO-JF10661
export SNOWFLAKE_USER=ontario_health_viewer
export SNOWFLAKE_PRIVATE_KEY=$(cat ~/.snowflake/ontario_health_viewer_key.p8)
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=ONTARIO_HEALTH
```

### 3. Run Locally

```bash
npm run dev
# Opens http://localhost:8788
```

## Deployment

### 1. Set Cloudflare Secrets

```bash
cd dashboard

# Set secrets (one-time)
wrangler pages secret put SNOWFLAKE_ACCOUNT
# Enter: BMWIVTO-JF10661

wrangler pages secret put SNOWFLAKE_USER
# Enter: ontario_health_viewer

wrangler pages secret put SNOWFLAKE_PRIVATE_KEY
# Paste contents of ~/.snowflake/ontario_health_viewer_key.p8

wrangler pages secret put SNOWFLAKE_WAREHOUSE
# Enter: COMPUTE_WH

wrangler pages secret put SNOWFLAKE_DATABASE
# Enter: ONTARIO_HEALTH
```

### 2. Deploy

```bash
npm run deploy
```

### 3. Configure Custom Domain

In Cloudflare Dashboard:
1. Pages → ontario-health-dashboard → Custom domains
2. Add domain: `ontariohealth.sbose.ca`
3. Add DNS record (done automatically if domain in Cloudflare)

## API Endpoints

| Endpoint | Description | Cache |
|----------|-------------|-------|
| `/api/current-week` | Latest week viral loads | 15 min |
| `/api/ed-status` | Current ED wait times | 3 min |
| `/api/viral-trends` | 4-week trends | 1 hour |
| `/api/data-freshness` | Data recency | 10 min |

## Security

- Read-only service account (`ontario_health_viewer`)
- Only SELECT access to MARTS schemas
- No access to RAW or STAGING
- Private key stored in Cloudflare secrets (not in code)
- Public data only (no PII)

## Data Sources

- **Wastewater**: Health Canada (weekly updates)
- **ED Wait Times**: Halton Healthcare (every 3 hours)
- Auto-refresh on page, updates every 5 minutes

