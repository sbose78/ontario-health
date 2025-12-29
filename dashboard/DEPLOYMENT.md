# Dashboard Deployment Guide

## Prerequisites

✅ Viewer service account created (`sql/migrations/007_create_viewer_account.sql`)  
✅ Dashboard code ready in `dashboard/`  
✅ Private key at `~/.snowflake/ontario_health_viewer_key.p8`

## Deployment via GitHub → Cloudflare Pages

### 1. Push Dashboard Code

```bash
cd /Users/shbose/games/ontariohealth
git add dashboard/
git commit -m "Add public dashboard"
git push
```

### 2. Create Cloudflare Pages Project

In Cloudflare Dashboard:
1. **Pages** → **Create application** → **Connect to Git**
2. Select repository: `sbose78/ontario-health`
3. **Build settings**:
   - Build command: (leave empty)
   - Build output directory: `dashboard`
   - Root directory: `/dashboard`
4. Click **Save and Deploy**

### 3. Configure Secrets in Cloudflare

Go to: Pages → ontario-health-dashboard → Settings → Environment variables

Add these 5 secrets (Production):

```
SNOWFLAKE_ACCOUNT = BMWIVTO-JF10661
SNOWFLAKE_USER = ontario_health_viewer
SNOWFLAKE_WAREHOUSE = COMPUTE_WH
SNOWFLAKE_DATABASE = ONTARIO_HEALTH

SNOWFLAKE_PRIVATE_KEY = (paste from ~/.snowflake/ontario_health_viewer_key.p8)
```

### 4. Configure Custom Domain

In Cloudflare Pages:
1. **Custom domains** → **Set up a custom domain**
2. Enter: `ontariohealth.sbose.ca`
3. Cloudflare auto-creates DNS record (if domain is in Cloudflare)

If domain not in Cloudflare:
- Add CNAME record: `ontariohealth → <pages-url>.pages.dev`

### 5. Verify

Visit: https://ontariohealth.sbose.ca

Expected:
- Respiratory surveillance data (Week 51, 2025)
- ED wait times (Halton Healthcare)
- 4-week viral trends
- Auto-refresh every 5 minutes

## Manual Deployment (Alternative)

```bash
cd dashboard
npm install
npm run deploy
```

Then configure custom domain in Cloudflare Dashboard.

## Security Notes

- Dashboard is **public** (no authentication)
- Uses **read-only** service account (`ontario_health_viewer`)
- Can only SELECT from MARTS (no RAW/STAGING access)
- Private key stored in Cloudflare secrets (never in code)
- Data is aggregated public health info (no PII)

## Troubleshooting

**Dashboard loads but shows "Loading..."**:
- Check Cloudflare Pages Functions logs
- Verify secrets are set correctly
- Test viewer account: `python dashboard/test_viewer_connection.py`

**API returns errors**:
- Check viewer account has SELECT on MARTS
- Verify private key matches public key in Snowflake
- Check network policy allows Cloudflare IPs

## Monitoring

- Cloudflare Pages → Deployment logs
- Cloudflare Pages → Functions → Real-time logs
- Dashboard shows "Last updated" timestamp

