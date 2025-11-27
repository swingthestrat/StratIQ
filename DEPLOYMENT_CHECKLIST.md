# StratIQ Free Tier Deployment Checklist

## Prerequisites (5 min)
- [ ] GitHub account
- [ ] Push code to GitHub repo

## Phase 1: Database (Turso) - 10 min
- [ ] Sign up at https://turso.tech
- [ ] Install Turso CLI: `curl -sSfL https://get.tur.so/install.sh | bash`
- [ ] Create database: `turso db create stratiq --location ord`
- [ ] Get connection URL: `turso db show stratiq --url`
- [ ] Create auth token: `turso db tokens create stratiq`
- [ ] Test connection locally:
  ```bash
  export TURSO_DATABASE_URL="libsql://..."
  export TURSO_AUTH_TOKEN="your_token"
  python -c "from database import Session; print('Connected!')"
  ```

## Phase 2: Backend (Render) - 15 min
- [ ] Sign up at https://render.com
- [ ] New > Web Service
- [ ] Connect GitHub repo
- [ ] Use `render.yaml` configuration
- [ ] Add environment variables:
  - `TURSO_DATABASE_URL` (from Turso)
  - `TURSO_AUTH_TOKEN` (from Turso)
- [ ] Deploy (wait ~3 min)
- [ ] Test API: `curl https://stratiq-api.onrender.com/api/alerts?timeframe=1D`

## Phase 3: Frontend (Vercel) - 10 min
- [ ] Sign up at https://vercel.com
- [ ] Import Git Repository
- [ ] Root Directory: `frontend`
- [ ] Framework Preset: Vite
- [ ] Add environment variable:
  - `VITE_API_URL`: `https://stratiq-api.onrender.com` (your Render URL)
- [ ] Deploy
- [ ] Visit your app: `https://stratiq-yourname.vercel.app`

## Phase 4: Scheduled Updates (GitHub Actions) - 5 min
- [ ] Go to GitHub repo > Settings > Secrets
- [ ] Add secrets:
  - `TURSO_DATABASE_URL`
  - `TURSO_AUTH_TOKEN`
- [ ] Workflow will run automatically every 2 hours (defined in `.github/workflows/update_data.yml`)
- [ ] Test manually: Actions tab > Update StratIQ Data > Run workflow

## Verification
- [ ] Frontend loads at Vercel URL
- [ ] API returns data
- [ ] Filters work correctly
- [ ] MSFT shows as "2dG" on 2D timeframe
- [ ] No cold start issues (Render free tier spins down after 15 min)

## Next Steps
- Monitor GitHub Actions for update success/failures
- Check yfinance rate limits (if updates fail)
- Upgrade to paid tier if cold starts become problematic ($7/mo Render Starter)

**Estimated Total Time:** 45 minutes
