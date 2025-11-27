# API Security Update

## Changes Made

### 1. Rate Limiting ✅
- **Library:** `slowapi` (added to requirements.txt)
- **Limit:** 60 requests/minute per IP address
- **Protection:** Prevents API abuse and quota exhaustion

### 2. CORS Whitelisting ✅
- **Before:** `allow_origins=["*"]` (anyone can access)
- **After:** Environment-based whitelist
  - **Local:** `http://localhost:5173, 5174, 5175`
  - **Production:** Your Vercel domain (configurable via `CORS_ORIGINS` env var)
- **Methods:** Restricted to GET only (no POST/DELETE needed)

### 3. Configuration
**Render Environment Variable:**
```yaml
CORS_ORIGINS: "https://stratiq-yourname.vercel.app,http://localhost:5173"
```

**Update after deployment:**
Once you get your Vercel URL, update the `CORS_ORIGINS` value in Render dashboard to include it.

## Security Benefits
- ✅ Prevents unauthorized websites from scraping your data
- ✅ Limits request frequency to prevent abuse
- ✅ Protects free tier quotas
- ✅ No code changes needed for different environments (uses env vars)

## Testing
After restart, the API will:
- Accept requests from localhost (for development)
- Reject requests from other origins
- Return 429 error if rate limit exceeded (60/min)
