# 🛰️ Open Eye v4 — MENA Intelligence Dashboard

Real-time open-source intelligence for the Middle East. RSS, Reddit, HN, ACLED conflict events, GDELT tone, weather, FX, and AI briefs via Groq (primary) → Gemini (fallback).

**Deployed on Vercel (free). Data pipeline on GitHub Actions (free). No backend servers.**

---

## Architecture

```
GitHub Actions (cron)            Vercel (SPA host)
┌──────────────────────┐         ┌────────────────────┐
│ fetch-feeds  (30min) │──┐      │                    │
│ fetch-signals (1hr)  │──┼─git──►  public/data/*.json │──► SPA reads /data/*
│ generate-brief (6hr) │──┘ push │                    │
└──────────────────────┘         └────────────────────┘
```

Actions fetches all sources → commits JSON to `public/data/` → Vercel rebuilds SPA.

---

## Quick Deploy

### 1. Push to GitHub
```bash
git add . && git commit -m "feat: v4" && git push
```

### 2. Connect Vercel
vercel.com → New Project → Import repo → Framework: Vite → Deploy ✅

### 3. GitHub Secrets (for Actions pipeline)

Repo → Settings → Secrets → Actions:

| Secret | Source |
|--------|--------|
| `GROQ_API_KEY` | console.groq.com (free, gsk_...) |
| `GEMINI_API_KEY` | aistudio.google.com (free, fallback) |
| `ACLED_KEY` | developer.acleddata.com (free, register) |

### 4. First run — trigger Actions manually
Actions → "Fetch Feeds" → Run workflow
Actions → "Fetch Signals" → Run workflow
Actions → "Generate Brief" → Run workflow

---

## Local Dev
```bash
npm install
node scripts/fetch-feeds.js
ACLED_KEY=xxx node scripts/fetch-signals.js
GROQ_API_KEY=xxx node scripts/generate-brief.js
npm run dev
```

---

## Data Sources

RSS (BBC ME, AJ, Arab News, MEE, National UAE, AA, Guardian) · Reddit 10 ME subs (Pullpush, no auth) · HN Algolia · ACLED conflict events · GDELT country tone · Open-Meteo weather · Open Exchange Rates · Lottery signals (Reddit + HN)

## AI Brief Chain
Groq llama-3.3-70b → Gemini 2.5 Flash (429 fallback) → keyword brief (no API)
