#!/usr/bin/env node
// scripts/fetch-signals.js
// Fetches GDELT tone, Open-Meteo weather, FX rates, lottery signals
// → public/data/signals.json
// Run by GitHub Actions cron every hour.

import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/signals.json");
mkdirSync(dirname(OUT), { recursive: true });

// ACLED uses OAuth 2.0 — store your myACLED email+password as GitHub Secrets
const NEWSAPI_KEY    = process.env.NEWSAPI_KEY    || "";
const GUARDIAN_KEY   = process.env.GUARDIAN_KEY   || "";

// MENA capitals for weather
const CAPITALS = {
  "UAE":           { lat: 24.4539, lon: 54.3773, city: "Abu Dhabi" },
  "Saudi Arabia":  { lat: 24.7136, lon: 46.6753, city: "Riyadh" },
  "Qatar":         { lat: 25.2854, lon: 51.5310, city: "Doha" },
  "Kuwait":        { lat: 29.3759, lon: 47.9774, city: "Kuwait City" },
  "Bahrain":       { lat: 26.2235, lon: 50.5876, city: "Manama" },
  "Oman":          { lat: 23.6139, lon: 58.5922, city: "Muscat" },
  "Jordan":        { lat: 31.9522, lon: 35.9334, city: "Amman" },
  "Lebanon":       { lat: 33.8886, lon: 35.4955, city: "Beirut" },
  "Iraq":          { lat: 33.3406, lon: 44.4009, city: "Baghdad" },
  "Egypt":         { lat: 30.0444, lon: 31.2357, city: "Cairo" },
  "Yemen":         { lat: 15.3694, lon: 44.1910, city: "Sanaa" },
  "Iran":          { lat: 35.6892, lon: 51.3890, city: "Tehran" },
};


// Lottery keywords
const LOT_KW = ["lottery","jackpot","won","winner","winning","lucky","raffle","prize","draw","million","ticket","duty free","big ticket","mahzooz","gambling","lotto","lucky draw","grand prize","sweepstake","powerball"];
const LOT_HOPEFUL = ["win","winner","jackpot","lucky","dream","hope","million","ticket","chance","raffle","prize","draw","blessed","fortune","rich","wealth","congratulations","won","awarded"];
const LOT_CYNICAL  = ["scam","fraud","rigged","impossible","waste","sucker","odds","cheat","fake","illegal","banned","haram","forbidden","corrupt","addiction","trap","beware","warning","regret"];
const LOT_ANXIOUS  = ["debt","desperate","spent","need","last","only","please","help","poor","broke","struggling","worry","afford","savings","emergency"];

function classifyLottery(text) {
  const t = (text||"").toLowerCase();
  const h = LOT_HOPEFUL.filter(w=>t.includes(w)).length;
  const c = LOT_CYNICAL.filter(w=>t.includes(w)).length;
  const a = LOT_ANXIOUS.filter(w=>t.includes(w)).length;
  if (a>1) return "ANXIOUS";
  if (c>h+1) return "CYNICAL";
  if (h>c+1) return "HOPEFUL";
  if (h>0) return "HOPEFUL";
  return "NEUTRAL";
}

function isME(text) {
  const t = (text||"").toLowerCase();
  return [
    // Countries / cities
    "uae","dubai","abu dhabi","sharjah","ajman","ras al khaimah","fujairah",
    "saudi","riyadh","jeddah","ksa","qatar","doha","kuwait","oman","muscat",
    "bahrain","manama","jordan","egypt","cairo","beirut","lebanon",
    // ME-specific lottery brands
    "mahzooz","emirates draw","big ticket","duty free","dream dubai",
    "abu dhabi duty free","dubai duty free","national lottery uae",
    "saudi lottery","riyadh draw","gulf lottery","arab lottery",
    // Currency signals (strong ME indicator)
    "dirham","aed","sar","riyal","qar","kwd","bhd","omr",
    // Expat context
    "expat","gulf","arab","middle east","mena",
  ].some(k => t.includes(k));
}



async function fetchACLED() {
  if (!ACLED_EMAIL || !ACLED_PASSWORD) {
    console.log("[acled] No ACLED_EMAIL/ACLED_PASSWORD set — skipping");
    return [];
  }
  try {
    const token = await getACLEDToken();
    console.log("[acled] Token obtained");

    const since = new Date(Date.now() - 30*24*3600*1000).toISOString().split("T")[0];
    const today = new Date().toISOString().split("T")[0];

    // ACLED country filter syntax: country=X:OR:country=Y (value of single param)
    const countryValue = ACLED_COUNTRIES.join(":OR:country=");

    const params = new URLSearchParams({
      "_format": "json",
      "country": countryValue,
      "event_date": `${since}|${today}`,
      "event_date_where": "BETWEEN",
      "fields": "event_id_cnty|event_date|event_type|sub_event_type|country|admin1|location|latitude|longitude|fatalities|notes",
      "limit": "500",
    });

    const url = `https://acleddata.com/api/acled/read?${params}`;

    const res = await fetch(url, {
      headers: { "Authorization": `Bearer ${token}` },
      signal: AbortSignal.timeout(25000),
    });

    if (res.status === 403) {
      const body = await res.text().catch(() => "");
      throw new Error(
        `ACLED 403 Forbidden — API access may not be enabled on your account.\n` +
        `  → Log in at acleddata.com → My Account → ensure API access is activated.\n` +
        `  → Response: ${body.slice(0, 200)}`
      );
    }
    if (!res.ok) throw new Error(`ACLED data HTTP ${res.status}`);

    const d = await res.json();
    if (d.status && d.status !== 200) {
      throw new Error(`ACLED API error: ${d.message || JSON.stringify(d).slice(0, 200)}`);
    }

    const events = (d.data||[]).map(e => ({
      event_id:       e.event_id_cnty,
      event_date:     e.event_date,
      event_type:     e.event_type,
      sub_event_type: e.sub_event_type,
      country:        e.country,
      admin1:         e.admin1,
      location:       e.location,
      latitude:       parseFloat(e.latitude)||0,
      longitude:      parseFloat(e.longitude)||0,
      fatalities:     parseInt(e.fatalities)||0,
      notes:          (e.notes||"").slice(0,200),
    }));
    console.log(`[acled] ${events.length} events`);
    return events;
  } catch (err) {
    console.error("[acled] failed:", err.message);
    return [];
  }
}

// ── GDELT via BigQuery ────────────────────────────────────────────────────────
// Uses gdelt-bq.gdeltv2.gkg (partitioned) — far more reliable than DOC API
// Requires GCP_SERVICE_ACCOUNT_JSON secret (base64-encoded service account key)
// Free tier: 1TB/month — this query uses ~200MB/day
//
// GKG SourceCountryCode uses FIPS codes:
const GDELT_COUNTRIES = [
  { id: "UAE",          fips: "AE" }, { id: "Saudi Arabia",  fips: "SA" },
  { id: "Qatar",        fips: "QA" }, { id: "Kuwait",        fips: "KU" },
  { id: "Bahrain",      fips: "BA" }, { id: "Oman",          fips: "MU" },
  { id: "Jordan",       fips: "JO" }, { id: "Lebanon",       fips: "LE" },
  { id: "Iraq",         fips: "IZ" }, { id: "Egypt",         fips: "EG" },
  { id: "Yemen",        fips: "YM" }, { id: "Iran",          fips: "IR" },
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Get OAuth2 access token from service account JSON
async function getBQToken(serviceAccountJson) {
  const sa = JSON.parse(serviceAccountJson);
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: sa.client_email,
    scope: "https://www.googleapis.com/auth/bigquery.readonly",
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };

  // Build JWT using Web Crypto (available in Node 18+)
  const enc = s => Buffer.from(JSON.stringify(s)).toString("base64url");
  const signingInput = `${enc(header)}.${enc(payload)}`;

  // Import private key
  const keyData = sa.private_key
    .replace(/-----BEGIN PRIVATE KEY-----/, "")
    .replace(/-----END PRIVATE KEY-----/, "")
    .replace(/\n/g, "");
  const keyBuffer = Buffer.from(keyData, "base64");

  const cryptoKey = await crypto.subtle.importKey(
    "pkcs8", keyBuffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false, ["sign"]
  );
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5", cryptoKey,
    Buffer.from(signingInput)
  );
  const jwt = `${signingInput}.${Buffer.from(signature).toString("base64url")}`;

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt,
    }),
  });
  if (!res.ok) throw new Error(`BQ auth failed: HTTP ${res.status}`);
  const d = await res.json();
  return d.access_token;
}

async function fetchGDELTTone() {
  const GCP_SA = process.env.GCP_SERVICE_ACCOUNT_JSON || "";

  if (!GCP_SA) {
    console.log("[gdelt] No GCP_SERVICE_ACCOUNT_JSON — falling back to DOC API");
    return fetchGDELTToneDocAPI();
  }

  try {
    const token = await getBQToken(GCP_SA);
    const projectId = JSON.parse(GCP_SA).project_id;

    // Query events_partitioned — same table as fetch-gdelt-bq.js, confirmed working
    // Aggregate AvgTone by ActionGeo_CountryCode (FIPS) over last 7 days
    const fipsList = GDELT_COUNTRIES.map(c => `'${c.fips}'`).join(",");
    const sevenDaysAgo = new Date(Date.now() - 7*24*3600*1000).toISOString().split("T")[0];
    const sqlDateFrom = sevenDaysAgo.replace(/-/g, "");

    const query = `
      SELECT
        ActionGeo_CountryCode,
        AVG(AvgTone) AS avg_tone,
        COUNT(*) AS event_count
      FROM \`gdelt-bq.gdeltv2.events_partitioned\`
      WHERE _PARTITIONTIME >= TIMESTAMP('${sevenDaysAgo}')
        AND SQLDATE >= ${sqlDateFrom}
        AND ActionGeo_CountryCode IN (${fipsList})
        AND AvgTone IS NOT NULL
        AND NumArticles >= 1
      GROUP BY ActionGeo_CountryCode
      ORDER BY event_count DESC
    `;

    const res = await fetch(
      `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/queries`,
      {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query, useLegacySql: false, timeoutMs: 30000 }),
        signal: AbortSignal.timeout(35000),
      }
    );

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`BQ query failed: HTTP ${res.status} — ${err.slice(0, 200)}`);
    }

    const result = await res.json();
    if (!result.jobComplete) throw new Error("BQ job did not complete in time");

    // Map FIPS → tone
    const byFips = {};
    for (const row of (result.rows || [])) {
      byFips[row.f[0].v] = { tone: parseFloat(row.f[1].v), count: parseInt(row.f[2].v) };
    }

    const tone = {};
    for (const { id, fips } of GDELT_COUNTRIES) {
      const entry = byFips[fips];
      tone[id] = entry ? Math.round(entry.tone * 100) / 100 : null;
      if (entry) console.log(`[gdelt:${id}] tone=${tone[id]} (${entry.count} events)`);
    }

    console.log(`[gdelt] BigQuery: tone for ${Object.values(tone).filter(v=>v!==null).length}/12 countries`);
    return tone;

  } catch (err) {
    console.error("[gdelt] BigQuery failed:", err.message);
    console.log("[gdelt] BQ failed — returning nulls (DOC API blocked from GH Actions)");
    return fetchGDELTToneDocAPI();
  }
}

// DOC API blocked from GitHub Actions — return nulls immediately
// Tone data comes from BQ events_partitioned; if BQ fails, nulls are fine
async function fetchGDELTToneDocAPI() {
  console.log("[gdelt-doc] skipped — DOC API blocked from GitHub Actions");
  return Object.fromEntries(GDELT_COUNTRIES.map(c => [c.id, null]));
}

// ── Weather ──────────────────────────────────────────────────────────────────
async function fetchWeather() {
  const weather = {};
  const entries = Object.entries(CAPITALS);
  // Batch request via Open-Meteo (free, no auth)
  await Promise.allSettled(entries.map(async ([country, { lat, lon, city }]) => {
    try {
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,wind_speed_10m,weathercode&forecast_days=1&timezone=auto`;
      const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      const code = d.current?.weathercode||0;
      const condition = code===0?"Clear":code<=3?"Partly Cloudy":code<=48?"Fog/Haze":code<=67?"Rain":code<=77?"Snow":code<=82?"Showers":code<=99?"Thunderstorm":"Unknown";
      weather[country] = { temp_c: Math.round(d.current?.temperature_2m||0), wind_kmh: Math.round(d.current?.wind_speed_10m||0), condition, city };
    } catch (err) {
      console.error(`[weather:${country}] failed:`, err.message);
    }
  }));
  console.log("[weather] fetched for", Object.keys(weather).length, "countries");
  return weather;
}

// ── FX Rates ─────────────────────────────────────────────────────────────────
async function fetchFX() {
  try {
    const res = await fetch("https://open.er-api.com/v6/latest/USD", { signal: AbortSignal.timeout(8000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    const r = d.rates||{};
    console.log("[fx] rates fetched");
    return { AED: r.AED, SAR: r.SAR, QAR: r.QAR, KWD: r.KWD, BHR: r.BHD, OMR: r.OMR, JOD: r.JOD, EGP: r.EGP, IQD: r.IQD };
  } catch (err) {
    console.error("[fx] failed:", err.message);
    return {};
  }
}

// ── Lottery Signals ──────────────────────────────────────────────────────────
async function fetchLotterySignals() {
  const all = [];
  const seen = new Set();

  // Arctic Shift — active Pushshift replacement with current data
  // Pullpush only has data up to May 2025 and is unreliable
  const after30dDate = new Date(Date.now() - 30*24*3600*1000).toISOString().split("T")[0];
  const LOTTERY_SUBS = [
    // UAE / Gulf — primary market
    { sub: "dubai",              meFocus: true  },
    { sub: "UAE",                meFocus: true  },
    { sub: "abudhabi",           meFocus: true  },
    { sub: "sharjah",            meFocus: true  },
    { sub: "saudiarabia",        meFocus: true  },
    { sub: "qatar",              meFocus: true  },
    { sub: "Kuwait",             meFocus: true  },
    { sub: "Bahrain",            meFocus: true  },
    { sub: "oman",               meFocus: true  },
    // Expat source communities — huge lottery participation in MENA
    { sub: "india",              meFocus: false },
    { sub: "pakistan",           meFocus: false },
    { sub: "Philippines",        meFocus: false },
    { sub: "bangladesh",         meFocus: false },
    { sub: "srilanka",           meFocus: false },
    { sub: "Nepal",              meFocus: false },
    { sub: "IndiansAbroad",      meFocus: false },
    { sub: "PakistaniAbroad",    meFocus: false },
    { sub: "expats",             meFocus: false },
    { sub: "expat",              meFocus: false },
    // Lottery-specific
    { sub: "lottery",            meFocus: false },
    { sub: "lotterywinners",     meFocus: false },
    { sub: "Lottery_Winnings",   meFocus: false },
    // Broader ME / North Africa
    { sub: "egypt",              meFocus: true  },
    { sub: "jordan",             meFocus: true  },
    { sub: "lebanon",            meFocus: true  },
    { sub: "morocco",            meFocus: true  },
  ];

  // Expat source country subs — strong ME lottery connection needed to avoid noise
  // A post from r/india about a local Indian state lottery is not relevant
  const EXPAT_SUBS = new Set([
    "india","pakistan","Philippines","bangladesh","srilanka","Nepal",
    "IndiansAbroad","PakistaniAbroad","expats","expat",
    "egypt","jordan","lebanon","morocco",
  ]);

  await Promise.allSettled(LOTTERY_SUBS.map(async ({ sub, meFocus }) => {
    try {
      const url = `https://arctic-shift.photon-reddit.com/api/posts/search` +
        `?subreddit=${sub}&after=${after30dDate}&limit=100&sort=desc`;
      const res = await fetch(url, { headers: { "User-Agent": "OpenEye/1.0" }, signal: AbortSignal.timeout(12000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      const posts = d.data || d.posts || d || [];
      let subCount = 0;
      for (const p of posts) {
        if (!p.title || p.removed_by_category || seen.has(`r-${p.id}`)) continue;
        const txt = p.title + " " + (p.selftext || "");
        const tl  = txt.toLowerCase();
        // Always need a lottery keyword
        if (!LOT_KW.some(k => tl.includes(k))) continue;
        // For expat/non-Gulf subs: also require a ME geography or brand signal
        // This filters out "won the Kerala lottery" from r/india etc.
        if (EXPAT_SUBS.has(sub) && !isME(txt)) continue;
        seen.add(`r-${p.id}`);
        subCount++;
        all.push({
          id: `reddit-lot-${p.id}`,
          title: p.title,
          summary: (p.selftext || "").replace(/\n+/g, " ").trim().slice(0, 240) || `↑${p.score || 0} · 💬${p.num_comments || 0}`,
          url: `https://reddit.com${p.permalink || ""}`,
          timestamp: new Date((p.created_utc || 0) * 1000).toISOString(),
          source: `r/${sub}`,
          sourceType: "Reddit",
          tag: isME(txt) ? "ME-LOT" : "LOT",
          country: isME(txt) ? "Regional" : "Global",
          score: p.score || 0,
          comments: p.num_comments || 0,
          mood: classifyLottery(txt),
          isME: isME(txt),
        });
      }
      if (subCount > 0) console.log(`[lottery:r/${sub}] ${subCount} signals`);
    } catch (err) { console.error(`[lottery:r/${sub}]`, err.message); }
  }));

  // ── NewsAPI ─────────────────────────────────────────────────────────────────
  if (!NEWSAPI_KEY) {
    console.log("[lottery:newsapi] NEWSAPI_KEY not set — skipping");
  } else {
    try {
      // Broad MENA lottery query — covers UAE brands + regional draws + expat winners
      const q = encodeURIComponent(
        'mahzooz OR "big ticket" OR "emirates draw" OR "duty free draw" OR "lucky draw"' +
        ' OR "lottery win" OR "jackpot winner" OR "prize draw" OR "raffle winner"' +
        ' OR "abu dhabi duty free" OR "dubai duty free" OR "saudi lottery"'
      );
      const from = new Date(Date.now() - 25*24*3600*1000).toISOString().split("T")[0]; // free tier max 30d
      const url = `https://newsapi.org/v2/everything?q=${q}&from=${from}&language=en&sortBy=publishedAt&pageSize=100&apiKey=${NEWSAPI_KEY}`;
      const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
      if (!res.ok) throw new Error(`HTTP ${res.status} — ${await res.text().then(t=>t.slice(0,120))}`);
      const d = await res.json();
      for (const a of (d.articles || [])) {
        if (!a.title || a.title === "[Removed]" || seen.has(`na-${a.url}`)) continue;
        const txt = a.title + " " + (a.description || "");
        if (!LOT_KW.some(k => txt.toLowerCase().includes(k))) continue;
        seen.add(`na-${a.url}`);
        all.push({
          id: `newsapi-lot-${Buffer.from(a.url).toString("base64").slice(0,12)}`,
          title: a.title,
          summary: a.description || "",
          url: a.url,
          timestamp: a.publishedAt || new Date().toISOString(),
          source: a.source?.name || "NewsAPI",
          sourceType: "NewsAPI",
          tag: isME(txt) ? "ME-LOT" : "LOT",
          country: isME(txt) ? "Regional" : "Global",
          score: 0,
          comments: 0,
          mood: classifyLottery(txt),
          isME: isME(txt),
        });
      }
      console.log(`[lottery:newsapi] ${d.articles?.length || 0} raw → ${all.length} total after filter`);
    } catch (err) { console.error("[lottery:newsapi]", err.message); }
  }

  // ── The Guardian API (free, 12 req/s) ────────────────────────────────────────
  try {
    const guardianKey = GUARDIAN_KEY || "test"; // "test" key works with rate limits
    const q = encodeURIComponent("lottery OR jackpot OR mahzooz OR \"big ticket\" OR \"lucky draw\"");
    const from = new Date(Date.now() - 30*24*3600*1000).toISOString().split("T")[0];
    const url = `https://content.guardianapis.com/search?q=${q}&from-date=${from}&show-fields=headline,trailText&page-size=20&api-key=${guardianKey}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const d = await res.json();
    for (const a of (d.response?.results || [])) {
      if (!a.webTitle || seen.has(`gu-${a.id}`)) continue;
      const txt = a.webTitle + " " + (a.fields?.trailText || "");
      if (!LOT_KW.some(k => txt.toLowerCase().includes(k))) continue;
      seen.add(`gu-${a.id}`);
      all.push({
        id: `guardian-lot-${a.id.replace(/\//g,"-")}`,
        title: a.webTitle,
        summary: a.fields?.trailText || "",
        url: a.webUrl,
        timestamp: a.webPublicationDate,
        source: "The Guardian",
        sourceType: "News",
        tag: isME(txt) ? "ME-LOT" : "LOT",
        country: isME(txt) ? "Regional" : "Global",
        score: 0,
        comments: 0,
        mood: classifyLottery(txt),
        isME: isME(txt),
      });
    }
    console.log(`[lottery:guardian] fetched`);
  } catch (err) { console.error("[lottery:guardian]", err.message); }

  // ── RSS feeds — MENA lottery / prize coverage ────────────────────────────────
  // All free, no auth — keyword-filtered before pushing to results
  const LOT_RSS = [
    // ✅ Confirmed working from last run
    { name: "Gulf Business",    url: "https://gulfbusiness.com/feed/" },
    { name: "Saudi Gazette",    url: "https://saudigazette.com.sa/rss" },
    { name: "Middle East Eye",  url: "https://www.middleeasteye.net/rss" },
    { name: "Albawaba",         url: "https://www.albawaba.com/rss.xml" },
    // Retry with corrected URLs
    { name: "Arab News",        url: "https://www.arabnews.com/rss.xml" },
    { name: "Gulf News",        url: "https://gulfnews.com/rss/world" },
    { name: "The National",     url: "https://www.thenationalnews.com/rss" },
    { name: "Khaleej Times",    url: "https://www.khaleejtimes.com/rss" },
  ];
  await Promise.allSettled(LOT_RSS.map(async ({ name, url }) => {
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": "OpenEye-OSINT/4.0" },
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const xml = await res.text();
      // Simple XML item extraction — no external parser needed
      const items = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].map(m => m[1]);
      for (const item of items) {
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/) ||
                         item.match(/<title>(.*?)<\/title>/))?.[1]?.trim() || "";
        const link    = (item.match(/<link>(.*?)<\/link>/))?.[1]?.trim() || "";
        const desc    = (item.match(/<description><!\[CDATA\[(.*?)\]\]><\/description>/) ||
                         item.match(/<description>(.*?)<\/description>/))?.[1]?.trim() || "";
        const pubDate = (item.match(/<pubDate>(.*?)<\/pubDate>/))?.[1]?.trim() || "";
        if (!title || seen.has(`rss-${link}`)) continue;
        const txt = title + " " + desc;
        if (!LOT_KW.some(k => txt.toLowerCase().includes(k))) continue;
        const ts = pubDate ? new Date(pubDate).toISOString() : new Date().toISOString();
        seen.add(`rss-${link}`);
        all.push({
          id: `rss-lot-${Buffer.from(link).toString("base64").slice(0,12)}`,
          title,
          summary: desc.replace(/<[^>]+>/g, "").trim().slice(0, 240),
          url: link,
          timestamp: ts,
          source: name,
          sourceType: "RSS",
          tag: "ME-LOT",
          country: "Regional",
          score: 0,
          comments: 0,
          mood: classifyLottery(txt),
          isME: true,
        });
      }
      console.log(`[lottery:${name}] fetched`);
    } catch (err) { console.error(`[lottery:${name}]`, err.message); }
  }));


  const since = Math.floor(Date.now()/1000) - 30*24*3600;
  const queries = ["lottery UAE expat","jackpot dubai","mahzooz big ticket","gulf lucky draw","lottery win middle east"];
  await Promise.allSettled(queries.map(async q => {
    try {
      const d = await fetch(`https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(q)}&tags=story&hitsPerPage=8&numericFilters=created_at_i>${since}`).then(r=>r.json());
      for (const h of (d.hits||[])) {
        if (!h.title || seen.has(`hn-${h.objectID}`)) continue;
        seen.add(`hn-${h.objectID}`);
        const txt = h.title + " " + (h.url||"");
        all.push({ id:`hn-lot-${h.objectID}`, title:h.title, summary:`${h.points||0} pts · ${h.num_comments||0} comments`, url:h.url||`https://news.ycombinator.com/item?id=${h.objectID}`, timestamp:new Date(h.created_at).toISOString(), source:"Hacker News", sourceType:"HN", tag:"LOT", country:isME(txt)?"Regional":"Global", score:h.points||0, comments:h.num_comments||0, mood:classifyLottery(txt), isME:isME(txt) });
      }
    } catch {}
  }));

  // Enforce 30-day cutoff — Pullpush can return old posts if date filter was ignored
  const cutoff30d = Date.now() - 30*24*3600*1000;
  const fresh = all.filter(i => new Date(i.timestamp).getTime() > cutoff30d);

  // Compute meta
  const meSignals = fresh.filter(i=>i.isME).length;
  const moodDist = fresh.reduce((a,i) => { a[i.mood]=(a[i.mood]||0)+1; return a; }, { HOPEFUL:0, CYNICAL:0, ANXIOUS:0, NEUTRAL:0 });
  const dominantMood = Object.entries(moodDist).sort((a,b)=>b[1]-a[1])[0]?.[0]||"NEUTRAL";
  const avgUpvoteRatio = fresh.length ? Math.round(fresh.reduce((s,i)=>s+(i.upvoteRatio||0.75),0)/fresh.length*100) : 75;
  const total = fresh.length||1;
  const maxPct = Math.max(...Object.values(moodDist))/total;
  const uncertaintyScore = Math.round((1-maxPct)*100);

  console.log(`[lottery] ${fresh.length} signals (from ${all.length} fetched), ${meSignals} ME-specific`);
  return { items: fresh.sort((a,b)=>new Date(b.timestamp)-new Date(a.timestamp)), meta: { totalSignals:fresh.length, meSignals, moodDist, dominantMood, avgUpvoteRatio, uncertaintyScore } };
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log("[fetch-signals] Starting…");

  const [gdelt_tone, weather, fx, lotteryData] = await Promise.all([
    fetchGDELTTone(),
    fetchWeather(),
    fetchFX(),
    fetchLotterySignals(),
  ]);

  const output = {
    generated_at: new Date().toISOString(),
    gdelt_tone,
    weather,
    fx,
    lottery: lotteryData,
  };

  writeFileSync(OUT, JSON.stringify(output, null, 2));
  console.log(`[fetch-signals] Done → ${OUT}`);
  console.log(`  GDELT: ${Object.keys(gdelt_tone).length} countries | FX: ${Object.keys(fx).length} rates | Lottery: ${lotteryData.items.length} signals`);
}

main().catch(err => { console.error("[fetch-signals] Fatal:", err); process.exit(0); });
