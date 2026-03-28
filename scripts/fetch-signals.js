#!/usr/bin/env node
// scripts/fetch-signals.js
// Fetches ACLED conflict events, GDELT tone, Open-Meteo weather, FX, lottery signals
// → public/data/signals.json
// Run by GitHub Actions cron every hour.

import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../public/data/signals.json");
mkdirSync(dirname(OUT), { recursive: true });

// ACLED uses OAuth 2.0 — store your myACLED email+password as GitHub Secrets
const ACLED_EMAIL    = process.env.ACLED_EMAIL    || "";
const ACLED_PASSWORD = process.env.ACLED_PASSWORD || "";

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

// ACLED country names (their API uses full country names)
const ACLED_COUNTRIES = [
  "United Arab Emirates","Saudi Arabia","Qatar","Kuwait","Bahrain","Oman",
  "Jordan","Lebanon","Syria","Iraq","Palestine","Israel","Egypt","Libya",
  "Tunisia","Algeria","Morocco","Sudan","Yemen","Iran"
];

// GDELT ISO2 codes for tone API
const GDELT_COUNTRIES = [
  { id: "UAE",          iso: "AE" }, { id: "Saudi Arabia",  iso: "SA" },
  { id: "Qatar",        iso: "QA" }, { id: "Kuwait",        iso: "KW" },
  { id: "Bahrain",      iso: "BH" }, { id: "Oman",          iso: "OM" },
  { id: "Jordan",       iso: "JO" }, { id: "Lebanon",       iso: "LB" },
  { id: "Iraq",         iso: "IQ" }, { id: "Egypt",         iso: "EG" },
  { id: "Yemen",        iso: "YE" }, { id: "Iran",          iso: "IR" },
];

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
  return ["uae","dubai","abu dhabi","sharjah","saudi","gulf","qatar","kuwait","oman","bahrain","arab","expat","mahzooz","emirates draw","big ticket","duty free","dream dubai","dirham","aed"].some(k=>t.includes(k));
}

// ── ACLED OAuth ──────────────────────────────────────────────────────────────
// ACLED now requires OAuth 2.0 (email + password → Bearer token, valid 24h)
// Docs: https://acleddata.com/api-documentation/getting-started
async function getACLEDToken() {
  const res = await fetch("https://acleddata.com/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      username:   ACLED_EMAIL,
      password:   ACLED_PASSWORD,
      grant_type: "password",
      client_id:  "acled",
    }),
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`ACLED auth HTTP ${res.status}`);
  const d = await res.json();
  if (!d.access_token) throw new Error("No access_token in ACLED response");
  return d.access_token;
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

    // Build country filter: country=UAE:OR:country=Saudi Arabia:OR:...
    const countryFilter = ACLED_COUNTRIES.map((c, i) =>
      i === 0 ? `country=${encodeURIComponent(c)}` : `country=${encodeURIComponent(c)}`
    ).join(":OR:");

    const url = `https://acleddata.com/api/acled/read?_format=json` +
      `&${ACLED_COUNTRIES.map(c => `country=${encodeURIComponent(c)}`).join(":OR:country=")}` +
      `&event_date=${since}|${today}&event_date_where=BETWEEN` +
      `&fields=event_id_cnty|event_date|event_type|sub_event_type|country|admin1|location|latitude|longitude|fatalities|notes` +
      `&limit=500`;

    const res = await fetch(url, {
      headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
      signal: AbortSignal.timeout(25000),
    });
    if (!res.ok) throw new Error(`ACLED data HTTP ${res.status}`);
    const d = await res.json();
    const events = (d.data||[]).map(e => ({
      event_id:      e.event_id_cnty,
      event_date:    e.event_date,
      event_type:    e.event_type,
      sub_event_type:e.sub_event_type,
      country:       e.country,
      admin1:        e.admin1,
      location:      e.location,
      latitude:      parseFloat(e.latitude)||0,
      longitude:     parseFloat(e.longitude)||0,
      fatalities:    parseInt(e.fatalities)||0,
      notes:         (e.notes||"").slice(0,200),
    }));
    console.log(`[acled] ${events.length} events`);
    return events;
  } catch (err) {
    console.error("[acled] failed:", err.message);
    return [];
  }
}

// ── GDELT Tone ───────────────────────────────────────────────────────────────
async function fetchGDELTTone() {
  const tone = {};
  await Promise.allSettled(GDELT_COUNTRIES.map(async ({ id, iso }) => {
    try {
      const url = `https://api.gdeltproject.org/api/v2/doc/doc?query=sourcecountry:${iso}&mode=tonechart&format=json&TIMESPAN=7days`;
      const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      // tonechart returns array of {date, tone} — average them
      const tones = (d.tonechart||[]).map(t=>parseFloat(t.avgtone||0)).filter(n=>!isNaN(n));
      tone[id] = tones.length ? Math.round((tones.reduce((a,b)=>a+b,0)/tones.length)*100)/100 : 0;
    } catch (err) {
      console.error(`[gdelt:${id}] failed:`, err.message);
      tone[id] = 0;
    }
  }));
  console.log("[gdelt] tone fetched for", Object.keys(tone).length, "countries");
  return tone;
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

  // Pullpush Reddit - lottery subreddits
  const LOTTERY_SUBS = [
    { sub: "lottery",   meFocus: false },
    { sub: "dubai",     meFocus: true  },
    { sub: "UAE",       meFocus: true  },
    { sub: "expats",    meFocus: true  },
  ];

  await Promise.allSettled(LOTTERY_SUBS.map(async ({ sub, meFocus }) => {
    try {
      const url = `https://api.pullpush.io/reddit/search/submission/?subreddit=${sub}&size=100&sort=desc&sort_type=score`;
      const res = await fetch(url, { headers: { "User-Agent": "OpenEye/1.0" }, signal: AbortSignal.timeout(12000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      for (const p of (d.data||[])) {
        if (!p.title || p.removed_by_category || seen.has(`r-${p.id}`)) continue;
        const txt = p.title + " " + (p.selftext||"");
        if (!LOT_KW.some(k=>txt.toLowerCase().includes(k))) continue;
        if (meFocus && !isME(txt) && !LOT_KW.some(k=>txt.toLowerCase().includes(k))) continue;
        seen.add(`r-${p.id}`);
        all.push({ id:`reddit-lot-${p.id}`, title:p.title, summary:(p.selftext||"").replace(/\n+/g," ").trim().slice(0,240)||`↑${p.score||0} · 💬${p.num_comments||0}`, url:`https://reddit.com${p.permalink||""}`, timestamp:new Date((p.created_utc||0)*1000).toISOString(), source:`r/${sub}`, sourceType:"Reddit", tag:isME(txt)?"ME-LOT":"LOT", country:isME(txt)?"Regional":"Global", score:p.score||0, comments:p.num_comments||0, mood:classifyLottery(txt), isME:isME(txt) });
      }
    } catch (err) { console.error(`[lottery:r/${sub}]`, err.message); }
  }));

  // HN lottery queries
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

  // Compute meta
  const meSignals = all.filter(i=>i.isME).length;
  const moodDist = all.reduce((a,i) => { a[i.mood]=(a[i.mood]||0)+1; return a; }, { HOPEFUL:0, CYNICAL:0, ANXIOUS:0, NEUTRAL:0 });
  const dominantMood = Object.entries(moodDist).sort((a,b)=>b[1]-a[1])[0]?.[0]||"NEUTRAL";
  const avgUpvoteRatio = all.length ? Math.round(all.reduce((s,i)=>s+(i.upvoteRatio||0.75),0)/all.length*100) : 75;
  // Uncertainty: how spread the moods are
  const total = all.length||1;
  const maxPct = Math.max(...Object.values(moodDist))/total;
  const uncertaintyScore = Math.round((1-maxPct)*100);

  console.log(`[lottery] ${all.length} signals, ${meSignals} ME-specific`);
  return { items: all.sort((a,b)=>new Date(b.timestamp)-new Date(a.timestamp)), meta: { totalSignals:all.length, meSignals, moodDist, dominantMood, avgUpvoteRatio, uncertaintyScore } };
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log("[fetch-signals] Starting…");

  const [acled, gdelt_tone, weather, fx, lotteryData] = await Promise.all([
    fetchACLED(),
    fetchGDELTTone(),
    fetchWeather(),
    fetchFX(),
    fetchLotterySignals(),
  ]);

  const output = {
    generated_at: new Date().toISOString(),
    acled,
    gdelt_tone,
    weather,
    fx,
    lottery: lotteryData,
  };

  writeFileSync(OUT, JSON.stringify(output, null, 2));
  console.log(`[fetch-signals] Done → ${OUT}`);
  console.log(`  ACLED: ${acled.length} events | GDELT: ${Object.keys(gdelt_tone).length} countries | FX: ${Object.keys(fx).length} rates | Lottery: ${lotteryData.items.length} signals`);
}

main().catch(err => { console.error("[fetch-signals] Fatal:", err); process.exit(0); });
